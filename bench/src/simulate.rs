use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::U256;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::sol_types::SolCall;
use chrono::Utc;
use eyre::Result;
use rand::Rng;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::SimulateConfig;
use crate::contracts::Vault;
use crate::metrics::{self, TxRecord};
use crate::nonce::AtomicNonce;
use crate::ramp::RampSchedule;
use crate::wallet;

const GAS_LIMIT_MATCH: u64 = 500_000;
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(60);

struct PendingTx {
    nonce: u64,
    tx_hash: alloy::primitives::TxHash,
    submit_time_ms: i64,
    phase: String,
}

pub async fn run(config: SimulateConfig) -> Result<()> {
    let addresses = {
        let wallets = wallet::derive_wallets(&config.base.mnemonic, config.base.num_accounts)?;
        wallet::addresses(&wallets)
    };

    let deployer = &config.base.deployer_key;
    let deployer_addr = deployer.address();

    let operator_wallet = EthereumWallet::from(deployer.clone());
    let send_provider = ProviderBuilder::new()
        .wallet(operator_wallet)
        .connect_http(config.base.rpc_url.parse()?);

    let poll_provider = ProviderBuilder::new()
        .connect_http(config.base.rpc_url.parse()?);

    info!(
        "simulate: {} accounts, target rate {:.1} TPS, duration {}s",
        addresses.len(),
        config.rate,
        config.duration
    );

    let gas_price = match config.base.gas_price {
        Some(p) => p,
        None => poll_provider.get_gas_price().await?,
    };
    info!("using gas price: {} wei", gas_price);

    let nonce = AtomicNonce::new(&poll_provider, deployer_addr).await?;

    let schedule = RampSchedule::new(
        config.rate,
        config.warmup,
        config.ramp_step,
        config.ramp_multiplier,
        config.duration,
        config.spike_multiplier,
        config.spike_duration,
        config.recovery,
    );

    let (pending_tx, pending_rx) = mpsc::channel::<PendingTx>(4096);
    let (record_tx, record_rx) = mpsc::channel::<TxRecord>(4096);

    let output_path = config.output.clone();

    // Spawn receipt poller
    let record_tx_clone = record_tx.clone();
    let poller_handle = tokio::spawn(async move {
        receipt_poller(poll_provider, pending_rx, record_tx_clone).await
    });

    // Spawn CSV writer
    let writer_handle = tokio::spawn(async move {
        metrics::csv_writer_task(record_rx, &output_path).await
    });

    // Submit loop
    let start = Instant::now();
    let mut submitted = 0u64;
    let mut rng = rand::rng();

    if addresses.len() < 2 {
        eyre::bail!("need at least 2 funded accounts");
    }

    loop {
        let Some((phase, _tps)) = schedule.current() else {
            break;
        };
        let delay = schedule.inter_tx_delay().unwrap();

        let idx_a = rng.random_range(0..addresses.len());
        let mut idx_b = rng.random_range(0..addresses.len());
        while idx_b == idx_a {
            idx_b = rng.random_range(0..addresses.len());
        }

        let amount_range: u64 = (config.match_amount_max - config.match_amount_min)
            .try_into()
            .unwrap_or(u64::MAX);
        let amount_a = config.match_amount_min + U256::from(rng.random_range(0..=amount_range));
        let amount_b = config.match_amount_min + U256::from(rng.random_range(0..=amount_range));

        let n = nonce.next();
        let call = Vault::matchOrdersCall {
            a: addresses[idx_a],
            b: addresses[idx_b],
            amountA: amount_a,
            amountB: amount_b,
        };

        let tx = TransactionRequest::default()
            .with_to(config.base.vault)
            .with_input(call.abi_encode())
            .with_nonce(n)
            .with_gas_limit(GAS_LIMIT_MATCH)
            .with_gas_price(gas_price)
            .with_chain_id(config.base.chain_id);

        let submit_time = Utc::now().timestamp_millis();

        match send_provider.send_transaction(tx).await {
            Ok(pending) => {
                let hash = *pending.tx_hash();
                submitted += 1;
                if submitted % 50 == 0 || submitted <= 5 {
                    info!("[{}] #{} nonce={} hash={}", phase, submitted, n, hash);
                }
                let _ = pending_tx
                    .send(PendingTx {
                        nonce: n,
                        tx_hash: hash,
                        submit_time_ms: submit_time,
                        phase: phase.to_string(),
                    })
                    .await;
            }
            Err(e) => {
                warn!("submit failed nonce={}: {}", n, e);
            }
        }

        tokio::time::sleep(delay).await;
    }

    let elapsed = start.elapsed();
    info!("submit loop done: {} txs in {:.1}s", submitted, elapsed.as_secs_f64());

    drop(pending_tx);
    drop(record_tx);

    if let Err(e) = poller_handle.await? {
        warn!("receipt poller error: {}", e);
    }

    let records = writer_handle.await??;

    let summary = metrics::Summary::compute(&records, elapsed.as_secs_f64());
    summary.print();

    Ok(())
}

async fn receipt_poller(
    provider: impl Provider,
    mut pending_rx: mpsc::Receiver<PendingTx>,
    record_tx: mpsc::Sender<TxRecord>,
) -> Result<()> {
    let mut queue: VecDeque<PendingTx> = VecDeque::new();
    let mut closed = false;

    loop {
        loop {
            match pending_rx.try_recv() {
                Ok(ptx) => queue.push_back(ptx),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }

        if queue.is_empty() && closed {
            break;
        }

        let batch_size = queue.len().min(20);
        let mut confirmed_indices = Vec::new();

        for i in 0..batch_size {
            let ptx = &queue[i];
            let age_ms = Utc::now().timestamp_millis() - ptx.submit_time_ms;

            match provider.get_transaction_receipt(ptx.tx_hash).await {
                Ok(Some(receipt)) => {
                    let confirm_time = Utc::now().timestamp_millis();
                    let record = TxRecord {
                        nonce: ptx.nonce,
                        submit_timestamp_ms: ptx.submit_time_ms,
                        tx_hash: format!("{}", ptx.tx_hash),
                        confirm_timestamp_ms: Some(confirm_time),
                        block_number: receipt.block_number,
                        gas_used: Some(receipt.gas_used),
                        effective_gas_price: Some(receipt.effective_gas_price),
                        status: Some(receipt.status()),
                        latency_ms: Some(confirm_time - ptx.submit_time_ms),
                        phase: ptx.phase.clone(),
                    };
                    let _ = record_tx.send(record).await;
                    confirmed_indices.push(i);
                }
                Ok(None) => {
                    if age_ms > RECEIPT_TIMEOUT.as_millis() as i64 {
                        let record = TxRecord {
                            nonce: ptx.nonce,
                            submit_timestamp_ms: ptx.submit_time_ms,
                            tx_hash: format!("{}", ptx.tx_hash),
                            confirm_timestamp_ms: None,
                            block_number: None,
                            gas_used: None,
                            effective_gas_price: None,
                            status: None,
                            latency_ms: None,
                            phase: ptx.phase.clone(),
                        };
                        let _ = record_tx.send(record).await;
                        confirmed_indices.push(i);
                    }
                }
                Err(e) => {
                    warn!("receipt poll error for {}: {}", ptx.tx_hash, e);
                }
            }
        }

        confirmed_indices.sort_unstable();
        for i in confirmed_indices.into_iter().rev() {
            queue.remove(i);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(())
}
