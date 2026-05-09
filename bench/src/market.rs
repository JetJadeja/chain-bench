use std::time::{Duration, Instant};

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::sol_types::SolCall;
use chrono::Utc;
use eyre::Result;
use rand::Rng;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::batch_rpc::BatchRpcClient;
use crate::block_stream;
use crate::config::MarketConfig;
use crate::contracts::Vault;
use crate::market_stats::{self, MarketStats};
use crate::tx_tracker::{self, PendingTx, TxLifecycle};
use crate::wallet;

const GAS_LIMIT_MATCH: u64 = 500_000;
const OPERATOR_ROLE: U256 = U256::from_limbs([1, 0, 0, 0]);

struct OperatorState {
    wallet: EthereumWallet,
    nonce: u64,
    label: String,
}

pub async fn run(config: MarketConfig) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(config.base.rpc_url.parse()?);

    let gas_price = match config.base.gas_price {
        Some(p) => p,
        None => provider.get_gas_price().await?,
    };
    info!("gas price: {gas_price} wei");

    let mut operators = setup_operators(&config, &provider).await?;
    info!("{} operator(s) ready", operators.len());

    let wallets = wallet::derive_wallets(&config.base.mnemonic, config.base.num_accounts)?;
    let addresses = wallet::addresses(&wallets);
    if addresses.len() < 2 {
        eyre::bail!("need at least 2 funded accounts");
    }

    // Infrastructure
    let submit_client = BatchRpcClient::new(&config.base.rpc_url);
    let tracker_client = BatchRpcClient::new(&config.base.rpc_url);
    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    let block_tx = block_stream::start_polling(provider.clone(), poll_interval);

    let (pending_tx, pending_rx) = mpsc::channel::<PendingTx>(65536);
    let (record_tx, mut record_rx) = mpsc::channel::<TxLifecycle>(65536);

    let block_rx = block_tx.subscribe();
    let tracker_handle = tokio::spawn(async move {
        tx_tracker::run(tracker_client, pending_rx, block_rx, record_tx).await;
    });

    let collector_handle = tokio::spawn(async move {
        let mut records = Vec::new();
        while let Some(record) = record_rx.recv().await {
            records.push(record);
        }
        records
    });

    let mut total_submitted = 0u64;
    let mut total_failed = 0u64;

    // --- Phase 1: Steady ---
    info!(
        "▶ steady phase: {:.0} TPS for {}s ({} operators)",
        config.steady_rate,
        config.steady_duration,
        operators.len()
    );

    let (sent, failed) = run_phase(
        &mut operators,
        &addresses,
        &config,
        gas_price,
        &submit_client,
        &pending_tx,
        "steady",
        config.steady_rate,
        config.steady_rate,
        Duration::from_secs(config.steady_duration),
    )
    .await?;
    total_submitted += sent;
    total_failed += failed;
    info!("  steady done: {sent} sent, {failed} failed");

    // --- Phase 2: Ramp ---
    let ramp_peak = config.steady_rate * 10.0;
    info!(
        "▶ ramp phase: {:.0} → {:.0} TPS over {}s",
        config.steady_rate, ramp_peak, config.ramp_duration
    );

    let (sent, failed) = run_phase(
        &mut operators,
        &addresses,
        &config,
        gas_price,
        &submit_client,
        &pending_tx,
        "ramp",
        config.steady_rate,
        ramp_peak,
        Duration::from_secs(config.ramp_duration),
    )
    .await?;
    total_submitted += sent;
    total_failed += failed;
    info!("  ramp done: {sent} sent, {failed} failed");

    // --- Phase 3: BURST ---
    info!(
        "▶ burst: {} txs across {} operator(s)",
        config.burst_size,
        operators.len()
    );

    let max_mempool = operators.len() * 100;
    if config.burst_size > max_mempool {
        warn!(
            "burst_size ({}) exceeds mempool capacity ({} operators × 100 = {}). \
             Excess txs will be rejected. Use --num-operators {}",
            config.burst_size,
            operators.len(),
            max_mempool,
            (config.burst_size + 99) / 100
        );
    }

    let burst_start_epoch = Utc::now().timestamp_millis();

    // Pre-sign all burst txs, interleaved round-robin across operators.
    // This ensures each RPC batch contains txs from ALL operators rather than
    // clustering 100 txs from one sender — avoids per-sender anti-DDoS throttling.
    let mut signed_burst: Vec<(Vec<u8>, u64, String)> = Vec::with_capacity(config.burst_size);
    let txs_per_op = config.burst_size / operators.len();
    let remainder = config.burst_size % operators.len();

    for round in 0..txs_per_op + 1 {
        for (i, op) in operators.iter_mut().enumerate() {
            let count = txs_per_op + if i < remainder { 1 } else { 0 };
            if round >= count {
                continue;
            }
            let encoded = sign_match_order(op, &addresses, &config, gas_price).await?;
            signed_burst.push(encoded);
        }
    }
    info!("  pre-signed {} txs", signed_burst.len());

    // Fire all concurrently
    let burst_start = Instant::now();

    let raw_txs: Vec<Vec<u8>> = signed_burst.iter().map(|(raw, _, _)| raw.clone()).collect();

    // Fire all burst txs — either as one giant batch (burst_chunk=0) or chunked
    let mut burst_ok = 0usize;
    let mut burst_fail = 0usize;

    let burst_chunk = if config.burst_chunk == 0 {
        raw_txs.len().max(1)
    } else {
        config.burst_chunk
    };
    info!("  batch size: {burst_chunk} txs per HTTP POST ({} POST(s))",
        (raw_txs.len() + burst_chunk - 1) / burst_chunk);

    let mut burst_tasks: JoinSet<(usize, usize)> = JoinSet::new();

    let chunks: Vec<(Vec<Vec<u8>>, Vec<(u64, String)>)> = raw_txs
        .chunks(burst_chunk)
        .enumerate()
        .map(|(chunk_idx, chunk)| {
            let raw_chunk = chunk.to_vec();
            let start = chunk_idx * burst_chunk;
            let meta: Vec<(u64, String)> = signed_burst[start..start + chunk.len()]
                .iter()
                .map(|(_, nonce, label)| (*nonce, label.clone()))
                .collect();
            (raw_chunk, meta)
        })
        .collect();

    const MAX_BURST_INFLIGHT: usize = 5;

    for (raw_chunk, meta) in chunks {
        // Backpressure: wait if too many concurrent POSTs
        while burst_tasks.len() >= MAX_BURST_INFLIGHT {
            if let Some(Ok((ok, fail))) = burst_tasks.join_next().await {
                burst_ok += ok;
                burst_fail += fail;
            }
        }

        let client = submit_client.clone();
        let ptx = pending_tx.clone();

        burst_tasks.spawn(async move {
            let t_submit = Instant::now();
            let t_epoch = Utc::now().timestamp_millis();
            let mut ok = 0usize;
            let mut fail = 0usize;

            match client.batch_send_raw(&raw_chunk).await {
                Ok(results) => {
                    for (i, result) in results.into_iter().enumerate() {
                        let (nonce, ref label) = meta[i];
                        match result {
                            Ok(hash) => {
                                let _ = ptx
                                    .send(PendingTx {
                                        tx_hash: hash,
                                        nonce,
                                        operator: label.clone(),
                                        t_submit,
                                        t_submit_epoch_ms: t_epoch,
                                        phase: "burst".into(),
                                        burst_id: Some(0),
                                    })
                                    .await;
                                ok += 1;
                            }
                            Err(e) => {
                                tracing::warn!("burst failed nonce={nonce}: {e}");
                                fail += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("burst batch send failed: {e}");
                    fail = raw_chunk.len();
                }
            }
            (ok, fail)
        });
    }

    while let Some(result) = burst_tasks.join_next().await {
        if let Ok((ok, fail)) = result {
            burst_ok += ok;
            burst_fail += fail;
        }
    }

    total_submitted += burst_ok as u64;
    total_failed += burst_fail as u64;

    info!(
        "  burst fired: {burst_ok} ok, {burst_fail} failed, spread: {:.0}ms",
        burst_start.elapsed().as_millis()
    );

    // --- Wait for completion ---
    drop(pending_tx);
    info!(
        "waiting for {} txs to confirm...",
        total_submitted
    );

    let records = collector_handle.await?;
    tracker_handle.await?;
    drop(block_tx);

    // --- Stats ---
    let stats = MarketStats::compute(&records, Some(burst_start_epoch));
    stats.print(config.num_operators, total_submitted, total_failed);

    market_stats::write_csv(&records, &config.output).await?;
    info!("results written to {}", config.output.display());

    Ok(())
}

/// Run a phase at a rate that linearly interpolates from start_rate to end_rate.
/// All operators are used round-robin. Signed txs are batched into HTTP posts.
async fn run_phase(
    operators: &mut [OperatorState],
    addresses: &[Address],
    config: &MarketConfig,
    gas_price: u128,
    batch_client: &BatchRpcClient,
    pending_tx: &mpsc::Sender<PendingTx>,
    phase: &str,
    start_rate: f64,
    end_rate: f64,
    duration: Duration,
) -> Result<(u64, u64)> {
    let phase_start = Instant::now();
    let mut submitted = 0u64;
    let mut failed = 0u64;
    let mut op_idx = 0usize;
    let mut next_send = Instant::now();

    const BATCH_MAX: usize = 200;
    const FLUSH_INTERVAL: Duration = Duration::from_millis(50);
    const MAX_INFLIGHT: usize = 20;

    struct SignedTx {
        raw: Vec<u8>,
        nonce: u64,
        operator: String,
    }

    let mut batch: Vec<SignedTx> = Vec::with_capacity(BATCH_MAX);
    let mut last_flush = Instant::now();
    let mut send_tasks: JoinSet<(u64, u64)> = JoinSet::new();

    loop {
        let phase_elapsed = phase_start.elapsed();
        if phase_elapsed >= duration && batch.is_empty() {
            break;
        }

        // Flush batch if full or flush interval elapsed (and we have txs)
        let should_flush = !batch.is_empty()
            && (batch.len() >= BATCH_MAX
                || last_flush.elapsed() >= FLUSH_INTERVAL
                || phase_elapsed >= duration);

        if should_flush {
            // Collect completed sends without blocking
            while let Some(result) = send_tasks.try_join_next() {
                if let Ok((ok, fail)) = result {
                    submitted += ok;
                    failed += fail;
                }
            }

            // Backpressure: if too many sends in flight, wait for one
            while send_tasks.len() >= MAX_INFLIGHT {
                if let Some(Ok((ok, fail))) = send_tasks.join_next().await {
                    submitted += ok;
                    failed += fail;
                }
            }

            let (raw_txs, meta): (Vec<Vec<u8>>, Vec<(u64, String)>) = batch
                .drain(..)
                .map(|s| (s.raw, (s.nonce, s.operator)))
                .unzip();
            let client = batch_client.clone();
            let ptx = pending_tx.clone();
            let ph = phase.to_string();

            send_tasks.spawn(async move {
                let t_submit = Instant::now();
                let t_submit_epoch = chrono::Utc::now().timestamp_millis();
                let mut ok = 0u64;
                let mut fail = 0u64;

                match client.batch_send_raw(&raw_txs).await {
                    Ok(results) => {
                        for (i, result) in results.into_iter().enumerate() {
                            let (nonce, ref operator) = meta[i];
                            match result {
                                Ok(hash) => {
                                    ok += 1;
                                    let _ = ptx
                                        .send(PendingTx {
                                            tx_hash: hash,
                                            nonce,
                                            operator: operator.clone(),
                                            t_submit,
                                            t_submit_epoch_ms: t_submit_epoch,
                                            phase: ph.clone(),
                                            burst_id: None,
                                        })
                                        .await;
                                }
                                Err(e) => {
                                    fail += 1;
                                    tracing::warn!("submit failed nonce={nonce}: {e}");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        fail = raw_txs.len() as u64;
                        tracing::warn!("batch send failed ({fail} txs): {e}");
                    }
                }
                (ok, fail)
            });

            last_flush = Instant::now();

            if submitted > 0 && submitted % 500 == 0 {
                let actual_tps = submitted as f64 / phase_start.elapsed().as_secs_f64();
                info!("  [{phase}] {submitted} sent ({actual_tps:.0} actual TPS)");
            }

            continue;
        }

        if phase_elapsed >= duration {
            break;
        }

        // Rate limiting
        let now = Instant::now();
        if now < next_send {
            let sleep_dur = (next_send - now).min(FLUSH_INTERVAL - last_flush.elapsed().min(FLUSH_INTERVAL));
            if sleep_dur > Duration::from_micros(100) {
                tokio::time::sleep(sleep_dur).await;
            } else {
                tokio::task::yield_now().await;
            }
            continue;
        }

        // Current rate based on linear interpolation
        let frac = phase_start.elapsed().as_secs_f64() / duration.as_secs_f64();
        let current_rate = start_rate + frac * (end_rate - start_rate);
        let interval = Duration::from_secs_f64(1.0 / current_rate);
        next_send = now + interval;

        // Round-robin operator
        let num_ops = operators.len();
        let op = &mut operators[op_idx];
        op_idx = (op_idx + 1) % num_ops;

        // Sign tx and add to batch
        let (encoded, nonce, label) = sign_match_order(op, addresses, config, gas_price).await?;
        batch.push(SignedTx {
            raw: encoded,
            nonce,
            operator: label,
        });
    }

    // Wait for all inflight batch sends to complete
    while let Some(result) = send_tasks.join_next().await {
        if let Ok((ok, fail)) = result {
            submitted += ok;
            failed += fail;
        }
    }

    Ok((submitted, failed))
}

/// Build, sign, and encode a matchOrders tx. Returns (raw_bytes, nonce, operator_label).
async fn sign_match_order(
    op: &mut OperatorState,
    addresses: &[Address],
    config: &MarketConfig,
    gas_price: u128,
) -> Result<(Vec<u8>, u64, String)> {
    let (a, b) = random_pair(addresses);
    let (amount_a, amount_b) = random_amounts(config);

    let call = Vault::matchOrdersCall {
        a,
        b,
        amountA: amount_a,
        amountB: amount_b,
    };

    let nonce = op.nonce;
    op.nonce += 1;

    let tx = TransactionRequest::default()
        .with_to(config.base.vault)
        .with_input(call.abi_encode())
        .with_nonce(nonce)
        .with_gas_limit(GAS_LIMIT_MATCH)
        .with_gas_price(gas_price)
        .with_chain_id(config.base.chain_id);

    let tx_envelope = tx.build(&op.wallet).await?;
    let mut encoded = Vec::new();
    alloy::rlp::Encodable::encode(&tx_envelope, &mut encoded);

    Ok((encoded, nonce, op.label.clone()))
}

async fn setup_operators(
    config: &MarketConfig,
    provider: &impl Provider,
) -> Result<Vec<OperatorState>> {
    let deployer = &config.base.deployer_key;
    let deployer_addr = deployer.address();

    if config.num_operators == 0 {
        eyre::bail!("need at least 1 operator");
    }

    if config.num_operators == 1 {
        let nonce = provider.get_transaction_count(deployer_addr).await?;
        return Ok(vec![OperatorState {
            wallet: EthereumWallet::from(deployer.clone()),
            nonce,
            label: "deployer".into(),
        }]);
    }

    let op_keys = wallet::derive_operators(&config.base.mnemonic, config.num_operators)?;
    let vault = Vault::new(config.base.vault, provider);

    let mut operators = Vec::new();
    for (i, key) in op_keys.iter().enumerate() {
        let addr = key.address();

        let has_role = vault.hasAnyRole(addr, OPERATOR_ROLE).call().await?;
        if !has_role {
            eyre::bail!(
                "operator {i} ({addr}) missing OPERATOR_ROLE — run `fund --num-operators {}` first",
                config.num_operators
            );
        }

        let balance = provider.get_balance(addr).await?;
        if balance.is_zero() {
            eyre::bail!(
                "operator {i} ({addr}) has no ETH — run `fund --num-operators {}` first",
                config.num_operators
            );
        }

        let nonce = provider.get_transaction_count(addr).await?;
        operators.push(OperatorState {
            wallet: EthereumWallet::from(key.clone()),
            nonce,
            label: format!("op-{i}"),
        });
    }

    Ok(operators)
}

fn random_pair(addresses: &[Address]) -> (Address, Address) {
    let mut rng = rand::rng();
    let a = rng.random_range(0..addresses.len());
    let mut b = rng.random_range(0..addresses.len());
    while b == a {
        b = rng.random_range(0..addresses.len());
    }
    (addresses[a], addresses[b])
}

fn random_amounts(config: &MarketConfig) -> (U256, U256) {
    let mut rng = rand::rng();
    let range: u64 = (config.match_amount_max - config.match_amount_min)
        .try_into()
        .unwrap_or(u64::MAX);
    let a = config.match_amount_min + U256::from(rng.random_range(0..=range));
    let b = config.match_amount_min + U256::from(rng.random_range(0..=range));
    (a, b)
}
