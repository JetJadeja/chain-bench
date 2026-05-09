use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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
use crate::tx_tracker::{self, InflightCounters, PendingTx, TxLifecycle};
use crate::wallet;

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

    let mut operators = setup_operators(&config).await?;
    info!("{} operator(s) ready", operators.len());

    let wallets = wallet::derive_wallets(&config.base.mnemonic, config.base.num_accounts)?;
    let addresses = wallet::addresses(&wallets);
    if addresses.len() < 2 {
        eyre::bail!("need at least 2 funded accounts");
    }

    // Infrastructure
    let submit_client = BatchRpcClient::new_multi(config.base.rpc_urls.clone());
    info!("{} RPC endpoint(s) for submission", submit_client.num_endpoints());
    let tracker_client = BatchRpcClient::new(&config.base.tracker_rpc_url);
    if config.base.tracker_rpc_url != config.base.rpc_url {
        info!("tracker using separate RPC: {}", config.base.tracker_rpc_url);
    }
    let poll_interval = Duration::from_millis(config.poll_interval_ms);
    let block_tx = block_stream::start_polling(provider.clone(), poll_interval);

    let (pending_tx, pending_rx) = mpsc::channel::<PendingTx>(65536);
    let (record_tx, mut record_rx) = mpsc::channel::<TxLifecycle>(65536);

    let inflight: InflightCounters = Arc::new(
        (0..operators.len())
            .map(|_| AtomicUsize::new(0))
            .collect(),
    );
    let inflight_for_tracker = if config.operator_window > 0 {
        Some(inflight.clone())
    } else {
        None
    };

    let failure_tx = record_tx.clone();

    let block_rx = block_tx.subscribe();
    let tracker_handle = tokio::spawn(async move {
        tx_tracker::run(
            tracker_client,
            pending_rx,
            block_rx,
            record_tx,
            inflight_for_tracker,
        )
        .await;
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
    if !config.skip_steady {
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
    } else {
        info!("▶ steady phase: skipped");
    }

    // --- Phase 2: Ramp ---
    if !config.skip_ramp {
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
    } else {
        info!("▶ ramp phase: skipped");
    }

    // --- Phase 3: BURST ---
    info!(
        "▶ burst: {} txs across {} operator(s), window={}",
        config.burst_size,
        operators.len(),
        if config.operator_window > 0 {
            config.operator_window.to_string()
        } else {
            "unlimited".into()
        }
    );

    let max_mempool = operators.len() * 100;
    if config.operator_window == 0 && config.burst_size > max_mempool {
        warn!(
            "burst_size ({}) exceeds mempool capacity ({} operators × 100 = {}). \
             Excess txs will be rejected. Use --num-operators {} or --operator-window 80",
            config.burst_size,
            operators.len(),
            max_mempool,
            (config.burst_size + 99) / 100
        );
    }

    let burst_start_epoch = Utc::now().timestamp_millis();
    let burst_start = Instant::now();
    let mut burst_ok = 0usize;
    let mut burst_fail = 0usize;

    let burst_chunk = config.burst_chunk.max(1);

    if config.operator_window > 0 {
        // Rolling window: sign and submit in batches, respecting per-operator inflight limits.
        let window = config.operator_window;
        let mut accepted = 0usize;
        let mut op_idx = 0usize;
        let num_ops = operators.len();
        let max_burst_inflight = config.burst_inflight;
        let mut batch_raw: Vec<Vec<u8>> = Vec::with_capacity(burst_chunk);
        let mut batch_meta: Vec<(u64, String, usize)> = Vec::with_capacity(burst_chunk);
        let mut send_tasks: JoinSet<(usize, usize)> = JoinSet::new();

        while accepted < config.burst_size {
            // Find an operator below the window
            let mut found = false;
            for _ in 0..num_ops {
                let current = inflight[op_idx].load(Ordering::Relaxed);
                if current < window {
                    found = true;
                    break;
                }
                op_idx = (op_idx + 1) % num_ops;
            }

            if !found {
                // All operators at capacity — drain some sends and yield
                while let Some(result) = send_tasks.try_join_next() {
                    if let Ok((ok, fail)) = result {
                        burst_ok += ok;
                        burst_fail += fail;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }

            let op = &mut operators[op_idx];
            let (raw, nonce, label) =
                sign_match_order(op, &addresses, &config, gas_price).await?;
            inflight[op_idx].fetch_add(1, Ordering::Relaxed);
            batch_raw.push(raw);
            batch_meta.push((nonce, label, op_idx));
            accepted += 1;
            op_idx = (op_idx + 1) % num_ops;

            // Flush batch when full
            if batch_raw.len() >= burst_chunk || accepted == config.burst_size {
                // Backpressure on HTTP sends
                while send_tasks.len() >= max_burst_inflight {
                    if let Some(Ok((ok, fail))) = send_tasks.join_next().await {
                        burst_ok += ok;
                        burst_fail += fail;
                    }
                }

                let raw_chunk: Vec<Vec<u8>> = batch_raw.drain(..).collect();
                let meta_chunk: Vec<(u64, String, usize)> = batch_meta.drain(..).collect();
                let client = submit_client.clone();
                let ptx = pending_tx.clone();
                let ftx = failure_tx.clone();

                send_tasks.spawn(async move {
                    let t_submit = Instant::now();
                    let t_epoch = Utc::now().timestamp_millis();
                    let mut ok = 0usize;
                    let mut fail = 0usize;

                    match client.batch_send_raw(&raw_chunk).await {
                        Ok(results) => {
                            for (i, result) in results.into_iter().enumerate() {
                                let (nonce, ref label, _oidx) = meta_chunk[i];
                                match result {
                                    Ok(hash) => {
                                        let _ = ptx
                                            .send(PendingTx {
                                                tx_hash: hash,
                                                nonce,
                                                operator: label.clone(),
                                                operator_idx: Some(_oidx),
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
                                        let _ = ftx.send(TxLifecycle {
                                            tx_hash: String::new(),
                                            nonce,
                                            operator: label.clone(),
                                            t_submit_ms: t_epoch,
                                            t_included_ms: None,
                                            latency_ms: None,
                                            block_number: None,
                                            block_timestamp: None,
                                            gas_used: None,
                                            effective_gas_price: None,
                                            status: "send_failed".into(),
                                            phase: "burst".into(),
                                            burst_id: Some(0),
                                            error_message: Some(format!("{e}")),
                                        }).await;
                                        fail += 1;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("burst batch send failed: {e}");
                            let err_msg = format!("{e}");
                            for (nonce, label, _) in &meta_chunk {
                                let _ = ftx.send(TxLifecycle {
                                    tx_hash: String::new(),
                                    nonce: *nonce,
                                    operator: label.clone(),
                                    t_submit_ms: t_epoch,
                                    t_included_ms: None,
                                    latency_ms: None,
                                    block_number: None,
                                    block_timestamp: None,
                                    gas_used: None,
                                    effective_gas_price: None,
                                    status: "send_failed".into(),
                                    phase: "burst".into(),
                                    burst_id: Some(0),
                                    error_message: Some(err_msg.clone()),
                                }).await;
                            }
                            fail = raw_chunk.len();
                        }
                    }
                    (ok, fail)
                });
            }

            if accepted % 500 == 0 {
                info!(
                    "  burst: {accepted}/{} signed, {burst_ok} confirmed ok",
                    config.burst_size
                );
            }
        }

        while let Some(result) = send_tasks.join_next().await {
            if let Ok((ok, fail)) = result {
                burst_ok += ok;
                burst_fail += fail;
            }
        }
    } else {
        // Original dump-all path (operator_window=0)
        let mut signed_burst: Vec<(Vec<u8>, u64, String, usize)> =
            Vec::with_capacity(config.burst_size);
        let txs_per_op = config.burst_size / operators.len();
        let remainder = config.burst_size % operators.len();

        for round in 0..txs_per_op + 1 {
            for (i, op) in operators.iter_mut().enumerate() {
                let count = txs_per_op + if i < remainder { 1 } else { 0 };
                if round >= count {
                    continue;
                }
                let (raw, nonce, label) =
                    sign_match_order(op, &addresses, &config, gas_price).await?;
                signed_burst.push((raw, nonce, label, i));
            }
        }
        info!("  pre-signed {} txs", signed_burst.len());

        struct BurstResult {
            ok: usize,
            failed: Vec<(Vec<u8>, u64, String)>,
        }

        let max_burst_inflight = config.burst_inflight;
        let raw_txs: Vec<Vec<u8>> = signed_burst.iter().map(|(raw, _, _, _)| raw.clone()).collect();

        let chunks: Vec<(Vec<Vec<u8>>, Vec<(u64, String, usize)>)> = raw_txs
            .chunks(burst_chunk)
            .enumerate()
            .map(|(chunk_idx, chunk)| {
                let raw_chunk = chunk.to_vec();
                let start = chunk_idx * burst_chunk;
                let meta: Vec<(u64, String, usize)> = signed_burst[start..start + chunk.len()]
                    .iter()
                    .map(|(_, nonce, label, oidx)| (*nonce, label.clone(), *oidx))
                    .collect();
                (raw_chunk, meta)
            })
            .collect();

        info!(
            "  batch size: {burst_chunk} txs per HTTP POST ({} POST(s))",
            chunks.len()
        );

        let mut burst_tasks: JoinSet<BurstResult> = JoinSet::new();

        for (raw_chunk, meta) in chunks {
            while burst_tasks.len() >= max_burst_inflight {
                if let Some(Ok(r)) = burst_tasks.join_next().await {
                    burst_ok += r.ok;
                    burst_fail += r.failed.len();
                }
            }

            let client = submit_client.clone();
            let ptx = pending_tx.clone();

            burst_tasks.spawn(async move {
                let t_submit = Instant::now();
                let t_epoch = Utc::now().timestamp_millis();
                let mut ok = 0usize;
                let mut failed: Vec<(Vec<u8>, u64, String)> = Vec::new();

                match client.batch_send_raw(&raw_chunk).await {
                    Ok(results) => {
                        for (i, result) in results.into_iter().enumerate() {
                            let (nonce, ref label, oidx) = meta[i];
                            match result {
                                Ok(hash) => {
                                    let _ = ptx
                                        .send(PendingTx {
                                            tx_hash: hash,
                                            nonce,
                                            operator: label.clone(),
                                            operator_idx: Some(oidx),
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
                                    failed.push((raw_chunk[i].clone(), nonce, label.clone()));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("burst batch send failed: {e}");
                        for (i, (nonce, label, _)) in meta.iter().enumerate() {
                            failed.push((raw_chunk[i].clone(), *nonce, label.clone()));
                        }
                    }
                }
                BurstResult { ok, failed }
            });
        }

        let mut retry_queue: Vec<(Vec<u8>, u64, String)> = Vec::new();

        while let Some(result) = burst_tasks.join_next().await {
            if let Ok(r) = result {
                burst_ok += r.ok;
                burst_fail += r.failed.len();
                retry_queue.extend(r.failed);
            }
        }

        // Retry failed txs
        const MAX_RETRIES: usize = 3;
        for retry_round in 0..MAX_RETRIES {
            if retry_queue.is_empty() {
                break;
            }
            let delay = 500u64 * (1 << retry_round);
            info!(
                "  retry round {}: {} txs after {delay}ms backoff",
                retry_round + 1,
                retry_queue.len()
            );
            tokio::time::sleep(Duration::from_millis(delay)).await;

            let retry_raw: Vec<Vec<u8>> =
                retry_queue.iter().map(|(raw, _, _)| raw.clone()).collect();
            let retry_meta: Vec<(u64, String)> = retry_queue
                .iter()
                .map(|(_, nonce, label)| (*nonce, label.clone()))
                .collect();
            retry_queue.clear();

            match submit_client.batch_send_raw(&retry_raw).await {
                Ok(results) => {
                    let t_submit = Instant::now();
                    let t_epoch = Utc::now().timestamp_millis();
                    for (i, result) in results.into_iter().enumerate() {
                        let (nonce, ref label) = retry_meta[i];
                        match result {
                            Ok(hash) => {
                                let _ = pending_tx
                                    .send(PendingTx {
                                        tx_hash: hash,
                                        nonce,
                                        operator: label.clone(),
                                        operator_idx: None,
                                        t_submit,
                                        t_submit_epoch_ms: t_epoch,
                                        phase: "burst".into(),
                                        burst_id: Some(0),
                                    })
                                    .await;
                                burst_ok += 1;
                                burst_fail -= 1;
                            }
                            Err(_) => {
                                retry_queue.push((retry_raw[i].clone(), nonce, label.clone()));
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("retry batch failed: {e}");
                    for (i, (nonce, label)) in retry_meta.into_iter().enumerate() {
                        retry_queue.push((retry_raw[i].clone(), nonce, label));
                    }
                }
            }
        }

        if !retry_queue.is_empty() {
            warn!(
                "  {} txs still failed after {MAX_RETRIES} retries",
                retry_queue.len()
            );
        }
    }

    total_submitted += burst_ok as u64;
    total_failed += burst_fail as u64;

    info!(
        "  burst done: {burst_ok} ok, {burst_fail} failed, spread: {:.0}ms",
        burst_start.elapsed().as_millis()
    );

    // --- Wait for completion ---
    drop(pending_tx);
    drop(failure_tx);
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

    let batch_max = config.batch_max;
    let flush_interval = Duration::from_millis(config.flush_ms);
    let max_inflight = config.phase_inflight;

    struct SignedTx {
        raw: Vec<u8>,
        nonce: u64,
        operator: String,
    }

    let mut batch: Vec<SignedTx> = Vec::with_capacity(batch_max);
    let mut last_flush = Instant::now();
    let mut send_tasks: JoinSet<(u64, u64)> = JoinSet::new();

    loop {
        let phase_elapsed = phase_start.elapsed();
        if phase_elapsed >= duration && batch.is_empty() {
            break;
        }

        // Flush batch if full or flush interval elapsed (and we have txs)
        let should_flush = !batch.is_empty()
            && (batch.len() >= batch_max
                || last_flush.elapsed() >= flush_interval
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
            while send_tasks.len() >= max_inflight {
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
                                            operator_idx: None,
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
            let sleep_dur = (next_send - now).min(flush_interval - last_flush.elapsed().min(flush_interval));
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
        .with_gas_limit(config.gas_limit_match)
        .with_gas_price(gas_price)
        .with_chain_id(config.base.chain_id);

    let tx_envelope = tx.build(&op.wallet).await?;
    let mut encoded = Vec::new();
    alloy::rlp::Encodable::encode(&tx_envelope, &mut encoded);

    Ok((encoded, nonce, op.label.clone()))
}

async fn setup_operators(config: &MarketConfig) -> Result<Vec<OperatorState>> {
    let deployer = &config.base.deployer_key;
    let deployer_addr = deployer.address();
    let batch_client = BatchRpcClient::new(&config.base.rpc_url);

    if config.num_operators == 0 {
        eyre::bail!("need at least 1 operator");
    }

    if config.num_operators == 1 {
        let nonces = batch_client
            .batch_get_pending_nonces(&[deployer_addr])
            .await?;
        return Ok(vec![OperatorState {
            wallet: EthereumWallet::from(deployer.clone()),
            nonce: nonces[0],
            label: "deployer".into(),
        }]);
    }

    let op_keys = wallet::derive_operators(&config.base.mnemonic, config.num_operators)?;
    let op_addrs: Vec<Address> = op_keys.iter().map(|k| k.address()).collect();

    // Batch fetch: balances, pending nonces, and role checks
    let role_calls: Vec<(Address, Vec<u8>)> = op_addrs
        .iter()
        .map(|addr| {
            let call = Vault::hasAnyRoleCall {
                user: *addr,
                roles: OPERATOR_ROLE,
            };
            (config.base.vault, call.abi_encode())
        })
        .collect();

    let (balances, nonces, role_results) = tokio::try_join!(
        batch_client.batch_get_balances(&op_addrs),
        batch_client.batch_get_pending_nonces(&op_addrs),
        batch_client.batch_eth_calls(&role_calls),
    )?;

    let mut operators = Vec::new();
    for (i, key) in op_keys.iter().enumerate() {
        let addr = key.address();

        let has_role = !role_results[i].is_empty() && role_results[i][31] == 1;
        if !has_role {
            eyre::bail!(
                "operator {i} ({addr}) missing OPERATOR_ROLE — run `fund --num-operators {}` first",
                config.num_operators
            );
        }

        if balances[i].is_zero() {
            eyre::bail!(
                "operator {i} ({addr}) has no ETH — run `fund --num-operators {}` first",
                config.num_operators
            );
        }

        operators.push(OperatorState {
            wallet: EthereumWallet::from(key.clone()),
            nonce: nonces[i],
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
