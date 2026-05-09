use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::U256;
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use eyre::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

use crate::rpc::{RpcClient, SendResult};

pub struct ThroughputConfig {
    pub rpc_url: String,
    pub chain_id: u64,
    pub signer: PrivateKeySigner,
    pub count: usize,
    pub batch_size: usize,
    pub workers: usize,
    pub wait_secs: u64,
}

struct RunResult {
    ok: u64,
    fail: u64,
    elapsed_ms: u64,
    errors: HashMap<String, u64>,
}

pub async fn run(config: ThroughputConfig) -> Result<()> {
    let rpc = Arc::new(RpcClient::new(&config.rpc_url));
    let addr = config.signer.address();

    println!("Submit Throughput Probe");
    println!("Account: {addr}");
    println!("Config:  {} txs, {} batch, {} workers", config.count, config.batch_size, config.workers);
    println!();

    let gas_price = rpc.get_gas_price().await?;

    // --- Test 1: Sequential ---
    let nonce1 = rpc.get_nonce(addr).await?;
    println!("Signing {} txs from nonce {nonce1}...", config.count);
    let raw_txs_1 = sign_batch(&config.signer, nonce1, config.count, gas_price, config.chain_id).await?;
    let batches_1 = chunk_txs(&raw_txs_1, config.batch_size);

    println!();
    println!("{}", "=".repeat(60));
    println!("Test 1: SEQUENTIAL — one batch at a time");
    println!("{}", "=".repeat(60));

    let seq_result = run_sequential(&rpc, &batches_1).await?;
    println!();
    println!(
        "  Sequential: {} ok, {} fail in {}ms",
        seq_result.ok, seq_result.fail, seq_result.elapsed_ms,
    );
    println!(
        "  Offered: {:.0} TPS",
        config.count as f64 / (seq_result.elapsed_ms as f64 / 1000.0),
    );
    print_top_errors(&seq_result.errors);

    // Wait for chain to settle
    println!();
    println!("  Waiting 5s for chain to settle...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // --- Test 2: Parallel ---
    let nonce2 = rpc.get_nonce(addr).await?;
    println!("  Re-signing {} txs from nonce {nonce2}...", config.count);
    let raw_txs_2 = sign_batch(&config.signer, nonce2, config.count, gas_price, config.chain_id).await?;
    let batches_2 = chunk_txs(&raw_txs_2, config.batch_size);

    println!();
    println!("{}", "=".repeat(60));
    println!("Test 2: PARALLEL — {} workers", config.workers);
    println!("{}", "=".repeat(60));

    let par_result = run_parallel(&rpc, batches_2, config.workers).await?;
    println!();
    println!(
        "  Parallel: {} ok, {} fail in {}ms",
        par_result.ok, par_result.fail, par_result.elapsed_ms,
    );
    let par_tps = config.count as f64 / (par_result.elapsed_ms as f64 / 1000.0);
    let seq_tps = config.count as f64 / (seq_result.elapsed_ms as f64 / 1000.0);
    println!("  Offered: {par_tps:.0} TPS  (speedup: {:.1}x)", par_tps / seq_tps);
    print_top_errors(&par_result.errors);

    // Wait for confirmations
    println!();
    println!("{}", "=".repeat(60));
    println!("Waiting {}s for confirmations...", config.wait_secs);
    println!("{}", "=".repeat(60));
    tokio::time::sleep(std::time::Duration::from_secs(config.wait_secs)).await;

    let final_nonce = rpc.get_confirmed_nonce(addr).await?;
    let total_confirmed = final_nonce.saturating_sub(nonce1);
    println!("  Nonce: {nonce1} → {final_nonce} ({total_confirmed} total confirmed)");
    println!();
    println!("SUMMARY:");
    println!("  Sequential: {seq_tps:.0} TPS");
    println!("  Parallel:   {par_tps:.0} TPS ({} workers)", config.workers);

    Ok(())
}

async fn sign_batch(
    signer: &PrivateKeySigner,
    start_nonce: u64,
    count: usize,
    gas_price: u128,
    chain_id: u64,
) -> Result<Vec<Vec<u8>>> {
    let addr = signer.address();
    let wallet = EthereumWallet::from(signer.clone());
    let mut raw_txs = Vec::with_capacity(count);
    for i in 0..count {
        let tx = TransactionRequest::default()
            .with_to(addr)
            .with_value(U256::ZERO)
            .with_nonce(start_nonce + i as u64)
            .with_gas_limit(60_000)
            .with_gas_price(gas_price)
            .with_chain_id(chain_id);
        let envelope = tx.build(&wallet).await?;
        let mut encoded = Vec::new();
        alloy::rlp::Encodable::encode(&envelope, &mut encoded);
        raw_txs.push(encoded);
    }
    Ok(raw_txs)
}

fn chunk_txs(raw_txs: &[Vec<u8>], batch_size: usize) -> Vec<Vec<Vec<u8>>> {
    raw_txs
        .chunks(batch_size)
        .map(|c| c.to_vec())
        .collect()
}

async fn run_sequential(rpc: &RpcClient, batches: &[Vec<Vec<u8>>]) -> Result<RunResult> {
    let mut ok = 0u64;
    let mut fail = 0u64;
    let mut errors: HashMap<String, u64> = HashMap::new();

    let start = Instant::now();
    for (i, batch) in batches.iter().enumerate() {
        let t = Instant::now();
        let results = rpc.batch_send_raw(batch).await?;
        let dt = t.elapsed().as_millis();

        let (batch_ok, batch_fail) = count_results(&results, &mut errors);
        ok += batch_ok;
        fail += batch_fail;
        println!("  Batch {i}: {batch_ok} ok, {batch_fail} fail, {dt}ms");
    }

    Ok(RunResult {
        ok,
        fail,
        elapsed_ms: start.elapsed().as_millis() as u64,
        errors,
    })
}

async fn run_parallel(
    rpc: &Arc<RpcClient>,
    batches: Vec<Vec<Vec<u8>>>,
    workers: usize,
) -> Result<RunResult> {
    let mut ok = 0u64;
    let mut fail = 0u64;
    let mut errors: HashMap<String, u64> = HashMap::new();

    let mut batch_iter = batches.into_iter().enumerate();
    let mut tasks: JoinSet<(usize, u64, u64, HashMap<String, u64>, u64)> = JoinSet::new();

    let start = Instant::now();

    loop {
        while tasks.len() < workers {
            let Some((idx, batch)) = batch_iter.next() else {
                break;
            };
            let rpc = Arc::clone(rpc);
            tasks.spawn(async move {
                let t = Instant::now();
                let mut errs: HashMap<String, u64> = HashMap::new();
                let (batch_ok, batch_fail) = match rpc.batch_send_raw(&batch).await {
                    Ok(results) => count_results(&results, &mut errs),
                    Err(e) => {
                        *errs.entry(e.to_string()).or_default() += batch.len() as u64;
                        (0, batch.len() as u64)
                    }
                };
                (idx, batch_ok, batch_fail, errs, t.elapsed().as_millis() as u64)
            });
        }

        let Some(result) = tasks.join_next().await else {
            break;
        };

        let (idx, batch_ok, batch_fail, batch_errors, dt) = result?;
        println!("  Batch {idx}: {batch_ok} ok, {batch_fail} fail, {dt}ms");
        ok += batch_ok;
        fail += batch_fail;
        for (msg, count) in batch_errors {
            *errors.entry(msg).or_default() += count;
        }
    }

    Ok(RunResult {
        ok,
        fail,
        elapsed_ms: start.elapsed().as_millis() as u64,
        errors,
    })
}

fn count_results(results: &[SendResult], errors: &mut HashMap<String, u64>) -> (u64, u64) {
    let mut ok = 0u64;
    let mut fail = 0u64;
    for r in results {
        match r {
            SendResult::Accepted(_) => ok += 1,
            SendResult::Rejected(msg) => {
                fail += 1;
                *errors.entry(msg.clone()).or_default() += 1;
            }
        }
    }
    (ok, fail)
}

fn print_top_errors(errors: &HashMap<String, u64>) {
    if errors.is_empty() {
        return;
    }
    let mut sorted: Vec<_> = errors.iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(a.1));
    println!("  Errors:");
    for (msg, count) in sorted.iter().take(5) {
        println!("    {count:>4}x  {msg}");
    }
}
