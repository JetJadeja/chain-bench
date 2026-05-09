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

pub struct MempoolProbeConfig {
    pub rpc_url: String,
    pub chain_id: u64,
    pub signer: PrivateKeySigner,
    pub count: usize,
    pub batch_size: usize,
    pub workers: usize,
    pub wait_secs: u64,
}

struct BatchResult {
    range_start: usize,
    range_end: usize,
    accepted: u64,
    rejected: u64,
    errors: HashMap<String, u64>,
    elapsed_ms: u64,
}

pub async fn run(config: MempoolProbeConfig) -> Result<()> {
    let rpc = Arc::new(RpcClient::new(&config.rpc_url));
    let addr = config.signer.address();

    let balance = rpc.get_balance(addr).await?;
    let nonce = rpc.get_nonce(addr).await?;
    let gas_price = rpc.get_gas_price().await?;

    let eth = format_eth(balance);
    println!("Account:   {addr}");
    println!("Balance:   {eth} ETH");
    println!("Nonce:     {nonce}");
    println!("Gas price: {gas_price} wei");
    println!();

    let n = config.count;
    println!("Pre-signing {n} txs (nonces {nonce}..{})...", nonce + n as u64 - 1);
    let t0 = Instant::now();

    let wallet = EthereumWallet::from(config.signer);

    let mut raw_txs: Vec<Vec<u8>> = Vec::with_capacity(n);
    for i in 0..n {
        let tx = TransactionRequest::default()
            .with_to(addr)
            .with_value(U256::ZERO)
            .with_nonce(nonce + i as u64)
            .with_gas_limit(60_000)
            .with_gas_price(gas_price)
            .with_chain_id(config.chain_id);

        let envelope = tx.build(&wallet).await?;
        let mut encoded = Vec::new();
        alloy::rlp::Encodable::encode(&envelope, &mut encoded);
        raw_txs.push(encoded);
    }

    let sign_ms = t0.elapsed().as_millis();
    println!("  Signed in {sign_ms}ms ({}/sec)", n as u128 * 1000 / sign_ms.max(1));
    println!();

    let batches: Vec<(usize, Vec<Vec<u8>>)> = raw_txs
        .chunks(config.batch_size)
        .enumerate()
        .map(|(i, chunk)| (i * config.batch_size, chunk.to_vec()))
        .collect();

    let total_batches = batches.len();
    println!(
        "Submitting {n} txs: {total_batches} batches of ≤{}, {}/batch concurrent",
        config.batch_size, config.workers,
    );

    let submit_start = Instant::now();
    let mut all_results: Vec<BatchResult> = Vec::with_capacity(total_batches);
    let mut total_accepted: u64 = 0;
    let mut total_rejected: u64 = 0;
    let mut all_errors: HashMap<String, u64> = HashMap::new();

    let mut batch_iter = batches.into_iter();
    let mut tasks: JoinSet<BatchResult> = JoinSet::new();

    loop {
        while tasks.len() < config.workers {
            let Some((range_start, batch)) = batch_iter.next() else {
                break;
            };
            let rpc = Arc::clone(&rpc);
            let range_end = range_start + batch.len() - 1;
            tasks.spawn(async move {
                let t = Instant::now();
                let results = rpc.batch_send_raw(&batch).await;
                let elapsed_ms = t.elapsed().as_millis() as u64;

                let mut accepted = 0u64;
                let mut rejected = 0u64;
                let mut errors: HashMap<String, u64> = HashMap::new();

                match results {
                    Ok(send_results) => {
                        for r in send_results {
                            match r {
                                SendResult::Accepted(_) => accepted += 1,
                                SendResult::Rejected(msg) => {
                                    rejected += 1;
                                    *errors.entry(msg).or_default() += 1;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        rejected = batch.len() as u64;
                        *errors.entry(e.to_string()).or_default() += batch.len() as u64;
                    }
                }

                BatchResult {
                    range_start,
                    range_end,
                    accepted,
                    rejected,
                    errors,
                    elapsed_ms,
                }
            });
        }

        let Some(result) = tasks.join_next().await else {
            break;
        };

        let br = result?;
        println!(
            "  [{:>4}-{:>4}]  {} ok, {} fail, {}ms",
            br.range_start, br.range_end, br.accepted, br.rejected, br.elapsed_ms,
        );
        total_accepted += br.accepted;
        total_rejected += br.rejected;
        for (msg, count) in &br.errors {
            *all_errors.entry(msg.clone()).or_default() += count;
        }
        all_results.push(br);
    }

    let submit_ms = submit_start.elapsed().as_millis();

    println!();
    println!("=== Results ===");
    println!("Submitted: {n}");
    println!("Accepted:  {total_accepted}  Rejected: {total_rejected}");
    println!("Time:      {submit_ms}ms ({:.0} offered/sec)", n as f64 / (submit_ms as f64 / 1000.0));
    println!();

    if !all_errors.is_empty() {
        println!("Errors:");
        let mut sorted: Vec<_> = all_errors.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        for (msg, count) in sorted {
            println!("  {count:>4}x  {msg}");
        }
        println!();
    }

    // Find where rejections start
    all_results.sort_by_key(|r| r.range_start);
    for br in &all_results {
        if br.rejected > 0 {
            let first_reject_nonce = nonce + br.range_start as u64;
            println!(
                "First rejection at nonce ~{first_reject_nonce} (batch [{}-{}])",
                br.range_start, br.range_end
            );
            println!("  → Mempool accepts ~{total_accepted} pending txs from this sender at {eth} ETH");
            break;
        }
    }

    if total_rejected == 0 {
        println!("No rejections — mempool limit is >{n} at {eth} ETH");
        println!("  Run with a higher --count to find the ceiling");
    }

    println!();
    println!("Waiting {}s for confirmations...", config.wait_secs);
    tokio::time::sleep(std::time::Duration::from_secs(config.wait_secs)).await;

    let final_nonce = rpc.get_confirmed_nonce(addr).await?;
    let confirmed = final_nonce - nonce;
    println!("Nonce: {nonce} → {final_nonce} ({confirmed} txs confirmed on-chain)");

    if confirmed < total_accepted {
        let stuck = total_accepted - confirmed;
        println!("  {stuck} accepted txs still pending — nonce gap or chain backlog");
    }

    Ok(())
}

fn format_eth(wei: U256) -> String {
    let wei_u128: u128 = wei.try_into().unwrap_or(u128::MAX);
    format!("{:.6}", wei_u128 as f64 / 1e18)
}
