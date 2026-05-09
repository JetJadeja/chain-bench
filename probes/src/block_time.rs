use eyre::Result;
use std::time::Instant;

use crate::rpc::RpcClient;

pub struct BlockTimeConfig {
    pub rpc_url: String,
    pub duration_secs: u64,
}

struct BlockEvent {
    block: u64,
    jump: u64,
    wall_dt_ms: u64,
    chain_ts: u64,
    tx_count: usize,
}

pub async fn run(config: BlockTimeConfig) -> Result<()> {
    let rpc = RpcClient::new(&config.rpc_url);

    println!("Block Time Probe");
    println!("RPC:      {}...", &config.rpc_url[..config.rpc_url.len().min(60)]);
    println!("Duration: {}s", config.duration_secs);
    println!();

    // Measure RPC latency
    let mut latencies = Vec::with_capacity(5);
    for _ in 0..5 {
        let t = Instant::now();
        rpc.get_block_number().await?;
        latencies.push(t.elapsed().as_millis() as u64);
    }
    let avg_lat: u64 = latencies.iter().sum::<u64>() / latencies.len() as u64;
    println!(
        "RPC latency: avg {avg_lat}ms  samples: {:?}",
        latencies,
    );
    println!();

    let mut last_block = rpc.get_block_number().await?;
    let mut last_wall = Instant::now();
    let mut last_chain_ts = rpc
        .get_block(last_block)
        .await?
        .map(|b| b.timestamp)
        .unwrap_or(0);
    let start_block = last_block;

    println!("Starting at block {last_block}");
    println!(
        "{:>8}  {:>10}  {:>5}  {:>8}  {:>12}  {:>8}  {:>5}  {:>6}",
        "Elapsed", "Block", "Jump", "WallDt", "ChainTs", "TsDelta", "Txs", "RpcMs"
    );
    println!("{}", "-".repeat(80));

    let start = Instant::now();
    let mut polls: u64 = 0;
    let mut events: Vec<BlockEvent> = Vec::new();

    while start.elapsed().as_secs() < config.duration_secs {
        let t0 = Instant::now();
        let bn = match rpc.get_block_number().await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("  poll error: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
        };
        let rpc_ms = t0.elapsed().as_millis() as u64;
        polls += 1;

        if bn > last_block {
            let wall_dt_ms = last_wall.elapsed().as_millis() as u64;
            let jump = bn - last_block;

            let block_info = rpc.get_block(bn).await.ok().flatten();
            let chain_ts = block_info.as_ref().map(|b| b.timestamp).unwrap_or(0);
            let tx_count = block_info.as_ref().map(|b| b.tx_count).unwrap_or(0);

            let ts_delta = if last_chain_ts > 0 && chain_ts > 0 {
                format!("{}s", chain_ts - last_chain_ts)
            } else {
                String::new()
            };

            let elapsed = start.elapsed().as_secs_f64();
            println!(
                "{elapsed:7.1}s  {bn:>10}  {jump:>+5}  {wall_dt_ms:>6}ms  {chain_ts:>12}  {ts_delta:>8}  {tx_count:>5}  {rpc_ms:>5}ms",
            );

            events.push(BlockEvent {
                block: bn,
                jump,
                wall_dt_ms,
                chain_ts,
                tx_count,
            });

            last_block = bn;
            last_wall = Instant::now();
            last_chain_ts = chain_ts;
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    println!();
    let total_blocks = last_block - start_block;
    println!("=== Summary ({} transitions, {total_blocks} blocks in {}s) ===", events.len(), config.duration_secs);
    println!("Blocks: {start_block} → {last_block}");
    println!("Polls:  {polls}");

    if events.is_empty() {
        println!("No block transitions observed.");
        return Ok(());
    }

    let mut wall_dts: Vec<u64> = events.iter().map(|e| e.wall_dt_ms).collect();
    wall_dts.sort();
    let n = wall_dts.len();
    println!("Wall-clock interval:");
    println!(
        "  min: {}ms  p50: {}ms  p95: {}ms  max: {}ms",
        wall_dts[0],
        wall_dts[n / 2],
        wall_dts[(n as f64 * 0.95) as usize],
        wall_dts[n - 1],
    );

    let singles = events.iter().filter(|e| e.jump == 1).count();
    let multis = events.iter().filter(|e| e.jump > 1).count();
    let max_jump = events.iter().map(|e| e.jump).max().unwrap_or(0);
    println!("Jumps: {singles} single (+1), {multis} multi, max +{max_jump}");

    let ts_deltas: Vec<u64> = events
        .windows(2)
        .filter_map(|w| {
            if w[0].chain_ts > 0 && w[1].chain_ts > 0 {
                Some(w[1].chain_ts - w[0].chain_ts)
            } else {
                None
            }
        })
        .collect();
    if !ts_deltas.is_empty() {
        let mut sorted = ts_deltas.clone();
        sorted.sort();
        let m = sorted.len();
        println!("Chain timestamp delta:");
        println!(
            "  min: {}s  p50: {}s  max: {}s",
            sorted[0],
            sorted[m / 2],
            sorted[m - 1],
        );
    }

    let mut tx_counts: Vec<usize> = events.iter().map(|e| e.tx_count).collect();
    tx_counts.sort();
    let tc = tx_counts.len();
    println!(
        "Txs/block: min: {}  p50: {}  max: {}",
        tx_counts[0],
        tx_counts[tc / 2],
        tx_counts[tc - 1],
    );

    Ok(())
}
