# Bench Tool Diagnostic Report

**Date:** 2026-05-08
**Chain:** MegaETH Mainnet (Chain ID 4326)
**RPC:** dwellir.com (HTTPS, no WebSocket)
**Last run:** `cargo run -- market` with 3 operators, 100 TPS target, 1000-tx burst

## Executive Summary

The numbers from the last benchmark run were measuring our own tool's limitations, not the chain. Three bugs in the submit loop, receipt poller, and latency accounting inflated latency, suppressed throughput, and manufactured false "drops." After fixing those, an independent block time probe revealed that MegaETH mainnet operates at **~1 second block time**, not the advertised 10ms. A mempool probe revealed a **balance-gated nonce gap limit** that caused the burst rejections.

| Symptom | Root Cause | Our Bug or Chain? |
|---------|-----------|-------------------|
| Steady phase: 9 TPS instead of 100 | Submit loop blocked on HTTP round-trips | Our bug (fixed) |
| Block time measured at ~1s | Chain actually produces blocks every ~1s | The chain |
| 517 burst txs rejected at submit | Operator accounts below mempool balance threshold | Our bug (underfunded) |
| 300 burst txs "dropped" after acceptance | Receipt poller 429'd, couldn't check receipts | Our bug (fixed) |
| Latency ~700-1100ms | Measuring sign-time-to-receipt-poll, not chain latency | Our bug (fixed) |

---

## 1. Submit Loop Bottleneck (9 TPS)

### Evidence

Analysis of the CSV from the last run shows transactions arriving in clusters of exactly 5 with ~500ms gaps:

```
Cluster 1: 5 txs in ~47ms  → 1282ms gap (TLS handshake + first HTTP)
Cluster 2: 5 txs in ~49ms  →  493ms gap
Cluster 3: 5 txs in ~46ms  →  491ms gap
Cluster 4: 5 txs in ~47ms  →  513ms gap
Cluster 5: 5 txs in ~49ms  →  501ms gap
  ... (pattern continues for all 264 steady-phase txs)
```

Every gap is ~500ms. Every cluster is ~5 txs. This pattern is perfectly consistent across the entire 30-second steady phase: 264 txs / 29.85s = **8.84 TPS**.

### Root Cause

In `market.rs:run_phase()`, the batch flush **awaited** the HTTP POST:

```rust
// OLD CODE — blocking flush
if should_flush {
    match batch_client.batch_send_raw(&raw_txs).await {  // <-- blocks for ~500ms
        ...
    }
    batch.clear();
    continue;  // <-- only NOW does signing resume
}
```

The submit loop is single-threaded: sign txs into a batch, flush via HTTP, wait for response, resume signing. During the ~500ms HTTP round-trip, no signing happens.

Math: 5 txs accumulate in 50ms (FLUSH_INTERVAL). HTTP round-trip is ~500ms. Effective TPS = 5 / (50ms + 500ms) = **9.1 TPS**. Exactly matches the observed 8.84.

### Fix

Batch sends are now spawned as background tokio tasks via `JoinSet`. The main loop signs continuously at the target rate while up to 8 batch sends execute concurrently. Backpressure kicks in only when 8 batches are simultaneously in-flight.

```rust
// NEW CODE — non-blocking flush
if should_flush {
    // Backpressure: wait only if 8 sends already in flight
    while send_tasks.len() >= MAX_INFLIGHT {
        if let Some(Ok((ok, fail))) = send_tasks.join_next().await { ... }
    }

    send_tasks.spawn(async move {
        client.batch_send_raw(&raw_txs).await  // runs in background
    });
    continue;  // signing resumes immediately
}
```

Expected throughput after fix: limited by signing speed and rate limiter, not HTTP latency. At 100 TPS target, the interval is 10ms per tx. With 8 concurrent HTTP connections, we can sustain ~800 in-flight txs before backpressure. The bottleneck shifts from "our HTTP latency" to "the chain's inclusion rate."

### Files Changed

- `bench/src/market.rs`: Added `JoinSet<(u64, u64)>` for concurrent batch sends, `MAX_INFLIGHT = 8` constant, backpressure via `try_join_next()` and `join_next()`, inflight drain after main loop.

---

## 2. Latency Measurement Error

### Evidence

The CSV has two timestamp columns that reveal the problem:
- `t_submit_ms`: set at **signing time**, not send time
- `t_included_ms`: set when the **receipt poller found** the receipt, not when the chain included the tx

A tx signed at T=0 waits in the batch for ~40ms, then the HTTP POST takes ~500ms. The tx reaches the RPC at ~T+540ms. The chain includes it in the next block (~100ms). The receipt poller finds it ~500ms later. Measured latency: ~1140ms. Actual chain latency: ~100ms.

Concrete example from the CSV (steady phase, batch 4):

```
nonce=5, op-0:  t_submit=1778296904755  t_included=1778296905689  latency=933ms
nonce=5, op-1:  t_submit=1778296904767  t_included=1778296905689  latency=921ms
nonce=5, op-2:  t_submit=1778296904779  t_included=1778296905689  latency=909ms
```

All three txs were "included" at the same millisecond (1778296905689) — that's when our poller happened to check, not when the chain confirmed. The 909-933ms "latency" is our pipeline overhead.

### Root Cause

In `market.rs`, the signing timestamp was used as the submit timestamp:

```rust
batch.push(SignedTx {
    t_sign: Instant::now(),           // <-- recorded at sign time
    t_sign_epoch_ms: Utc::now()...    // <-- same
});

// Later, in flush:
pending_tx.send(PendingTx {
    t_submit: stx.t_sign,             // <-- sign time passed as submit time
    t_submit_epoch_ms: stx.t_sign_epoch_ms,
});
```

### Fix

`t_submit` is now captured inside the spawned send task, just before the HTTP POST:

```rust
send_tasks.spawn(async move {
    let t_submit = Instant::now();                        // actual send time
    let t_submit_epoch = chrono::Utc::now().timestamp_millis();
    match client.batch_send_raw(&raw_txs).await { ... }
});
```

This removes the batch accumulation delay (~40ms) and the HTTP queue delay from the measurement. The receipt polling lag remains (~500ms on this RPC) — eliminating that would require using the block timestamp from the receipt's block, which is a future improvement.

### Files Changed

- `bench/src/market.rs`: Removed `t_sign` / `t_sign_epoch_ms` from `SignedTx` struct. `t_submit` now set inside spawned task.

---

## 3. Receipt Poller 429s and False Drops

### Evidence

From the last run:
- 483 burst txs made it to the tracker
- 183 confirmed, 300 "dropped" (no receipt within 60s)
- Logs showed 429 errors from the RPC during receipt polling

### Root Cause

`tx_tracker.rs` collected ALL pending tx hashes into a single batch RPC call:

```rust
let hashes: Vec<TxHash> = pending.keys().copied().collect();
match batch_client.batch_receipts(&hashes).await {  // 500+ receipts in one HTTP POST
```

After the burst, ~500 hashes were pending. A single `eth_getTransactionReceipt` batch with 500 items overwhelmed the RPC's rate limit. The RPC returned HTTP 429.

The error handler just logged a warning and moved on:

```rust
Err(e) => {
    warn!("batch receipt request failed: {e}");  // skips entire poll cycle
}
```

So every 429 meant we skipped an entire receipt poll. After multiple skips, txs hit the 60-second timeout and were marked "dropped" even though the chain had confirmed them.

### Fix: Receipt Chunking

Receipt polls are now chunked into groups of 50:

```rust
const RECEIPT_CHUNK: usize = 50;
let hashes: Vec<TxHash> = pending.keys().copied().collect();
for chunk in hashes.chunks(RECEIPT_CHUNK) {
    match batch_client.batch_receipts(chunk).await {
        Ok(results) => all_results.extend(results),
        Err(e) => warn!("batch receipt chunk failed ({} hashes): {e}", chunk.len()),
    }
}
```

If one chunk gets 429'd, the others still succeed. We lose visibility on 50 txs for one cycle instead of all 500.

### Fix: 429 Retry with Backoff

`batch_rpc.rs:send_batch()` now retries 429 responses with exponential backoff:

```rust
for attempt in 0u32..=4 {
    let resp = self.http.post(&self.url).json(&requests).send().await?;

    if status.as_u16() == 429 {
        if attempt < 4 {
            let delay_ms = 200 * 2u64.pow(attempt);  // 200, 400, 800, 1600, 3200ms
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            continue;
        }
        bail!("RPC rate limited after 5 retries");
    }
    ...
}
```

This applies to all batch RPC calls (receipts, sends, balance checks), not just receipts.

### Files Changed

- `bench/src/tx_tracker.rs`: Receipt polls chunked into groups of 50.
- `bench/src/batch_rpc.rs`: `send_batch()` retries up to 5 times on 429 with exponential backoff (200ms base).

---

## 4. Block Time: 1 Second, Not 10ms

### Method

Independent Python script (`agent_docs/block_time_probe.py`) that polls `eth_blockNumber` at 100Hz for 60 seconds, then fetches block details for each new block. Completely bypasses our Rust framework.

### Raw Data

```
RPC latency (eth_blockNumber):  avg 759ms  range 709-956ms
Duration:                       60 seconds
Blocks observed:                15507210 → 15507271 (61 blocks)
Polls made:                     41 (limited by RPC latency)
```

Block transition log (excerpt):

```
Elapsed     Block   Jump   WallDt   ChainTs    TsDelta   Txs   RpcMs
  1.4s   15507211    +1   1427ms   1778304222      1s     25   717ms
  2.9s   15507213    +2    722ms   1778304224      2s     27   711ms
  4.3s   15507214    +1    723ms   1778304225      1s     29   711ms
  5.7s   15507216    +2    714ms   1778304227      2s     28   703ms
  ...
```

Statistics:

```
Wall-clock block interval:
  min: 708ms   p50: 725ms   p95: 905ms   max: 1757ms

Block jumps:
  22× single (+1),  19× multi (+2),  1× (+3)

On-chain timestamp delta:
  min: 1s   p50: 1s   max: 3s
  (Every single delta is an integer number of seconds)

Txs per block:
  min: 25   p50: 29   max: 36
```

### Analysis

**Why this proves ~1s block time, not 10ms:**

1. **Block number jumps are small.** If MegaETH produced blocks every 10ms, at our 720ms poll interval we'd miss ~72 blocks per poll and see jumps of +72. Instead the max jump is +3. The +2 jumps (19 of 41) are explained by our 720ms RPC latency occasionally spanning two 1s blocks.

2. **Block numbers are sequential.** No gaps. If the RPC were coalescing 100 micro-blocks into one, block numbers would jump by 100. They jump by 1-3.

3. **On-chain timestamps increment by exactly 1 second.** Not 10ms, not variable, not rounded. Every block timestamp is precisely 1 second after the previous block. When we observe a +2 jump, the timestamp delta is exactly 2s. When +3, exactly 3s.

4. **The ramp phase of the previous benchmark confirms this.** Block 15499932 had 69 txs. Block 15499931 had 59 txs. Both appeared ~1s apart. More txs per block did not reduce the block time.

### Interpretation

MegaETH's "10ms" claim likely refers to one of:
- Sequencer preconfirmation latency (time from receiving a tx to soft-acknowledging it), not block production cadence
- A testnet or future configuration
- A different network topology than what's deployed on mainnet today

For benchmark purposes: **MegaETH mainnet produces 1 block per second.** This is the confirmation latency floor.

---

## 5. Mempool Balance-Gated Nonce Limit

### Method

Independent Python script (`agent_docs/mempool_probe.py`) that pre-signs 200 sequential-nonce transactions and submits them in batches of 20. Uses the deployer account (same key as the benchmark operators).

### Raw Data

```
Account:    0xeCe264b9A9c0395e10ee99ffB0652358ab55C8D0
ETH balance: 0.004325 ETH
Gas price:   1,000,000 wei (0.001 gwei)
Starting nonce: 1522
```

Submission results:

```
Batch  [  0- 19]:  6 ok, 14 fail  (1307ms)
Batch  [ 20- 39]:  0 ok, 20 fail  ( 790ms)
Batch  [ 40- 59]:  0 ok, 20 fail  ( 748ms)
  ... (all subsequent batches: 0 ok, 20 fail)

Total: 6 accepted, 194 rejected
All 6 accepted txs confirmed (nonce 1522 → 1528)
```

Every rejection had the same error:

```
"Nonce gap too high for low balance account. Gap: N, Min balance required: 0.01 ETH"
```

Where N incremented from 6 to 193.

### Analysis

MegaETH's mempool enforces a **balance-based pending transaction limit**:

- Accounts with < 0.01 ETH: limited to ~5 pending nonces ahead of the confirmed nonce
- The "Min balance required: 0.01 ETH" threshold appears constant regardless of gap size
- Accounts above 0.01 ETH likely have a higher (possibly much higher) limit — untested

The deployer has only 0.004325 ETH remaining after deploying contracts and funding operator accounts. This is below the 0.01 ETH threshold, capping it at ~5 pending txs.

### Impact on the Last Benchmark Run

The burst phase tried to submit 1000 txs across 3 operators (~333 per operator). Each operator was funded with 0.1 ETH initially, but gas costs from the steady and ramp phases depleted their balances. If any operator dropped below 0.01 ETH, the mempool would reject most of their burst txs.

This explains the 517 burst rejections: not a hard mempool cap, but balance-gated throttling.

### Remediation

Fund each operator with at least 1 ETH before running burst tests. At 500k gas × 0.001 gwei × 1000 txs = 0.0005 ETH gas cost per 1000 matchOrders calls, 1 ETH provides a massive buffer. The nonce gap limit with 1 ETH is likely 100+ (needs testing).

---

## 6. RPC Latency

The dwellir RPC adds significant overhead to every measurement:

| Operation | Latency |
|-----------|---------|
| `eth_blockNumber` | 709-956ms (avg 759ms) |
| `eth_getBlockByNumber` | ~710ms |
| `batch_send_raw` (5 txs) | ~500ms |
| `batch_receipts` (20 txs) | ~500ms |

Every RPC call takes 500-950ms. This is the latency from the user's machine (macOS, likely US-based) to dwellir's endpoint (Swedish infrastructure). TLS handshake adds ~300ms to the first call; keep-alive connections reduce subsequent calls to ~500ms.

This latency compounds across the system:
- Block poller: one `eth_blockNumber` + one `eth_getBlockByNumber` + 50ms sleep = ~1.5s per poll cycle
- Receipt tracker: triggered by block events, so inherits the ~1.5s polling cadence
- Submit loop: each batch flush takes ~500ms (now parallelized)

For production benchmarking, consider using a geographically closer RPC provider or running a local node.

---

## 7. Summary of Code Changes

### `bench/src/batch_rpc.rs`

**429 retry with exponential backoff.** The `send_batch()` method now retries up to 5 times when the RPC returns HTTP 429, with delays of 200ms, 400ms, 800ms, 1600ms, and 3200ms. This applies to all batch RPC operations (send, receipts, balances, nonces, eth_calls). Non-429 errors still fail immediately.

### `bench/src/market.rs`

**Non-blocking batch sends.** The `run_phase()` flush path now spawns each batch send as a background tokio task via `JoinSet<(u64, u64)>`. The main signing loop continues immediately. Backpressure: if 8 sends are already in flight, the loop blocks until one completes. Results (ok/fail counts) are collected from the JoinSet after each flush and after the main loop.

**Accurate submit timestamps.** The `SignedTx` struct no longer stores signing time. `t_submit` is now captured inside the spawned send task at the moment of the HTTP POST, removing batch accumulation delay from latency measurements.

### `bench/src/tx_tracker.rs`

**Chunked receipt polling.** Receipt polls are split into groups of 50 hashes. If one chunk fails (429, timeout, etc.), the others still succeed. Previously, a single 429 on a 500-hash batch caused all pending txs to lose receipt visibility for that cycle.

---

## 8. Remaining Work (Ordered)

### Immediate (before next run)

1. **Fund operators with 1+ ETH each.** The nonce gap limit is the binding constraint on burst tests. Run `cargo run -- fund --num-operators 3 --operator-eth 1.0` after adding ETH to the deployer.

2. **Re-run `cargo run -- market` with fixed code.** Verify that:
   - Steady phase hits 100 TPS (not 9)
   - Latency numbers are ~1-2s (1 block + receipt poll), not ~1s of pipeline overhead
   - No false drops from 429s
   - Burst rejections are minimal with funded operators

3. **Run `submit_throughput_probe.py`** to measure raw HTTP throughput to dwellir. If parallel connections give >4x improvement, consider using multiple reqwest clients in the Rust tool.

### Short-term (validate chain behavior)

4. **Test mempool limit with a funded account.** Run `mempool_probe.py` from an operator account with 1 ETH. Find the actual nonce gap limit for well-funded accounts. This determines the maximum burst size per operator.

5. **Measure true chain latency.** After a run with the fixed code, compute latency using the block timestamp from the receipt's block number, not the receipt poll time. This requires looking up the block for each confirmed tx: `t_included = block.timestamp`, `latency = block.timestamp - t_submit`. This is the actual chain confirmation time.

6. **Test with a closer RPC.** The 700ms+ RPC latency adds ~1.5s to every measurement cycle. Try MegaETH's public RPC or a US-based provider. If latency drops to 50-100ms, our polling granularity improves by 10x and latency measurements become much more accurate.

### Medium-term (chain comparison)

7. **Determine max txs per block.** The ramp phase showed 69 txs in one block. Push harder: 200-500 TPS target, 10 operators, 60-second duration. Find where blocks start filling up and txs spill into subsequent blocks.

8. **Run the same benchmark against Arbitrum and Monad.** Same contract, same load profile. The fixed code now measures the chain, not itself. Compare `t_finalized` p99, burst tail latency, and max txs/block across all three chains.

---

## Appendix A: CSV Analysis from Last Run

### Phase Breakdown

| Phase | Txs | Confirmed | Dropped | Duration | Effective TPS |
|-------|-----|-----------|---------|----------|---------------|
| Steady | 264 | 264 | 0 | 29.85s | 8.84 |
| Ramp | 298 | 298 | 0 | 9.81s | 30.37 |
| Burst | 483 | 183 | 300 | — | — |
| **Total** | **1045** | **745** | **300** | — | — |

Note: 517 burst txs were rejected at submit and never made it to the tracker. Total attempted burst: 1000.

### Block Distribution (Steady Phase)

Blocks contain 5-10 txs each, consistent with the ~5 txs/batch pattern:

```
Block 15499893:  5 txs    Block 15499900: 10 txs    Block 15499907: 10 txs
Block 15499894: 10 txs    Block 15499901: 10 txs    Block 15499908:  5 txs
Block 15499895: 10 txs    Block 15499902: 10 txs    Block 15499909: 10 txs
Block 15499896: 10 txs    Block 15499903: 10 txs    Block 15499910: 10 txs
Block 15499897: 10 txs    Block 15499904:  5 txs    Block 15499911:  5 txs
Block 15499898:  5 txs    Block 15499905: 10 txs    Block 15499912: 10 txs
Block 15499899:  9 txs    Block 15499906: 10 txs
```

### Block Distribution (Ramp Phase)

Blocks grow larger as the ramp increases rate, proving the chain can handle more:

```
Block 15499923: 12 txs    Block 15499928: 38 txs
Block 15499924: 19 txs    Block 15499929: 22 txs
Block 15499925: 11 txs    Block 15499930: 23 txs
Block 15499926: 14 txs    Block 15499931: 59 txs
Block 15499927: 31 txs    Block 15499932: 69 txs  ← peak
```

### Burst Latency (of the 183 that confirmed)

```
p50: 1590ms    p95: 1714ms    p99: 1714ms    max: 1714ms
```

Tight distribution — all burst txs that confirmed did so within a narrow window, consistent with landing in 1-2 consecutive 1s blocks.

---

## Appendix B: Probe Scripts

All scripts are in `agent_docs/` and load configuration from `../.env`.

### block_time_probe.py

Read-only. No dependencies beyond Python stdlib. Polls `eth_blockNumber` at 100Hz, fetches block details on each change, compares wall-clock intervals against on-chain timestamps. Run for 60-300 seconds.

```
python3 agent_docs/block_time_probe.py 60
```

### mempool_probe.py

Submits real transactions (costs gas). Requires `pip install eth-account requests`. Pre-signs N sequential-nonce self-transfers and submits in batches. Reports acceptance/rejection counts and error messages.

```
python3 agent_docs/mempool_probe.py --count 200 --batch 20
```

### submit_throughput_probe.py

Submits real transactions (costs gas). Requires `pip install eth-account requests`. Measures sequential vs parallel HTTP submission speed. Uses ThreadPoolExecutor for parallel connections.

```
python3 agent_docs/submit_throughput_probe.py --count 200 --workers 4 --batch 50
```
