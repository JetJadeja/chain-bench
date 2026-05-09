# Chain Evaluation Tool — Design

## The Question We're Answering

How late can we accept trades?

The matcher commits to a fill off-chain. The on-chain `matchOrders` tx must confirm before resolution fires. If it doesn't, we eat the loss. The p99 confirmation latency during a settlement burst directly bounds the trade cutoff time. Every millisecond we shave off that tail is another millisecond of trading volume.

We need a tool that measures the specific failure mode: slam N settlements onto the chain in under a second, and observe what happens. Everything else — steady-state latency, gas costs, block monitoring — is supporting context for the burst result.

---

## Data Model

### TxLifecycle — the atomic measurement

Every test mode produces the same thing: a transaction with lifecycle timestamps.

```rust
struct TxLifecycle {
    // Identity
    tx_hash: TxHash,
    nonce: u64,
    wallet: Address,          // which key signed it

    // Timestamps (all Instant, converted to epoch ms for output)
    t_submit: Instant,        // when eth_sendRawTransaction returned the hash
    t_mempool: Option<Instant>,   // first non-null eth_getTransactionByHash
    t_included: Option<Instant>,  // first non-null eth_getTransactionReceipt
    t_finalized: Option<Instant>, // block crossed finality threshold

    // Receipt data (populated at t_included)
    block_number: Option<u64>,
    block_timestamp: Option<u64>, // chain's clock, not ours
    gas_used: Option<u64>,
    effective_gas_price: Option<u128>,
    status: TxStatus,

    // Context
    phase: String,            // which test phase generated this tx
    burst_id: Option<u64>,    // which burst this belonged to, if any
}

enum TxStatus {
    Pending,    // submitted, not yet included
    Confirmed,  // receipt with status=true
    Reverted,   // receipt with status=false
    Dropped,    // no receipt after timeout
}
```

Key difference from existing code: four timestamps instead of two (submit + confirm), plus the block timestamp so we can decompose latency into "chain processing" vs "RPC propagation."

### BlockRecord — chain health snapshot

```rust
struct BlockRecord {
    block_number: u64,
    block_hash: B256,
    parent_hash: B256,
    timestamp: u64,           // chain's reported time
    observed_at: Instant,     // when we saw this block
    gas_used: u64,
    gas_limit: u64,
    tx_count: u32,
    base_fee: Option<u128>,

    // Derived
    utilization: f64,         // gas_used / gas_limit
    interval_ms: Option<u64>, // time since previous block (from chain timestamps)
}
```

### BurstResult — per-burst summary

```rust
struct BurstResult {
    burst_id: u64,
    n: usize,                      // how many txs in the burst
    mode: BurstMode,               // single-wallet or multi-wallet
    t_burst_start: Instant,        // before first submit
    t_all_submitted: Instant,      // after last submit returned
    t_last_confirmed: Option<Instant>,

    // Derived
    submission_spread_ms: u64,     // how long to fire all N
    completion_time_ms: Option<u64>, // first submit to last confirm
    max_single_tx_latency_ms: Option<u64>,
    confirmed: usize,
    reverted: usize,
    dropped: usize,

    // Chain state at time of burst
    chain_utilization: f64,        // avg gasUsed/gasLimit of recent blocks
    chain_tps: f64,                // recent on-chain txs/sec
}
```

---

## Concurrency Architecture

### The Pipeline

```
                                          ┌──────────────────┐
                                          │   Block Stream   │
                                          │  (newHeads sub   │
                                          │   or polling)    │
                                          └────────┬─────────┘
                                                   │ broadcast
                                      ┌────────────┼────────────┐
                                      ▼            ▼            ▼
┌────────────┐  mpsc   ┌─────────────────┐   ┌──────────┐   ┌──────────┐
│ Submitter  │────────▶│    Tracker       │   │  Block   │   │ Finality │
│ (per mode) │ Pending │ (receipt checks  │   │ Monitor  │   │ Watcher  │
└────────────┘   Tx    │  on new blocks)  │   │ (health) │   │ (chain-  │
                       └────────┬─────────┘   └────┬─────┘   │ specific)│
                                │ mpsc              │ mpsc    └────┬─────┘
                                ▼                   ▼              │ mpsc
                       ┌─────────────────────────────────────┐     │
                       │           Recorder                  │◀────┘
                       │  (CSV writer + in-memory for stats) │
                       └─────────────────────────────────────┘
```

### Why channels, not shared state

The existing bench uses this pattern and it's correct. Each task owns its data. No `Arc<Mutex<_>>`, no `DashMap`, no lock contention. The submitter produces `PendingTx` values. The tracker consumes them, owns the pending pool internally, and produces `TxLifecycle` records. The recorder consumes records.

The one exception is the block stream, which multiple consumers need. Use `tokio::sync::broadcast` — each consumer gets its own copy of each block notification.

### Task breakdown

| Task | Runs for | Reads from | Writes to |
|------|----------|------------|-----------|
| Block stream | entire run | RPC (WS sub or poll) | broadcast channel |
| Submitter | varies by mode | mode config | mpsc → tracker |
| Tracker | entire run | mpsc from submitter + broadcast from block stream | mpsc → recorder |
| Block monitor | entire run | broadcast from block stream + RPC for block details | mpsc → recorder (block records) |
| Finality watcher | entire run | broadcast from block stream | mpsc → tracker (finality updates) |
| Recorder | entire run | mpsc from tracker + block monitor | CSV files |

---

## The Tracker — Core Engine

The existing receipt poller has two problems:

1. **Polls on a timer (50ms)** — wastes RPC calls when no new block has been produced, and misses the exact moment of inclusion by up to 50ms.
2. **Individual receipt requests** — one HTTP call per pending tx per poll cycle. At 200 pending txs, that's 200 calls every 50ms = 4000 calls/sec, way over the 500 RPC limit.

### Block-driven tracking

The tracker listens to the block stream. On each new block notification:

1. If there are pending txs awaiting inclusion: batch `eth_getTransactionReceipt` for all of them in a single JSON-RPC batch request. One HTTP call, N responses.
2. If there are pending txs not yet seen in mempool: batch `eth_getTransactionByHash` for them (separate batch, lower priority).
3. Update lifecycle timestamps for any newly-confirmed txs.
4. Check drop timeout for old pending txs.
5. Send completed `TxLifecycle` records downstream.

This is dramatically more efficient: we only check when something *could* have changed (new block), and we batch all checks into one HTTP call.

### Batch RPC

Construct a raw JSON-RPC batch array:

```json
[
  {"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0xabc..."]},
  {"jsonrpc":"2.0","id":2,"method":"eth_getTransactionReceipt","params":["0xdef..."]},
  ...
]
```

Send as a single HTTP POST. Parse the response array. One HTTP call, one rate-limit hit, N results.

alloy's `Provider` trait doesn't expose batch methods directly. We need to go through the raw transport or use `reqwest` to post the batch ourselves. This is a key piece of infrastructure — wrap it in a `BatchRpcClient` that:

- Accepts a list of (method, params) tuples
- Serializes the batch JSON
- Sends via HTTP POST
- Deserializes the response array
- Returns results keyed by request ID

```rust
struct BatchRpcClient {
    http: reqwest::Client,
    url: Url,
    rate_limiter: Governor,  // optional, for rate-limited providers
}

impl BatchRpcClient {
    async fn batch_receipts(&self, hashes: &[TxHash]) -> Vec<(TxHash, Option<Receipt>)>;
    async fn batch_get_tx(&self, hashes: &[TxHash]) -> Vec<(TxHash, Option<Transaction>)>;
}
```

### Timing accuracy

When we receive a receipt from a batch response, `t_included = Instant::now()` is when *we observed* it, not when the block was produced. The block's own timestamp is in the receipt. Record both:

- `t_included` (our clock): used for end-to-end latency (`t_included - t_submit`)
- `block.timestamp` (chain clock): recorded but not used for latency computation, since our clock and the chain's clock are different domains

For relative comparisons (p50 vs p99), the observation jitter is roughly constant across txs, so it cancels out. For absolute latency, the jitter is bounded by block time (how long between newHeads notifications).

On MegaETH with ~10ms blocks, this is tight. On Arbitrum with ~250ms blocks, we might miss by up to 250ms. If we need tighter resolution on slow-block chains, fall back to high-frequency polling during burst tests only.

---

## Test Modes

### Mode 1: Latency Sampling (24-48h)

**Submission pattern:** One `matchOrders` tx every N seconds (e.g., every 10s = 0.1 TPS). Low enough to not create self-congestion. The goal is baseline latency under varying organic load.

**What it measures:**
- Steady-state `t_included - t_submit` distribution
- How latency correlates with chain utilization (from block monitor)
- Time-of-day patterns (are there peak hours?)
- Whether latency is stable or has fat tails

**Implementation:** A simple loop with `tokio::time::interval`. No burst, no ramp. Just one tx, wait, one tx, wait. The tracker and block monitor run alongside.

**Duration:** 24h minimum. 48h better. The longer it runs, the more organic congestion variation we capture.

### Mode 2: Burst Test (the critical one)

**Submission pattern:** Pre-sign N txs, fire all within <1 second, track to completion.

**Steps:**

```
1. Fetch current nonce from chain
2. Build N matchOrders calldata (random wallet pairs, random amounts)
3. Sign all N txs with nonces [n, n+1, ..., n+N-1] and current gas price + buffer
4. t_burst_start = Instant::now()
5. Fire all N via concurrent eth_sendRawTransaction (FuturesUnordered, not sequential awaits)
6. Collect individual (t_submit, tx_hash) per tx as each future completes
7. t_all_submitted = Instant::now()
8. Register all with tracker
9. Wait until all N are confirmed or timed out (60s)
10. Compute BurstResult
```

**Why concurrent submission matters:** If we await each `eth_sendRawTransaction` sequentially, each call takes RTT (~10-50ms). For N=200, that's 2-10 seconds of submission time — the "burst" is spread over seconds, not sub-second. We need to fire them concurrently so they arrive at the chain's mempool roughly simultaneously.

```rust
let futs: FuturesUnordered<_> = signed_txs.iter().map(|raw| {
    let client = client.clone();
    async move {
        let t = Instant::now();
        let result = client.send_raw_transaction(raw).await;
        (t, result)
    }
}).collect();

let submissions: Vec<_> = futs.collect().await;
```

Alternatively, batch all N `eth_sendRawTransaction` calls in a single JSON-RPC batch. This gives truly simultaneous submission (one HTTP round-trip), but all txs share the same `t_submit`. For burst testing, same-t_submit is arguably more accurate — the chain sees them all at once.

**Run both ways** and compare. If they produce different results, the difference tells us something about RPC queuing behavior.

**Burst sizes:** N = 10, 50, 100, 200. Run each size multiple times (e.g., 10 rounds) at random intervals over several hours. This naturally captures varying chain load levels. Tag each burst result with the chain's utilization at burst time.

**Cooldown between bursts:** Wait for all pending txs to confirm, plus enough time for the chain to clear any backlog. We're testing burst response, not sustained overload.

**Single-wallet vs multi-wallet:**

- **Single wallet:** Sequential nonces from the operator. Tests the production constraint — nonce K blocks until K-1 confirms. If the chain processes them in-order, latency is dominated by block inclusion time. If out-of-order processing delays them, we see it here.

- **Multi-wallet:** One tx per wallet, no nonce dependencies. Tests raw chain throughput. *But:* in the current contract, only the operator role can call `matchOrders`. Options:
  1. Grant operator role to multiple wallets (easy, just add roles)
  2. Deploy a stub contract with no access control (better — isolates chain behavior from contract logic)
  3. Use simple ETH transfers (cheapest, but different gas profile)

  Option 2 is best for the settlement cost test anyway, so we'll need a stub contract regardless.

### Mode 3: Settlement Cost Analysis

**What we need:**

1. **Actual gas per settlement** — submit `matchOrders` txs at low rate, record gas_used. Should be near-constant since the contract logic is deterministic.

2. **Base fee distribution** — call `eth_feeHistory` periodically (every minute) for 24-72 hours. Build a histogram of base fees. The p99/p50 ratio tells us gas price predictability.

3. **Contention behavior** — submit 5-10 settlements simultaneously, all hitting the same Vault contract. Measure whether confirmation time is worse than serial. On chains with parallel execution (Monad), settlements touching the same storage slots might serialize at the EVM level even if blocks are parallel.

**No separate submission mode needed** — this piggybacks on the latency sampling mode (for steady-state gas) and burst mode (for contention). Just make sure we record gas_used and effective_gas_price on every tx, and run a separate `eth_feeHistory` polling task.

### Mode 4: Passive Monitoring (1 week)

**No transaction submission.** Just watch the chain.

**Block monitor (runs on all modes, standalone for passive):**

```rust
async fn block_monitor(block_rx: broadcast::Receiver<Block>, record_tx: mpsc::Sender<BlockRecord>) {
    let mut prev_block: Option<BlockRecord> = None;
    let mut known_hashes: HashMap<u64, B256> = HashMap::new(); // for reorg detection

    while let Ok(block) = block_rx.recv().await {
        let interval = prev_block.as_ref().map(|p| block.timestamp - p.timestamp);

        // Reorg detection
        if let Some(old_hash) = known_hashes.get(&block.number) {
            if *old_hash != block.hash {
                // REORG: block at this height changed
                // Walk back to find reorg depth
                log_reorg(block.number, *old_hash, block.hash);
            }
        }
        known_hashes.insert(block.number, block.hash);

        let record = BlockRecord {
            block_number: block.number,
            block_hash: block.hash,
            parent_hash: block.parent_hash,
            timestamp: block.timestamp,
            observed_at: Instant::now(),
            gas_used: block.gas_used,
            gas_limit: block.gas_limit,
            tx_count: block.transactions.len() as u32,
            base_fee: block.base_fee_per_gas,
            utilization: block.gas_used as f64 / block.gas_limit as f64,
            interval_ms: interval.map(|i| i * 1000),
        };

        prev_block = Some(record.clone());
        let _ = record_tx.send(record).await;
    }
}
```

**RPC health check (for multi-provider comparison):**

For each provider, periodically (every 10s):
1. `eth_blockNumber` — is the provider tracking the tip?
2. `eth_getBlockByNumber("latest")` — does it agree on the latest block hash?
3. Record response time, error/timeout

After the monitoring period, compare:
- Which provider sees blocks first
- Error rate per provider
- Tip lag between providers (is one always N blocks behind?)

---

## Chain Abstraction

### FinalityRule trait

```rust
enum FinalityStatus {
    Pending,
    SoftConfirmed,   // sequencer/validator accepted, could theoretically revert
    Finalized,       // irreversible under honest-majority assumption
}

trait FinalityRule: Send + Sync {
    /// Given a tx's inclusion block and the current chain state, what's the finality status?
    async fn check(
        &self,
        tx_block: u64,
        current_tip: u64,
        extra: &dyn FinalityContext,
    ) -> FinalityStatus;
}
```

### Implementations

**MegaETH:**
```rust
// Single sequencer, no reorgs in normal operation.
// t_included ≈ t_finalized.
fn check(&self, tx_block: u64, current_tip: u64, _: &dyn FinalityContext) -> FinalityStatus {
    if current_tip >= tx_block {
        FinalityStatus::Finalized
    } else {
        FinalityStatus::Pending
    }
}
```

**Monad:**
```rust
// BFT finality ~2 blocks after inclusion.
fn check(&self, tx_block: u64, current_tip: u64, _: &dyn FinalityContext) -> FinalityStatus {
    if current_tip >= tx_block + 2 {
        FinalityStatus::Finalized
    } else if current_tip >= tx_block {
        FinalityStatus::SoftConfirmed
    } else {
        FinalityStatus::Pending
    }
}
```

**Arbitrum:**
```rust
// Soft confirm on sequencer inclusion.
// Hard finality requires L1 batch posting + L1 finalization (~13 min).
// For v1: only track soft confirm. Log that hard finality is ~13 min.
// For v2: monitor Sequencer Inbox on L1 for batch postings.
fn check(&self, tx_block: u64, current_tip: u64, ctx: &dyn FinalityContext) -> FinalityStatus {
    if current_tip >= tx_block {
        FinalityStatus::SoftConfirmed  // v1: stop here
    } else {
        FinalityStatus::Pending
    }
}
```

### Chain config

```rust
struct ChainConfig {
    name: String,
    chain_id: u64,
    rpc_http: Vec<Url>,       // multiple providers
    rpc_ws: Option<Url>,      // for subscriptions (preferred)
    vault: Address,
    token: Address,
    finality: Box<dyn FinalityRule>,
    min_intrinsic_gas: u64,   // 21000 default, 60000 on MegaETH
    gas_price_buffer: f64,    // multiplier for pre-signed txs (e.g., 2.0)
    block_time_hint_ms: u64,  // expected block time, for fallback poll intervals
}
```

For v1, this is hardcoded per chain. Later, move to a TOML config file.

---

## RPC Layer

### The problem

At 500 calls/sec limit, we need to be efficient. A burst of 200 txs generates:
- 200 `eth_sendRawTransaction` calls (submission — can't batch these if we want per-tx t_submit)
- Periodic batch receipt polls (1 call per poll cycle, regardless of pending count)
- Block stream (1 WS subscription, or 1 poll per block_time)
- Mempool detection polls (1 batch call per cycle)

With batch RPC, the receipt polling is O(1) HTTP calls per cycle regardless of pending pool size. The submission is the bottleneck: 200 concurrent `sendRawTransaction` calls in a burst is 200 calls in ~100ms = 2000 calls/sec instantaneous. This might hit the rate limit.

### Solutions

1. **Batch submission for bursts:** Put all N `sendRawTransaction` in one JSON-RPC batch. 1 HTTP call, 1 rate-limit hit, all txs submitted. Trade-off: shared t_submit for all txs in the batch. Fine for burst testing where we want "all submitted simultaneously."

2. **Rate-limiter with burst allowance:** Use a token-bucket rate limiter (e.g., `governor` crate) set to 450/sec with burst capacity of 200. Normal operation stays under limit; bursts consume the bucket and then slow down. This is only needed if we do individual (non-batched) submission.

3. **Multiple RPC endpoints:** Rotate across providers. If each allows 500/sec, three providers give us 1500/sec effective. But responses need to be attributed to the right provider for health tracking.

### BatchRpcClient

```rust
struct BatchRpcClient {
    http: reqwest::Client,
    url: Url,
}

impl BatchRpcClient {
    /// Send a batch of eth_getTransactionReceipt calls, return results keyed by hash.
    async fn batch_receipts(&self, hashes: &[TxHash]) -> Result<Vec<(TxHash, Option<TransactionReceipt>)>>;

    /// Send a batch of eth_getTransactionByHash calls.
    async fn batch_get_txs(&self, hashes: &[TxHash]) -> Result<Vec<(TxHash, Option<Transaction>)>>;

    /// Send a batch of eth_sendRawTransaction calls (for burst submission).
    async fn batch_send_raw(&self, raw_txs: &[Bytes]) -> Result<Vec<Result<TxHash>>>;
}
```

This lives alongside alloy's `Provider` — use `Provider` for individual calls (block subscriptions, gas price, nonce), use `BatchRpcClient` for bulk operations.

---

## Block Stream

The block stream is the heartbeat of the system. Everything else reacts to new blocks.

### WebSocket (preferred)

```rust
async fn block_stream_ws(ws_url: &str) -> broadcast::Sender<BlockNotification> {
    let (tx, _) = broadcast::channel(256);
    let provider = ProviderBuilder::new().on_ws(ws_url).await?;
    let sub = provider.subscribe_blocks().await?;

    tokio::spawn(async move {
        let mut stream = sub.into_stream();
        while let Some(block) = stream.next().await {
            let _ = tx.send(BlockNotification {
                number: block.number,
                hash: block.hash,
                timestamp: block.timestamp,
                observed_at: Instant::now(),
            });
        }
    });

    tx
}
```

### HTTP polling (fallback)

```rust
async fn block_stream_poll(rpc_url: &str, poll_interval: Duration) -> broadcast::Sender<BlockNotification> {
    let (tx, _) = broadcast::channel(256);
    let provider = ProviderBuilder::new().on_http(rpc_url);
    let mut last_block = 0u64;

    tokio::spawn(async move {
        loop {
            if let Ok(tip) = provider.get_block_number().await {
                if tip > last_block {
                    for n in (last_block + 1)..=tip {
                        if let Ok(Some(block)) = provider.get_block_by_number(n.into()).await {
                            let _ = tx.send(BlockNotification { ... });
                        }
                    }
                    last_block = tip;
                }
            }
            tokio::time::sleep(poll_interval).await;
        }
    });

    tx
}
```

Use WS when available. Fall back to polling with interval = `chain.block_time_hint / 2` (e.g., 5ms for MegaETH, 125ms for Arbitrum).

---

## Gas Strategy for Pre-Signed Transactions

For burst mode, all N txs are pre-signed before submission. The gas price is locked at signing time. If the base fee moves between signing and inclusion, txs with too-low gas will be stuck or dropped.

**EIP-1559 chains:** Set `maxFeePerGas` to `current_base_fee * gas_price_buffer` (e.g., 2x). Set `maxPriorityFeePerGas` to a reasonable tip (fetch from `eth_maxPriorityFeePerGas`). The protocol refunds the difference, so overpaying maxFeePerGas doesn't cost more — it just ensures inclusion.

**Legacy gas (MegaETH):** Base fee is fixed. Set `gasPrice = current + small buffer`. No risk of base fee movement.

**What if gas changes during a long latency run?** The submitter re-fetches gas price before each tx. No pre-signing for the latency mode.

---

## Nonce Edge Cases

### Burst mode: what if submission K fails?

If `eth_sendRawTransaction` returns an error for nonce K, all subsequent nonces (K+1, K+2, ...) are blocked — the chain won't include them until K is filled.

**Don't retry during the burst.** Record the failure and move on. A failed burst is data — it tells us the RPC rejected a tx under load. If we retry, we're measuring our retry logic, not the chain.

**After the burst:** Wait for the timeout period. Any txs that never confirm are marked as dropped. Then re-fetch the nonce from the chain for the next burst.

### Long-running latency mode: stuck nonce recovery

If a tx is pending for >2 minutes, the submitter should:
1. Log the stuck nonce
2. Submit a replacement tx (same nonce, 2x gas price, zero-value transfer to self)
3. Wait for either the original or replacement to confirm
4. Resume normal nonce sequence

This prevents a single stuck tx from killing a 48h run.

---

## Output

### CSV schemas

**tx_records.csv:**
```
tx_hash, nonce, wallet, t_submit_ms, t_mempool_ms, t_included_ms, t_finalized_ms,
block_number, block_timestamp, gas_used, effective_gas_price, status,
latency_included_ms, latency_finalized_ms, phase, burst_id
```

**block_records.csv:**
```
block_number, block_hash, parent_hash, timestamp, observed_at_ms,
gas_used, gas_limit, utilization, tx_count, base_fee, interval_ms
```

**burst_results.csv:**
```
burst_id, n, mode, t_start_ms, submission_spread_ms, completion_time_ms,
max_latency_ms, confirmed, reverted, dropped, chain_utilization, chain_tps
```

**reorg_events.csv:**
```
detected_at_ms, block_number, old_hash, new_hash, depth
```

### Statistics (computed at end, printed to stdout + summary.json)

**Latency distribution:**
- p50, p95, p99 of `latency_included_ms` (overall and per-phase)
- p50, p95, p99 of `latency_finalized_ms`
- Breakdown by chain utilization bucket (0-25%, 25-50%, 50-75%, 75-100%)

**Burst analysis:**
- For each burst size: median completion time, max completion time, drop rate
- Completion time vs N curve (does it scale linearly or blow up?)
- Single-wallet vs multi-wallet comparison

**Cost:**
- Gas used: mean, p50, p99
- Base fee distribution: p50, p95, p99, p99/p50 ratio
- Cost per settlement in ETH and USD (if ETH price provided)

**Reliability:**
- Block time: mean, p50, p99, stall count (gaps > 5x mean)
- Reorg count and max depth
- Chain-wide utilization distribution
- RPC error rate per provider

**Scorecard (the final output):**
```
┌────────────────────────────┬─────────┬────────────┬────────────┐
│ Metric                     │ Idle    │ Light load │ Heavy load │
├────────────────────────────┼─────────┼────────────┼────────────┤
│ t_included p50             │         │            │            │
│ t_included p99             │         │            │            │
│ t_finalized p50            │         │            │            │
│ Burst-100 max latency      │         │            │            │
│ Burst-100 drop rate        │         │            │            │
│ Settlement gas cost (USD)  │         │            │            │
│ Gas price p99/p50 ratio    │         │            │            │
│ Reorg events / 24h         │         │            │            │
│ Block time p99             │         │            │            │
│ RPC error rate             │         │            │            │
└────────────────────────────┴─────────┴────────────┴────────────┘
```

"Idle" / "Light" / "Heavy" are determined by chain utilization at the time of measurement, not by our own load. We bucket results by the chain's utilization when they were collected.

---

## Module Structure

```
bench/src/
├── main.rs                // CLI entry, subcommand dispatch
├── config.rs              // ChainConfig, test configs, CLI args
├── contracts.rs           // ABI definitions (Vault, MockToken, stub)
├── wallet.rs              // HD mnemonic derivation
│
├── rpc/
│   ├── mod.rs             // RpcClient (wraps alloy Provider + BatchRpcClient)
│   ├── batch.rs           // JSON-RPC batch requests over raw HTTP
│   └── health.rs          // Per-provider health tracking
│
├── stream/
│   ├── mod.rs             // block_stream() — returns broadcast::Sender
│   └── reorg.rs           // Reorg detection from block stream
│
├── tx/
│   ├── mod.rs             // TxLifecycle, TxStatus, PendingTx
│   ├── builder.rs         // Build + sign matchOrders/stub txs
│   ├── tracker.rs         // Block-driven receipt tracking (the core engine)
│   └── nonce.rs           // NonceManager (sequential, with stuck recovery)
│
├── chain/
│   ├── mod.rs             // FinalityRule trait, ChainConfig
│   ├── megaeth.rs
│   ├── arbitrum.rs
│   └── monad.rs
│
├── modes/
│   ├── fund.rs            // Existing fund mode (wallets + tokens + approvals)
│   ├── latency.rs         // 24-48h steady-rate sampling
│   ├── burst.rs           // Burst test orchestration
│   ├── monitor.rs         // Passive chain monitoring (no txs)
│   └── eval.rs            // Full evaluation (runs latency + burst + monitor)
│
├── output/
│   ├── csv.rs             // CSV writer tasks
│   └── stats.rs           // Percentile computation, scorecard generation
│
└── gas.rs                 // Gas price fetching + buffering strategy
```

---

## Implementation Order

### Phase 1 — Core infrastructure that everything else needs

1. **`rpc/batch.rs`** — BatchRpcClient. Build and test this first because the tracker depends on it. Test: send a batch of 10 `eth_getTransactionReceipt` calls, verify we get 10 responses in one HTTP round-trip.

2. **`stream/mod.rs`** — Block stream (WS + polling fallback). Test: subscribe, verify we get block notifications.

3. **`tx/tracker.rs`** — Block-driven tracker. Receives pending txs via channel, checks receipts on each new block via batch RPC, sends completed records downstream. Test: submit 5 txs, verify all get tracked to completion.

4. **`output/csv.rs`** — CSV writer for TxLifecycle records. Straightforward port from existing code with more columns.

### Phase 2 — Burst mode (the critical test)

5. **`tx/builder.rs`** — Pre-sign N txs with sequential nonces. Test: build 10 txs, verify nonces are correct, calldata encodes properly.

6. **`modes/burst.rs`** — Burst orchestrator. Pre-sign, concurrent-submit, wait for completion, compute BurstResult. Test: burst of 10 on MegaETH, verify all confirm.

7. **`output/stats.rs`** — Percentile computation. Test: feed known latency values, verify p50/p95/p99 are correct.

### Phase 3 — Long-running modes

8. **`modes/latency.rs`** — Steady-rate submitter. Simple interval loop. Shares tracker and recorder with burst mode.

9. **`stream/reorg.rs`** — Reorg detection from block stream.

10. **`modes/monitor.rs`** — Passive chain monitoring. Block monitor + reorg detection + RPC health, no tx submission.

### Phase 4 — Polish and multi-chain

11. **`chain/`** — Finality rules per chain. Wire into tracker.

12. **`modes/eval.rs`** — Full evaluation mode: run latency sampling for N hours, then burst tests, report scorecard.

13. **`rpc/health.rs`** — Multi-provider comparison.

### What to skip for now

- **t_mempool tracking:** Most chains return null from `eth_getTransactionByHash` until the tx is included. Not worth the RPC calls until we find a chain where mempool detection is useful.
- **Arbitrum L1 finality:** Requires monitoring a separate L1 chain. Complex. For v1, just log "soft confirm only, hard finality ~13 min."
- **Stub settlement contract:** Can use the real Vault + matchOrders for now. Same gas profile. Deploy a stub later if we need to test without access-control constraints.
- **TOML config:** CLI args + .env is fine for now. Config file when we have >2 chains.

---

## Risks and Open Questions

1. **RPC rate limits during burst submission:** 200 concurrent `sendRawTransaction` calls might exceed 500/sec. Mitigation: use batch RPC for submission (one HTTP call for all N). Downside: lose per-tx submission timestamps. Test both approaches and compare.

2. **Clock accuracy:** `Instant::now()` is monotonic but its resolution varies by platform. On macOS, it's ~1μs. On Linux, ~1ns. Either is fine for our ms-resolution measurements.

3. **RPC response latency vs chain latency:** When we measure `t_included - t_submit`, we're measuring chain processing time PLUS RPC round-trip. If the RPC is slow, our latency measurement is inflated. Mitigation: test with multiple RPC providers; use the fastest one as baseline; the difference between fastest and slowest is RPC overhead.

4. **Self-congestion during burst:** A burst of 200 txs from us might congest the chain, especially low-throughput chains. This is actually what we want to measure — if our own burst congests the chain, that's a problem for the product. But we should note whether the burst itself caused the congestion or whether it coincided with organic congestion (check chain utilization before/during/after burst).

5. **Wallet funding for multi-chain:** Each chain needs funded wallets with tokens and approvals. The existing `fund` mode handles this but is hardcoded to MegaETH. Needs to be parameterized per chain.
