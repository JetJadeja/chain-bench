# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Random scripts and contracts for Tiny. Currently contains a Foundry project at `contracts/` with a prediction-market-style Vault that holds ERC20 deposits and lets an operator match orders between users.

## Build & Test

All commands run from `contracts/`:

```shell
forge build          # compile
forge test           # run all tests
forge test --mt test_withdraw  # run a single test by name
forge fmt            # format Solidity
forge snapshot       # gas snapshots
```

Deploy (requires `OPERATOR_KEY` env var):
```shell
forge script script/Deploy.s.sol:Deploy --rpc-url <url> --private-key <key> --broadcast
```

## Architecture

- **Vault** (`src/Vault.sol`): Core contract. Holds an ERC20 token, exposes `matchOrders` (operator-only, pulls tokens from two users via `transferFrom` and credits cross-balances) and `withdraw` (users claim credited balances). Uses Solady's `OwnableRoles` for access control with a single `OPERATOR_ROLE`.
- **MockToken** (`src/MockToken.sol`): Mintable/burnable ERC20 for testing. Uses Solady's `ERC20` and `Ownable`.
- **Deploy** (`script/Deploy.s.sol`): Deploys MockToken + Vault and grants operator role.

## Dependencies

Solady (via Foundry libs) for ERC20, OwnableRoles, Ownable, and SafeTransferLib. Solidity `^0.8.28`.

## Deployed Contracts

### MegaETH Mainnet (Chain ID 4326)

| Contract | Address |
|----------|---------|
| MockToken | `0x19F3a47011C396Af574c821e97112d20d75572E3` |
| Vault | `0xDFb282822456D50553AE0Dc1649D152EC11871a8` |

- RPC: in `.env` (`MEGAETH_RPC`)
- Deployer/owner/operator: `0xeCe264b9A9c0395e10ee99ffB0652358ab55C8D0`
- Gas price: ~0.001 gwei (1,000,000 wei). EIP-1559 base fee adjustment is disabled.
- Min intrinsic gas: 60,000 (not the standard 21,000). Contract creation gas is ~10-20x higher than standard EVM.
- `forge create` is broken in Foundry 1.2.3 — use `cast send --create` or `forge script` with `--gas-estimate-multiplier 1000` for deploys.

## Chain Benchmark Tool (`bench/`, Rust)

Rust CLI that stress-tests candidate EVM chains for the one-minute prediction market. Answers: can this chain handle our matching engine at the throughput we need, and what does it cost?

Build and run from `bench/`:
```shell
cargo build
cargo run -- --num-accounts 50 fund
cargo run -- --num-accounts 50 simulate --rate 10 --duration 120 --output results.csv
```

All config (RPC, addresses, keys, mnemonic) auto-loads from `../.env`. Override any value with CLI flags. Gas price is fetched from the RPC if `--gas-price` is not provided.

### Fund Mode

One-time setup per chain. For each of N counterparty wallets (derived from mnemonic, starting at index 1 — index 0 is the deployer/operator):

1. **Send ETH** — deployer sends a small amount for gas. Only enough to cover one approval tx. Wallets never submit anything else; only the operator calls matchOrders.
2. **Mint tokens** — deployer calls `MockToken.mint(wallet, amount)` as owner. Large amount (default 1M tokens) so we can run many simulation rounds before depletion.
3. **Approve Vault** — each wallet signs its own `token.approve(vault, uint256.max)` so the Vault can pull tokens via transferFrom.

Every step checks on-chain state first and skips if already done:
- `eth_getBalance(wallet)` >= threshold → skip ETH send
- `token.balanceOf(wallet)` >= target → skip mint
- `token.allowance(wallet, vault)` >= target → skip approval

Safe to re-run at any time. If it crashes halfway, just run again and it picks up where it left off. After a simulate run depletes token balances, re-running fund will mint more tokens without re-sending ETH or re-approving.

Steps 1-2 are signed by the deployer with a local nonce counter (fire all txs, then batch-wait for receipts). Step 3 is signed by each wallet's own key.

### Simulate Mode

Three concurrent tokio tasks connected by mpsc channels:

```
[Submit Loop] --PendingTx--> [Receipt Poller] --TxRecord--> [CSV Writer]
```

**Submit loop** (single task, operator key): Tight loop picking two random funded wallets, random token amounts, ABI-encoding matchOrders, signing with the next nonce, and firing at the RPC via `eth_sendRawTransaction`. Does not wait for confirmation before sending the next one. Nonce managed by an AtomicU64 initialized once from the RPC — no per-tx nonce lookup.

**Receipt poller** (single task): Every 50ms, checks the RPC for each outstanding tx hash. Records confirmation timestamp, block number, gas used, effective gas price, success/revert. Txs pending > 60 seconds are marked as dropped. Must use JSON-RPC batch requests to stay within RPC rate limits (see below).

**CSV writer** (single task): Writes each completed TxRecord to the output CSV as it arrives.

### Load Shape (5 phases)

We ramp through five phases because the interesting data is where the chain starts to bend, not where it's completely overwhelmed.

1. **Warmup** — 10% of target rate for a few seconds. Sanity check: nonces incrementing, receipts coming back, gas price right. If warmup txs revert, something is misconfigured.
2. **Ramp** — Every `--ramp-step` seconds, multiply rate by `--ramp-multiplier` (default 1.5x) until hitting `--rate`. This is where we find the inflection point. If p95 latency jumps between rate steps 3 and 4, the chain saturates around step 3's rate.
3. **Hold** — Sustain target rate for the bulk of `--duration`. This is the steady-state measurement. Any TPS number we report should come from this phase. We want to know: at X offered TPS sustained over minutes, what does the chain actually include?
4. **Spike** — Hit `--spike-multiplier` × target rate (default 2x) for `--spike-duration` seconds. Simulates market close — everyone rushing to place bets in the last few seconds. Does latency spike? Do txs start dropping? Does gas price move? How long does the backlog take to clear?
5. **Recovery** — Drop to 10% rate for `--recovery` seconds. How long before latency normalizes? Are there stuck txs from the spike? Tells us whether a 60-second market can recover in time for the next one.

### Per-Transaction Record (CSV columns)

| Field | Description |
|-------|-------------|
| nonce | Operator nonce used |
| submit_timestamp_ms | When we called sendRawTransaction |
| tx_hash | Transaction hash |
| confirm_timestamp_ms | When we got the receipt |
| block_number | Block the tx was included in |
| gas_used | Actual gas consumed |
| effective_gas_price | What we paid per gas unit (wei) |
| status | true = success, false = revert, null = dropped |
| latency_ms | confirm - submit |
| phase | Which load phase (warmup/ramp/hold/spike/recovery) |

### Statistics Computed at End

**Throughput** (how fast):
- **Offered TPS** — txs submitted per second. The load we applied.
- **Included TPS** — txs confirmed on-chain per second. The chain's actual throughput.
- **The gap between these two is the most important number.** If offered is 50 and included is 50, the chain keeps up. If offered is 50 and included is 30, 40% of bets are stuck in limbo.

**Latency** (how fast does a user know their bet landed):
- **p50 confirmation latency** — typical user experience.
- **p95 confirmation latency** — unlucky user experience.
- **p99 confirmation latency** — worst case outside of total failure.
- Should be broken down by phase. p50 during hold vs p50 during spike tells you whether the one-minute market's final rush will be a bad experience.

**Cost** (how much to operate):
- **Avg gas per matchOrders** — should be roughly constant since the contract logic is deterministic.
- **Avg effective gas price** — on MegaETH with base fee adjustment disabled, should stay flat. On other chains, expect it to climb during spike. The shape of this curve is the whole point of the gas scaling question.
- **Avg cost per match in ETH** — gas used × gas price. Multiply by ETH price for per-trade operating cost.

**Reliability** (can we trust it):
- **Revert rate** — % of included txs that reverted. Should be near zero in a well-configured test.
- **Drop rate** — % of submitted txs that never got a receipt within 60 seconds. Drops kill a one-minute market — user doesn't know if they're in or out.

### RPC Rate Limits and Batching

Our RPC provider limits us to 500 calls/sec. At 100 TPS the submit loop alone is 100 calls/sec, and individual receipt polling would blow through 500 quickly.

**JSON-RPC batching** solves this: send an array of requests in one HTTP POST, get back an array of responses. One HTTP call carrying 50 receipt requests counts as 1 call against the rate limit.

Budget at 100 TPS with batched receipts:
- Submits: 100 calls/sec (one `eth_sendRawTransaction` each)
- Receipt polls: batch all pending into one call every ~100ms = ~10 calls/sec
- Total: ~110 calls/sec, well under 500

Without batching, receipt polling alone would hit 500 at ~30 TPS. With batching, we can comfortably do 400+ TPS before submits become the bottleneck.

### Recommended Test Sequence

Same contract, same tx shape, same load profile across all candidate chains. Only variable is the chain.

1. **Smoke test**: 10 accounts, 5 TPS, 60 seconds. Confirm everything works, CSV is sane, no reverts.
2. **Moderate load**: 50 accounts, 20 TPS, 120 seconds. See where the chain sits under normal operation.
3. **Push it**: 100 accounts, 50-100 TPS, 180 seconds. Find the bend point — where latency climbs, drops appear, gas moves.

### CLI Flags

Shared (defaults from .env):
```
--rpc <url>                    env: MEGAETH_RPC
--chain-id <id>                default: 4326
--vault <address>              env: MEGAETH_VAULT
--token <address>              env: MEGAETH_MOCK_TOKEN
--deployer-key <hex>           env: DEPLOYER_PRIVATE_KEY
--mnemonic <phrase>            env: MNEMONIC
--num-accounts <N>             required
--gas-price <wei>              optional, fetched from RPC if omitted
```

Fund-specific:
```
--token-amount <u256>          default: 1000000000000000000000000 (1M tokens)
--eth-amount <float>           default: 0.0001
```

Simulate-specific:
```
--rate <tps>                   default: 10.0
--duration <secs>              default: 120
--warmup <secs>                default: 10
--ramp-step <secs>             default: 10
--ramp-multiplier <float>      default: 1.5
--spike-multiplier <float>     default: 2.0
--spike-duration <secs>        default: 10
--recovery <secs>              default: 10
--output <path>                default: results.csv
--match-amount-min <u256>      default: 1e18
--match-amount-max <u256>      default: 10e18
```

### Design Notes

- Counterparty wallets derived from HD mnemonic starting at index 1 (index 0 = deployer/operator). Zero on-chain cost to "create" accounts, portable across chains.
- Fund mode mints tokens directly (MockToken owner privilege) rather than transferring, saving gas.
- Single operator EOA means strictly ordered nonces — this is the actual production constraint and is what we're benchmarking.
- matchOrders pulls tokens from wallets into the Vault's internal balances. They don't come back without calling withdraw. Over time wallets deplete. Re-running fund will top up tokens.
- Gas limits are hardcoded per tx type (60k transfer, 200k mint/approve, 500k matchOrders) and set explicitly on every TransactionRequest to avoid eth_estimateGas RPC calls.

## Chain Evaluation Spec

The bench tool above is the existing throughput benchmark. Below is the full evaluation spec for deciding which chain(s) to run the prediction market on. This measures the specific failure mode that matters: can this chain reliably settle a burst of fills in the last second before market resolution?

### 1. Latency Distribution (headline metric)

Submit identical transactions over 24-48 hours and capture four timestamps per tx:

| Timestamp | Source | What it measures |
|-----------|--------|------------------|
| `t_submit` | local clock when `eth_sendRawTransaction` returns | When we handed it off |
| `t_mempool` | first non-null `eth_getTransactionByHash` response | When the chain acknowledged it (some chains return null until inclusion — note this) |
| `t_included` | first non-null `eth_getTransactionReceipt` | Soft confirmation |
| `t_finalized` | block crosses chain's finality threshold | Hard finality |

Compute p50/p95/p99 of `t_included - t_submit` and `t_finalized - t_submit`. No averages — tail latency is what kills you during the close burst.

**Finality definitions:**

- **MegaETH**: Single-sequencer preconfirmation. `t_included` ≈ `t_finalized` for practical purposes (no reorgs unless sequencer fails).
- **Arbitrum**: `t_included` is sequencer soft-confirm. For hard finality, watch for the L1 batch tx via the Sequencer Inbox contract on Ethereum and wait for that L1 block to finalize (~13 min after batch posts).
- **Monad**: BFT finality fires ~2 blocks after inclusion (~800ms). Subscribe to `finalizedBlockNumber` if exposed, or use `block.number - 2` as proxy.

### 2. Burst Behavior (the actual test)

Simulate the close-second burst. Submit N transactions within a 1-second window and measure:

- Time from last submit to last confirmation
- Max single-tx latency in the burst
- Any txs that failed, got dropped, or stuck in mempool

Run with N = 10, 50, 100, 200. Run at idle and during organic congestion.

The key metric is whether tail latency stays bounded as N grows or starts blowing up.

**Two modes:**
- **Single wallet** (sequential nonces): tests ordering under contention — matches our production constraint of a single operator EOA.
- **Multiple wallets** (parallel): tests pure chain throughput without nonce serialization.

### 3. Settlement-Specific Cost and Behavior

Deploy a stub contract that mimics real settlement — signature verification, a few storage slot modifications, event emissions. Doesn't need to be the real contract, just realistic in gas profile (~500K-1M gas per call).

For each chain, capture:

- **Gas used** for a 1-maker + 15-taker bundle
- **Cost in USD** at current gas price
- **Cost variance** — sample `eth_feeHistory` over 24-72 hours, get base fee distribution
- **Contention behavior** — submit 5-10 stub settlements simultaneously, all touching the same contract. On Monad, do they parallelize or serialize? Compare confirmation time vs. serial submission.

### 4. Reliability and External Congestion (passive monitoring, ~1 week)

- **Block time variance**: poll `eth_getBlockByNumber` for new blocks, compute interval distribution. Look for outliers and stalls.
- **Reorg frequency and depth**: store last N block hashes, check if they ever change. Use `safeBlock` and `finalizedBlock` where exposed.
- **Gas utilization**: `gasUsed / gasLimit` per block over time — how full is the chain.
- **Chain-wide TPS**: count txs per block. Correlate with own latency measurements to spot when external congestion hurts us.
- **RPC reliability**: same calls via 3+ providers (Alchemy, QuickNode, dRPC, public RPC). Measure error rate, timeout rate, response latency, consistency between providers.

### RPC Calls Used

```
eth_sendRawTransaction        // submit
eth_getTransactionByHash      // mempool detection
eth_getTransactionReceipt     // inclusion detection
eth_blockNumber               // current tip
eth_getBlockByNumber          // block details (gasUsed, gasLimit, txs, timestamp)
eth_feeHistory                // historical fee data
eth_gasPrice                  // current gas
eth_maxPriorityFeePerGas      // EIP-1559 priority
```

WebSocket subscriptions (preferred over polling):
```
newHeads                      // new block notifications
newPendingTransactions        // mempool firehose
```

### Output: Per-Chain Scorecard

| Metric | Idle | Light load | Heavy load |
|--------|------|------------|------------|
| `t_included` p50 | | | |
| `t_included` p99 | | | |
| `t_finalized` p50 | | | |
| Burst-100 tail latency | | | |
| Settlement gas cost (USD) | | | |
| Gas price p99 / p50 ratio | | | |
| Reorg events / 24h | | | |
| RPC error rate | | | |

**The two numbers that drive the decision:** `t_finalized` p99 under load and burst-100 tail latency. Everything else is supporting context.
