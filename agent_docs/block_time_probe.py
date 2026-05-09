#!/usr/bin/env python3
"""
Independent block-time measurement for MegaETH.
Polls eth_blockNumber as fast as possible and compares wall-clock deltas
against on-chain block timestamp deltas.

This bypasses our Rust framework entirely. If blocks appear every ~1s here too,
it's the chain/RPC, not our tool.

Deps: requests (pip install requests)
Usage: python3 block_time_probe.py          # 60s default
       python3 block_time_probe.py 300      # 5 minutes
"""
import json
import os
import sys
import time

RPC_URL = None

def load_env():
    global RPC_URL
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
    RPC_URL = os.environ.get("MEGAETH_RPC")
    if not RPC_URL:
        print("MEGAETH_RPC not set")
        sys.exit(1)

import urllib.request

def rpc_call(method, params=None):
    payload = json.dumps({
        "jsonrpc": "2.0", "id": 1,
        "method": method, "params": params or []
    }).encode()
    req = urllib.request.Request(
        RPC_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())

def get_block_number():
    r = rpc_call("eth_blockNumber")
    return int(r["result"], 16)

def get_block(number):
    r = rpc_call("eth_getBlockByNumber", [hex(number), False])
    return r.get("result")

def main():
    load_env()
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 60

    print(f"Block Time Probe — MegaETH")
    print(f"RPC: {RPC_URL[:60]}...")
    print(f"Duration: {duration}s")
    print()

    # Measure RPC latency first
    latencies = []
    for _ in range(5):
        t = time.time()
        get_block_number()
        latencies.append((time.time() - t) * 1000)
    avg_lat = sum(latencies) / len(latencies)
    print(f"RPC latency (eth_blockNumber): avg {avg_lat:.0f}ms  samples: {[f'{l:.0f}' for l in latencies]}")
    print()

    last_block = get_block_number()
    last_wall = time.time()
    last_chain_ts = None

    blk = get_block(last_block)
    if blk:
        last_chain_ts = int(blk.get("timestamp", "0x0"), 16)
    start_block = last_block

    print(f"Starting at block {last_block}")
    print(f"{'Elapsed':>8}  {'Block':>10}  {'Jump':>5}  {'WallDt':>8}  {'ChainTs':>12}  {'TsDelta':>8}  {'Txs':>5}  {'RpcMs':>6}")
    print("-" * 85)

    start = time.time()
    polls = 0
    block_events = []

    while time.time() - start < duration:
        t0 = time.time()
        try:
            bn = get_block_number()
        except Exception as e:
            print(f"  poll error: {e}")
            time.sleep(0.5)
            continue
        rpc_ms = (time.time() - t0) * 1000
        polls += 1

        if bn > last_block:
            wall_dt = time.time() - last_wall
            jump = bn - last_block

            # Fetch block details for the latest block
            try:
                blk = get_block(bn)
            except Exception:
                blk = None

            chain_ts = None
            ts_delta = ""
            tx_count = "?"
            if blk:
                chain_ts = int(blk.get("timestamp", "0x0"), 16)
                tx_count = len(blk.get("transactions", []))
                if last_chain_ts is not None:
                    ts_delta = f"{chain_ts - last_chain_ts}s"

            elapsed = time.time() - start
            print(
                f"{elapsed:7.1f}s  {bn:>10}  {jump:>+5}  {wall_dt*1000:7.0f}ms  "
                f"{chain_ts or '':>12}  {ts_delta:>8}  {tx_count:>5}  {rpc_ms:5.0f}ms"
            )

            block_events.append({
                "block": bn,
                "jump": jump,
                "wall_dt_ms": wall_dt * 1000,
                "chain_ts": chain_ts,
                "tx_count": tx_count if isinstance(tx_count, int) else 0,
            })

            last_block = bn
            last_wall = time.time()
            last_chain_ts = chain_ts

        time.sleep(0.01)  # 10ms poll interval — faster than our Rust tool's 50ms

    # Summary
    print()
    print(f"=== Summary ({len(block_events)} block transitions in {duration}s) ===")
    total_blocks = last_block - start_block
    print(f"Blocks observed: {start_block} → {last_block} ({total_blocks} blocks)")
    print(f"Polls made: {polls}")

    if block_events:
        wall_dts = [e["wall_dt_ms"] for e in block_events]
        wall_dts.sort()
        n = len(wall_dts)
        print(f"Wall-clock block interval:")
        print(f"  min: {wall_dts[0]:.0f}ms  p50: {wall_dts[n//2]:.0f}ms  "
              f"p95: {wall_dts[int(n*0.95)]:.0f}ms  max: {wall_dts[-1]:.0f}ms")

        jumps = [e["jump"] for e in block_events]
        single = sum(1 for j in jumps if j == 1)
        multi = sum(1 for j in jumps if j > 1)
        max_jump = max(jumps)
        print(f"Block jumps: {single} single (+1), {multi} multi-jumps, max jump: +{max_jump}")

        if any(e["chain_ts"] for e in block_events):
            ts_deltas = []
            for i in range(1, len(block_events)):
                if block_events[i]["chain_ts"] and block_events[i-1]["chain_ts"]:
                    ts_deltas.append(block_events[i]["chain_ts"] - block_events[i-1]["chain_ts"])
            if ts_deltas:
                ts_deltas.sort()
                m = len(ts_deltas)
                print(f"On-chain timestamp delta (seconds):")
                print(f"  min: {ts_deltas[0]}  p50: {ts_deltas[m//2]}  max: {ts_deltas[-1]}")

        tx_counts = [e["tx_count"] for e in block_events if isinstance(e["tx_count"], int)]
        if tx_counts:
            tx_counts.sort()
            tc = len(tx_counts)
            print(f"Txs per block: min: {tx_counts[0]}  p50: {tx_counts[tc//2]}  max: {tx_counts[-1]}")

    print()
    print("INTERPRETATION:")
    if block_events:
        median_dt = wall_dts[n // 2]
        jumps_gt1 = sum(1 for j in jumps if j > 1)
        if jumps_gt1 > len(jumps) * 0.3:
            print("  Many multi-block jumps → chain produces blocks faster than we can poll")
            print("  The RPC likely serves real micro-blocks, we just can't keep up at 10ms")
        elif median_dt > 500:
            print(f"  Median block interval ~{median_dt:.0f}ms with +1 increments")
            print("  Either the RPC aggregates micro-blocks or the chain is actually this slow")
            print("  Check on-chain timestamps for ground truth")
        else:
            print(f"  Blocks arriving every ~{median_dt:.0f}ms — chain is fast")

if __name__ == "__main__":
    main()
