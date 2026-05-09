#!/usr/bin/env python3
"""
Raw submit throughput: how fast can we push txs to the RPC using parallel connections?

Pre-signs all txs, then fires them from multiple threads simultaneously.
Measures pure HTTP throughput independent of signing speed.

Deps: pip install eth-account requests
Usage: python3 submit_throughput_probe.py                  # 200 txs, 4 workers
       python3 submit_throughput_probe.py --count 500 --workers 8 --batch 50
"""
import argparse
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from eth_account import Account

RPC_URL = None
CHAIN_ID = 4326

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

def rpc_single(method, params):
    resp = requests.post(RPC_URL, json={
        "jsonrpc": "2.0", "id": 1,
        "method": method, "params": params,
    }, timeout=10)
    return resp.json()

def send_batch(session, raw_txs, batch_id):
    """Send a batch of raw txs, return (ok, fail, elapsed_ms, errors)"""
    batch_req = [
        {"jsonrpc": "2.0", "id": i, "method": "eth_sendRawTransaction",
         "params": [raw]}
        for i, raw in enumerate(raw_txs)
    ]
    t0 = time.time()
    try:
        resp = session.post(RPC_URL, json=batch_req, timeout=30)
    except Exception as e:
        return 0, len(raw_txs), (time.time() - t0) * 1000, {str(e): len(raw_txs)}

    dt = (time.time() - t0) * 1000

    if resp.status_code != 200:
        return 0, len(raw_txs), dt, {f"HTTP {resp.status_code}": len(raw_txs)}

    ok = 0
    fail = 0
    errors = {}
    hashes = []
    for r in resp.json():
        if r.get("error"):
            fail += 1
            msg = r["error"].get("message", str(r["error"]))[:80]
            errors[msg] = errors.get(msg, 0) + 1
        else:
            ok += 1
            hashes.append(r.get("result"))

    return ok, fail, dt, errors, hashes

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch", type=int, default=50)
    args = parser.parse_args()

    load_env()
    key = os.environ.get("DEPLOYER_PRIVATE_KEY")
    if not key or not RPC_URL:
        print("Set DEPLOYER_PRIVATE_KEY and MEGAETH_RPC in .env")
        sys.exit(1)

    acct = Account.from_key(key)
    print(f"Account: {acct.address}")
    print(f"Config: {args.count} txs, {args.workers} workers, batch size {args.batch}")
    print()

    nonce = int(rpc_single("eth_getTransactionCount", [acct.address, "pending"])["result"], 16)
    gas_price = int(rpc_single("eth_gasPrice", [])["result"], 16)
    print(f"Starting nonce: {nonce}, gas price: {gas_price}")

    # Pre-sign
    N = args.count
    print(f"Pre-signing {N} txs...")
    t0 = time.time()
    signed = []
    for i in range(N):
        tx = {
            "nonce": nonce + i,
            "to": acct.address,
            "value": 0,
            "gas": 60_000,
            "gasPrice": gas_price,
            "chainId": CHAIN_ID,
            "type": 0,
        }
        stx = acct.sign_transaction(tx)
        signed.append("0x" + stx.raw_transaction.hex())
    print(f"  Done in {(time.time() - t0)*1000:.0f}ms")
    print()

    # Split into batches
    batches = [signed[i:i + args.batch] for i in range(0, N, args.batch)]
    print(f"Split into {len(batches)} batches of ≤{args.batch}")

    # --- Test 1: Sequential (baseline) ---
    print(f"\n{'='*60}")
    print(f"Test 1: SEQUENTIAL — one batch at a time, single connection")
    print(f"{'='*60}")
    session = requests.Session()
    total_ok = 0
    total_fail = 0
    all_errors = {}
    all_hashes = []

    t_start = time.time()
    for i, batch in enumerate(batches):
        result = send_batch(session, batch, i)
        ok, fail, dt, errors = result[:4]
        hashes = result[4] if len(result) > 4 else []
        total_ok += ok
        total_fail += fail
        all_hashes.extend(hashes)
        for msg, cnt in errors.items():
            all_errors[msg] = all_errors.get(msg, 0) + cnt
        print(f"  Batch {i}: {ok} ok, {fail} fail, {dt:.0f}ms")
    t_seq = time.time() - t_start

    print(f"\n  Sequential: {total_ok} ok, {total_fail} fail in {t_seq*1000:.0f}ms")
    print(f"  Offered: {N/t_seq:.0f} TPS")
    if all_errors:
        print("  Errors:", dict(list(all_errors.items())[:5]))

    # --- Test 2: Parallel (actual throughput) ---
    # Re-sign with fresh nonces
    fresh_nonce = int(rpc_single("eth_getTransactionCount", [acct.address, "pending"])["result"], 16)
    print(f"\n  (Waiting 5s for chain to settle, fresh nonce: {fresh_nonce})")
    time.sleep(5)
    fresh_nonce = int(rpc_single("eth_getTransactionCount", [acct.address, "pending"])["result"], 16)

    print(f"\n{'='*60}")
    print(f"Test 2: PARALLEL — {args.workers} workers, concurrent connections")
    print(f"{'='*60}")
    print(f"  Re-signing {N} txs from nonce {fresh_nonce}...")
    signed2 = []
    for i in range(N):
        tx = {
            "nonce": fresh_nonce + i,
            "to": acct.address,
            "value": 0,
            "gas": 60_000,
            "gasPrice": gas_price,
            "chainId": CHAIN_ID,
            "type": 0,
        }
        stx = acct.sign_transaction(tx)
        signed2.append("0x" + stx.raw_transaction.hex())
    batches2 = [signed2[i:i + args.batch] for i in range(0, N, args.batch)]

    lock = threading.Lock()
    par_ok = 0
    par_fail = 0
    par_errors = {}
    par_hashes = []

    def submit_worker(batch_idx, batch_data):
        nonlocal par_ok, par_fail
        s = requests.Session()
        result = send_batch(s, batch_data, batch_idx)
        ok, fail, dt, errors = result[:4]
        hashes = result[4] if len(result) > 4 else []
        with lock:
            par_ok += ok
            par_fail += fail
            par_hashes.extend(hashes)
            for msg, cnt in errors.items():
                par_errors[msg] = par_errors.get(msg, 0) + cnt
        return batch_idx, ok, fail, dt

    t_start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(submit_worker, i, b) for i, b in enumerate(batches2)]
        for f in as_completed(futures):
            idx, ok, fail, dt = f.result()
            print(f"  Batch {idx}: {ok} ok, {fail} fail, {dt:.0f}ms")
    t_par = time.time() - t_start

    print(f"\n  Parallel: {par_ok} ok, {par_fail} fail in {t_par*1000:.0f}ms")
    print(f"  Offered: {N/t_par:.0f} TPS  (speedup: {t_seq/t_par:.1f}x)")
    if par_errors:
        print("  Errors:", dict(list(par_errors.items())[:5]))

    # --- Wait for confirmations ---
    print(f"\n{'='*60}")
    print("Waiting for confirmations (15s)...")
    print(f"{'='*60}")
    time.sleep(15)
    final_nonce = int(rpc_single("eth_getTransactionCount", [acct.address, "latest"])["result"], 16)
    total_confirmed = final_nonce - nonce
    print(f"  Nonce: {nonce} → {final_nonce} ({total_confirmed} total confirmed)")
    print()

    print("SUMMARY:")
    print(f"  Sequential submit: {N/t_seq:.0f} TPS (HTTP serialization is the bottleneck)")
    print(f"  Parallel submit:   {N/t_par:.0f} TPS ({args.workers} connections)")
    print(f"  The gap between these tells you if parallelism helps vs. RPC limit")

if __name__ == "__main__":
    main()
