#!/usr/bin/env python3
"""
Test mempool acceptance limits on MegaETH.

Pre-signs N transactions with sequential nonces and submits them in batches.
Measures how many the RPC/mempool accepts before rejecting.

Deps: pip install eth-account requests
Usage: python3 mempool_probe.py              # 100 txs, batch size 20
       python3 mempool_probe.py --count 500 --batch 50
"""
import argparse
import json
import os
import sys
import time

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

def rpc_batch(calls):
    resp = requests.post(RPC_URL, json=calls, timeout=30)
    return resp.status_code, resp.json() if resp.status_code == 200 else resp.text

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--batch", type=int, default=20)
    args = parser.parse_args()

    load_env()
    key = os.environ.get("DEPLOYER_PRIVATE_KEY")
    if not key or not RPC_URL:
        print("Set DEPLOYER_PRIVATE_KEY and MEGAETH_RPC in .env")
        sys.exit(1)

    acct = Account.from_key(key)
    print(f"Account: {acct.address}")

    nonce_hex = rpc_single("eth_getTransactionCount", [acct.address, "pending"])["result"]
    nonce = int(nonce_hex, 16)
    gas_price_hex = rpc_single("eth_gasPrice", [])["result"]
    gas_price = int(gas_price_hex, 16)
    print(f"Nonce: {nonce}, Gas price: {gas_price} wei")
    print()

    # Pre-sign N txs: self-transfer of 0 value
    N = args.count
    print(f"Pre-signing {N} txs (nonces {nonce}..{nonce + N - 1})...")
    signed = []
    t0 = time.time()
    for i in range(N):
        tx = {
            "nonce": nonce + i,
            "to": acct.address,
            "value": 0,
            "gas": 60_000,
            "gasPrice": gas_price,
            "chainId": CHAIN_ID,
        }
        stx = acct.sign_transaction(tx)
        signed.append("0x" + stx.raw_transaction.hex())
    sign_time = time.time() - t0
    print(f"  Signed in {sign_time*1000:.0f}ms ({N/sign_time:.0f} signs/sec)")
    print()

    # Submit in batches, record what the RPC says
    BATCH = args.batch
    total_ok = 0
    total_fail = 0
    errors = {}
    batch_results = []

    print(f"Submitting in batches of {BATCH}...")
    overall_start = time.time()

    for chunk_start in range(0, N, BATCH):
        chunk = signed[chunk_start : chunk_start + BATCH]
        batch_req = [
            {"jsonrpc": "2.0", "id": i, "method": "eth_sendRawTransaction",
             "params": [raw]}
            for i, raw in enumerate(chunk)
        ]

        t0 = time.time()
        status_code, result = rpc_batch(batch_req)
        dt = (time.time() - t0) * 1000

        if status_code != 200:
            print(f"  [{chunk_start:>4}-{chunk_start+len(chunk)-1:>4}] HTTP {status_code} in {dt:.0f}ms")
            total_fail += len(chunk)
            batch_results.append({"range": (chunk_start, chunk_start+len(chunk)-1), "ok": 0, "fail": len(chunk), "http": status_code})
            if status_code == 429:
                print("    Rate limited! Backing off 2s...")
                time.sleep(2)
            continue

        ok = 0
        fail = 0
        for r in result:
            if r.get("error"):
                fail += 1
                msg = r["error"].get("message", str(r["error"]))
                errors[msg] = errors.get(msg, 0) + 1
            else:
                ok += 1

        total_ok += ok
        total_fail += fail
        batch_results.append({"range": (chunk_start, chunk_start+len(chunk)-1), "ok": ok, "fail": fail})
        print(f"  [{chunk_start:>4}-{chunk_start+len(chunk)-1:>4}] {ok} ok, {fail} fail, {dt:.0f}ms")

    submit_time = time.time() - overall_start

    print()
    print(f"=== Mempool Acceptance Results ===")
    print(f"Total submitted: {N}")
    print(f"Accepted: {total_ok}  Rejected: {total_fail}")
    print(f"Submit time: {submit_time*1000:.0f}ms ({N/submit_time:.0f} offered/sec)")
    print()

    if errors:
        print("Error breakdown:")
        for msg, count in sorted(errors.items(), key=lambda x: -x[1]):
            print(f"  {count:>4}x  {msg}")
        print()

    # Check where rejections start
    for br in batch_results:
        if br["fail"] > 0:
            print(f"First rejection in range {br['range']}")
            print(f"  → Mempool likely accepts ~{br['range'][0]} pending txs per sender")
            break

    print()
    print("Waiting 10s then checking receipts...")
    time.sleep(10)

    # Batch check receipts for accepted txs
    # We don't have the hashes stored conveniently, so re-check nonce
    final_nonce_hex = rpc_single("eth_getTransactionCount", [acct.address, "latest"])["result"]
    final_nonce = int(final_nonce_hex, 16)
    confirmed = final_nonce - nonce
    print(f"Nonce advanced: {nonce} → {final_nonce} ({confirmed} txs confirmed on-chain)")

if __name__ == "__main__":
    main()
