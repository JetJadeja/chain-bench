#!/usr/bin/env bash
set -euo pipefail

# End-to-end test: start Anvil, deploy contracts, fund wallets, run market simulation.
# Usage: ./test_anvil.sh [--burst-size N] [--num-operators N] [--num-accounts N]

BURST_SIZE="${1:-20}"
NUM_OPERATORS="${2:-1}"
NUM_ACCOUNTS="${3:-5}"

ANVIL_PORT=8546
RPC="http://127.0.0.1:${ANVIL_PORT}"
# Anvil's default first private key
DEPLOYER_KEY="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
# Deterministic mnemonic for test wallets
MNEMONIC="test test test test test test test test test test test junk"

cleanup() {
    if [[ -n "${ANVIL_PID:-}" ]]; then
        kill "$ANVIL_PID" 2>/dev/null || true
        wait "$ANVIL_PID" 2>/dev/null || true
    fi
    rm -f /tmp/anvil_test_deploy.log
}
trap cleanup EXIT

echo "=== Starting Anvil on port ${ANVIL_PORT} ==="
anvil --port "$ANVIL_PORT" --block-time 1 --silent &
ANVIL_PID=$!
sleep 1

# Verify Anvil is up
if ! curl -s "$RPC" -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' > /dev/null; then
    echo "ERROR: Anvil not responding"
    exit 1
fi
echo "  Anvil running (PID ${ANVIL_PID})"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTRACTS_DIR="${SCRIPT_DIR}/../contracts"
BENCH_DIR="${SCRIPT_DIR}"

echo ""
echo "=== Deploying contracts ==="

DEPLOY_OUTPUT=$(cd "$CONTRACTS_DIR" && OPERATOR_KEY="$DEPLOYER_KEY" forge script script/Deploy.s.sol:Deploy \
    --rpc-url "$RPC" \
    --private-key "$DEPLOYER_KEY" \
    --broadcast 2>&1)

TOKEN=$(echo "$DEPLOY_OUTPUT" | grep "token " | awk '{print $NF}')
VAULT=$(echo "$DEPLOY_OUTPUT" | grep "vault " | awk '{print $NF}')

if [[ -z "$TOKEN" || -z "$VAULT" ]]; then
    echo "ERROR: Failed to parse deployed addresses"
    echo "$DEPLOY_OUTPUT"
    exit 1
fi
echo "  Token: ${TOKEN}"
echo "  Vault: ${VAULT}"

echo ""
echo "=== Building bench tool ==="
cd "$BENCH_DIR"
cargo build --quiet 2>&1

echo ""
echo "=== Funding ${NUM_ACCOUNTS} wallets + ${NUM_OPERATORS} operator(s) ==="
cargo run --quiet -- \
    --rpc "$RPC" \
    --chain-id 31337 \
    --vault "$VAULT" \
    --token "$TOKEN" \
    --deployer-key "$DEPLOYER_KEY" \
    --mnemonic "$MNEMONIC" \
    --num-accounts "$NUM_ACCOUNTS" \
    fund \
    --num-operators "$NUM_OPERATORS" \
    --operator-eth 1.0 \
    2>&1 | grep -E "(funding|wallet|operator|complete|error)" || true

echo ""
echo "=== Running market simulation ==="
echo "    operators: ${NUM_OPERATORS}"
echo "    burst size: ${BURST_SIZE}"
echo "    accounts: ${NUM_ACCOUNTS}"
echo ""

cargo run --quiet -- \
    --rpc "$RPC" \
    --chain-id 31337 \
    --vault "$VAULT" \
    --token "$TOKEN" \
    --deployer-key "$DEPLOYER_KEY" \
    --mnemonic "$MNEMONIC" \
    --num-accounts "$NUM_ACCOUNTS" \
    market \
    --num-operators "$NUM_OPERATORS" \
    --burst-size "$BURST_SIZE" \
    --steady-rate 1.0 \
    --steady-duration 5 \
    --ramp-duration 3 \
    --poll-interval-ms 100 \
    --output /tmp/market_test_results.csv \
    2>&1

echo ""
echo "=== Results CSV ==="
if [[ -f /tmp/market_test_results.csv ]]; then
    head -5 /tmp/market_test_results.csv
    TOTAL=$(wc -l < /tmp/market_test_results.csv)
    echo "  ... (${TOTAL} total rows)"
else
    echo "  WARNING: no results CSV generated"
fi

echo ""
echo "=== Test complete ==="
