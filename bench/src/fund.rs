use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::sol_types::SolCall;
use eyre::Result;
use std::time::Instant;
use tracing::{info, warn};

use crate::batch_rpc::BatchRpcClient;
use crate::config::FundConfig;
use crate::contracts::{MockToken, Vault};
use crate::wallet;

const GAS_LIMIT_TRANSFER: u64 = 60_000;
const GAS_LIMIT_MINT: u64 = 200_000;
const GAS_LIMIT_APPROVE: u64 = 200_000;
const GAS_LIMIT_GRANT_ROLE: u64 = 200_000;
const OPERATOR_ROLE: U256 = U256::from_limbs([1, 0, 0, 0]);
const BATCH_CHUNK: usize = 500;

struct WalletStatus {
    address: Address,
    eth_deficit: U256,
    mint_amount: U256,
    needs_approve: bool,
}

pub async fn run(config: FundConfig) -> Result<()> {
    let t0 = Instant::now();
    let batch = BatchRpcClient::new(&config.base.rpc_url);
    let provider = ProviderBuilder::new().connect_http(config.base.rpc_url.parse()?);

    let gas_price = match config.base.gas_price {
        Some(p) => p,
        None => provider.get_gas_price().await?,
    };
    info!("gas price: {gas_price} wei");

    // Wallets need enough ETH for one approve tx. Use 3x buffer over gas_limit * gas_price
    // to cover priority fees and any chain-specific overhead.
    let min_wallet_eth =
        U256::from(GAS_LIMIT_APPROVE) * U256::from(gas_price) * U256::from(3);
    let wallet_eth_amount = if config.eth_amount_wei > min_wallet_eth {
        config.eth_amount_wei
    } else {
        min_wallet_eth
    };
    info!("wallet ETH amount: {} wei", wallet_eth_amount);

    let wallets = wallet::derive_wallets(&config.base.mnemonic, config.base.num_accounts)?;
    let addresses: Vec<Address> = wallets.iter().map(|w| w.address()).collect();
    let deployer_addr = config.base.deployer_key.address();

    info!(
        "funding {} wallets from deployer {}",
        wallets.len(),
        deployer_addr
    );

    // ── Phase 1: Batch scan all wallets ──────────────────────────────
    let t_scan = Instant::now();

    let balance_calls: Vec<(Address, Vec<u8>)> = addresses
        .iter()
        .map(|addr| {
            let call = MockToken::balanceOfCall { account: *addr };
            (config.base.token, call.abi_encode())
        })
        .collect();

    let allowance_calls: Vec<(Address, Vec<u8>)> = addresses
        .iter()
        .map(|addr| {
            let call = MockToken::allowanceCall {
                owner: *addr,
                spender: config.base.vault,
            };
            (config.base.token, call.abi_encode())
        })
        .collect();

    // Fire all three batch scans concurrently
    let (eth_balances, token_results, allowance_results) = tokio::try_join!(
        batch.batch_get_balances(&addresses),
        batch.batch_eth_calls(&balance_calls),
        batch.batch_eth_calls(&allowance_calls),
    )?;

    let mut statuses = Vec::with_capacity(addresses.len());
    let mut eth_count = 0usize;
    let mut mint_count = 0usize;
    let mut approve_count = 0usize;

    for (i, addr) in addresses.iter().enumerate() {
        let eth_bal = eth_balances[i];
        let tok_bal = U256::from_be_slice(&token_results[i]);
        let allowance = U256::from_be_slice(&allowance_results[i]);

        let eth_deficit = if eth_bal < wallet_eth_amount {
            wallet_eth_amount - eth_bal
        } else {
            U256::ZERO
        };
        let mint_amount = if tok_bal < config.token_amount {
            config.token_amount - tok_bal
        } else {
            U256::ZERO
        };
        let needs_approve = allowance < config.token_amount;

        if eth_deficit > U256::ZERO {
            eth_count += 1;
        }
        if mint_amount > U256::ZERO {
            mint_count += 1;
        }
        if needs_approve {
            approve_count += 1;
        }

        statuses.push(WalletStatus {
            address: *addr,
            eth_deficit,
            mint_amount,
            needs_approve,
        });
    }

    info!(
        "scanned {} wallets in {:.0}ms — {} need ETH, {} need mint, {} need approve",
        addresses.len(),
        t_scan.elapsed().as_millis(),
        eth_count,
        mint_count,
        approve_count
    );

    // ── Phase 2: Sign and batch-send deployer txs (ETH sends + mints) ──
    let deployer_wallet = EthereumWallet::from(config.base.deployer_key.clone());
    let mut deployer_nonce = provider.get_transaction_count(deployer_addr).await?;
    let mut all_deployer_hashes: Vec<alloy::primitives::TxHash> = Vec::new();

    if eth_count > 0 || mint_count > 0 {
        let t_deploy = Instant::now();
        let mut raw_txs: Vec<Vec<u8>> = Vec::with_capacity(eth_count + mint_count);

        // ETH sends (only the deficit)
        for s in &statuses {
            if s.eth_deficit == U256::ZERO {
                continue;
            }
            let tx = TransactionRequest::default()
                .with_to(s.address)
                .with_value(s.eth_deficit)
                .with_nonce(deployer_nonce)
                .with_gas_limit(GAS_LIMIT_TRANSFER)
                .with_gas_price(gas_price)
                .with_chain_id(config.base.chain_id);

            let envelope = tx.build(&deployer_wallet).await?;
            let mut encoded = Vec::new();
            alloy::rlp::Encodable::encode(&envelope, &mut encoded);
            raw_txs.push(encoded);
            deployer_nonce += 1;
        }

        // Mints
        for s in &statuses {
            if s.mint_amount == U256::ZERO {
                continue;
            }
            let call = MockToken::mintCall {
                to: s.address,
                amount: s.mint_amount,
            };
            let tx = TransactionRequest::default()
                .with_to(config.base.token)
                .with_input(call.abi_encode())
                .with_nonce(deployer_nonce)
                .with_gas_limit(GAS_LIMIT_MINT)
                .with_gas_price(gas_price)
                .with_chain_id(config.base.chain_id);

            let envelope = tx.build(&deployer_wallet).await?;
            let mut encoded = Vec::new();
            alloy::rlp::Encodable::encode(&envelope, &mut encoded);
            raw_txs.push(encoded);
            deployer_nonce += 1;
        }

        info!("signed {} deployer txs, batch-sending...", raw_txs.len());

        // MegaETH has a 100 tx per-sender mempool limit. Send in chunks of 90,
        // wait for confirmation, then send next chunk.
        const DEPLOYER_CHUNK: usize = 90;
        for chunk in raw_txs.chunks(DEPLOYER_CHUNK) {
            let results = batch.batch_send_raw(chunk).await?;
            let mut chunk_hashes = Vec::new();
            for (i, result) in results.into_iter().enumerate() {
                match result {
                    Ok(hash) => chunk_hashes.push(hash),
                    Err(e) => warn!("deployer tx {i} send failed: {e}"),
                }
            }

            // Wait for this chunk to confirm before sending next
            let mut pending = chunk_hashes.clone();
            while !pending.is_empty() {
                let mut still_pending = Vec::new();
                for poll_chunk in pending.chunks(BATCH_CHUNK) {
                    let receipts = batch.batch_receipts(poll_chunk).await?;
                    for (hash, maybe_receipt) in receipts {
                        match maybe_receipt {
                            Some(receipt) if !receipt.status() => {
                                warn!("deployer tx {} REVERTED", hash);
                            }
                            Some(_) => {}
                            None => still_pending.push(hash),
                        }
                    }
                }
                if !still_pending.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                pending = still_pending;
            }

            all_deployer_hashes.extend(chunk_hashes);
            info!(
                "  deployer chunk done ({}/{})",
                all_deployer_hashes.len(),
                raw_txs.len()
            );
        }

        info!(
            "sent {} deployer txs in {:.0}ms",
            all_deployer_hashes.len(),
            t_deploy.elapsed().as_millis()
        );
    }

    // ── Phase 3: Sign and batch-send approval txs ──────────────────
    let mut all_approve_hashes: Vec<alloy::primitives::TxHash> = Vec::new();

    if approve_count > 0 {
        let t_approve = Instant::now();

        // Batch-fetch nonces for wallets that need approval
        let approve_indices: Vec<usize> = statuses
            .iter()
            .enumerate()
            .filter(|(_, s)| s.needs_approve)
            .map(|(i, _)| i)
            .collect();

        let approve_addrs: Vec<Address> = approve_indices.iter().map(|&i| addresses[i]).collect();
        let nonces = batch.batch_get_nonces(&approve_addrs).await?;

        // Sign all approve txs locally
        let mut raw_approvals: Vec<Vec<u8>> = Vec::with_capacity(approve_count);
        for (j, &idx) in approve_indices.iter().enumerate() {
            let w = &wallets[idx];
            let wallet = EthereumWallet::from(w.clone());

            let call = MockToken::approveCall {
                spender: config.base.vault,
                amount: U256::MAX,
            };
            let tx = TransactionRequest::default()
                .with_to(config.base.token)
                .with_input(call.abi_encode())
                .with_nonce(nonces[j])
                .with_gas_limit(GAS_LIMIT_APPROVE)
                .with_gas_price(gas_price)
                .with_chain_id(config.base.chain_id);

            let envelope = tx.build(&wallet).await?;
            let mut encoded = Vec::new();
            alloy::rlp::Encodable::encode(&envelope, &mut encoded);
            raw_approvals.push(encoded);
        }

        info!("signed {} approve txs, batch-sending...", raw_approvals.len());

        for chunk in raw_approvals.chunks(BATCH_CHUNK) {
            let results = batch.batch_send_raw(chunk).await?;
            for (i, result) in results.into_iter().enumerate() {
                match result {
                    Ok(hash) => all_approve_hashes.push(hash),
                    Err(e) => warn!("approve tx {i} send failed: {e}"),
                }
            }
        }

        info!(
            "sent {} approve txs in {:.0}ms",
            all_approve_hashes.len(),
            t_approve.elapsed().as_millis()
        );
    }

    // ── Phase 4: Wait for approval receipts ────────────────────────
    if !all_approve_hashes.is_empty() {
        let t_wait = Instant::now();
        info!("waiting for {} approval receipts...", all_approve_hashes.len());

        let mut pending: Vec<alloy::primitives::TxHash> = all_approve_hashes;
        let mut confirmed = 0usize;
        let mut reverted = 0usize;

        while !pending.is_empty() {
            let mut still_pending = Vec::new();

            for chunk in pending.chunks(BATCH_CHUNK) {
                let results = batch.batch_receipts(chunk).await?;
                for (hash, maybe_receipt) in results {
                    match maybe_receipt {
                        Some(receipt) => {
                            if receipt.status() {
                                confirmed += 1;
                            } else {
                                reverted += 1;
                                warn!("tx {} REVERTED", hash);
                            }
                        }
                        None => still_pending.push(hash),
                    }
                }
            }

            if !still_pending.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            pending = still_pending;
        }

        info!(
            "all receipts in {:.0}ms — {} confirmed, {} reverted",
            t_wait.elapsed().as_millis(),
            confirmed,
            reverted
        );
    }

    // ── Phase 5: Operator setup ──────────────────────────────────────
    if config.num_operators > 0 {
        let t_ops = Instant::now();
        info!("setting up {} operator wallet(s)", config.num_operators);

        let op_keys = wallet::derive_operators(&config.base.mnemonic, config.num_operators)?;
        let op_addrs: Vec<Address> = op_keys.iter().map(|k| k.address()).collect();

        // Batch scan operator state
        let role_calls: Vec<(Address, Vec<u8>)> = op_addrs
            .iter()
            .map(|addr| {
                let call = Vault::hasAnyRoleCall {
                    user: *addr,
                    roles: OPERATOR_ROLE,
                };
                (config.base.vault, call.abi_encode())
            })
            .collect();

        let (op_balances, role_results) = tokio::try_join!(
            batch.batch_get_balances(&op_addrs),
            batch.batch_eth_calls(&role_calls),
        )?;

        let mut op_raw_txs: Vec<Vec<u8>> = Vec::new();

        for (i, key) in op_keys.iter().enumerate() {
            let addr = key.address();
            let has_role = !role_results[i].is_empty() && role_results[i][31] == 1;
            let eth_deficit = if op_balances[i] < config.operator_eth_wei {
                config.operator_eth_wei - op_balances[i]
            } else {
                U256::ZERO
            };
            let needs_role = !has_role;

            if eth_deficit == U256::ZERO && !needs_role {
                info!("operator {i} ({addr}): ok");
                continue;
            }

            info!(
                "operator {i} ({addr}): ETH:{} ROLE:{}",
                if eth_deficit > U256::ZERO { "need" } else { "ok" },
                if needs_role { "need" } else { "ok" },
            );

            if eth_deficit > U256::ZERO {
                let tx = TransactionRequest::default()
                    .with_to(addr)
                    .with_value(eth_deficit)
                    .with_nonce(deployer_nonce)
                    .with_gas_limit(GAS_LIMIT_TRANSFER)
                    .with_gas_price(gas_price)
                    .with_chain_id(config.base.chain_id);
                let envelope = tx.build(&deployer_wallet).await?;
                let mut encoded = Vec::new();
                alloy::rlp::Encodable::encode(&envelope, &mut encoded);
                op_raw_txs.push(encoded);
                deployer_nonce += 1;
            }

            if needs_role {
                let call = Vault::grantRolesCall {
                    user: addr,
                    roles: OPERATOR_ROLE,
                };
                let tx = TransactionRequest::default()
                    .with_to(config.base.vault)
                    .with_input(call.abi_encode())
                    .with_nonce(deployer_nonce)
                    .with_gas_limit(GAS_LIMIT_GRANT_ROLE)
                    .with_gas_price(gas_price)
                    .with_chain_id(config.base.chain_id);
                let envelope = tx.build(&deployer_wallet).await?;
                let mut encoded = Vec::new();
                alloy::rlp::Encodable::encode(&envelope, &mut encoded);
                op_raw_txs.push(encoded);
                deployer_nonce += 1;
            }
        }

        if !op_raw_txs.is_empty() {
            let results = batch.batch_send_raw(&op_raw_txs).await?;
            let mut op_hashes = Vec::new();
            for result in results {
                match result {
                    Ok(hash) => op_hashes.push(hash),
                    Err(e) => warn!("operator tx failed: {e}"),
                }
            }

            // Wait for operator txs
            let mut pending = op_hashes;
            while !pending.is_empty() {
                let mut still_pending = Vec::new();
                let results = batch.batch_receipts(&pending).await?;
                for (hash, maybe_receipt) in results {
                    match maybe_receipt {
                        Some(receipt) if receipt.status() => {
                            info!("operator tx {} confirmed", hash);
                        }
                        Some(_) => warn!("operator tx {} REVERTED", hash),
                        None => still_pending.push(hash),
                    }
                }
                if !still_pending.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                pending = still_pending;
            }
        }

        info!("operators done in {:.0}ms", t_ops.elapsed().as_millis());
    }

    info!("funding complete in {:.1}s", t0.elapsed().as_secs_f64());
    Ok(())
}
