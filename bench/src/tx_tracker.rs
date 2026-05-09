use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::TxHash;
use alloy::rpc::types::TransactionReceipt;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use crate::batch_rpc::BatchRpcClient;
use crate::block_stream::BlockNotification;

const DROP_TIMEOUT: Duration = Duration::from_secs(60);

pub struct PendingTx {
    pub tx_hash: TxHash,
    pub nonce: u64,
    pub operator: String,
    pub operator_idx: Option<usize>,
    pub t_submit: Instant,
    pub t_submit_epoch_ms: i64,
    pub phase: String,
    pub burst_id: Option<u64>,
}

pub type InflightCounters = Arc<Vec<AtomicUsize>>;

#[derive(Debug, Clone, Serialize)]
pub struct TxLifecycle {
    pub tx_hash: String,
    pub nonce: u64,
    pub operator: String,
    pub t_submit_ms: i64,
    pub t_included_ms: Option<i64>,
    pub latency_ms: Option<i64>,
    pub block_number: Option<u64>,
    pub block_timestamp: Option<u64>,
    pub gas_used: Option<u64>,
    pub effective_gas_price: Option<u128>,
    pub status: String,
    pub phase: String,
    pub burst_id: Option<u64>,
    pub error_message: Option<String>,
}

pub async fn run(
    batch_client: BatchRpcClient,
    mut pending_rx: mpsc::Receiver<PendingTx>,
    mut block_rx: broadcast::Receiver<BlockNotification>,
    record_tx: mpsc::Sender<TxLifecycle>,
    inflight: Option<InflightCounters>,
) {
    let mut pending: HashMap<TxHash, PendingTx> = HashMap::new();
    let mut submitter_done = false;

    loop {
        drain_pending(&mut pending_rx, &mut pending, &mut submitter_done);

        if submitter_done && pending.is_empty() {
            info!("tracker: all txs resolved, shutting down");
            break;
        }

        match block_rx.recv().await {
            Ok(block) => {
                debug!(
                    "tracker: block {} with {} pending txs",
                    block.number,
                    pending.len()
                );
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("tracker lagged by {n} blocks");
            }
            Err(broadcast::error::RecvError::Closed) => {
                debug!("block stream closed");
                break;
            }
        }

        drain_pending(&mut pending_rx, &mut pending, &mut submitter_done);

        if pending.is_empty() {
            continue;
        }

        const RECEIPT_CHUNK: usize = 50;
        const RECEIPT_INFLIGHT: usize = 5;
        let hashes: Vec<TxHash> = pending.keys().copied().collect();
        let mut all_results: Vec<(TxHash, Option<TransactionReceipt>)> = Vec::new();

        let mut receipt_tasks: JoinSet<Vec<(TxHash, Option<TransactionReceipt>)>> = JoinSet::new();
        for chunk in hashes.chunks(RECEIPT_CHUNK) {
            while receipt_tasks.len() >= RECEIPT_INFLIGHT {
                if let Some(Ok(results)) = receipt_tasks.join_next().await {
                    all_results.extend(results);
                }
            }
            let client = batch_client.clone();
            let chunk_owned: Vec<TxHash> = chunk.to_vec();
            receipt_tasks.spawn(async move {
                match client.batch_receipts(&chunk_owned).await {
                    Ok(results) => results,
                    Err(e) => {
                        tracing::warn!(
                            "batch receipt chunk failed ({} hashes): {e}",
                            chunk_owned.len()
                        );
                        Vec::new()
                    }
                }
            });
        }
        while let Some(Ok(results)) = receipt_tasks.join_next().await {
            all_results.extend(results);
        }

        let now = Instant::now();
        let now_epoch = chrono::Utc::now().timestamp_millis();

        for (hash, maybe_receipt) in all_results {
            if let Some(receipt) = maybe_receipt {
                if let Some(ptx) = pending.remove(&hash) {
                    if let (Some(counters), Some(idx)) = (&inflight, ptx.operator_idx) {
                        if idx < counters.len() {
                            counters[idx].fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                    let latency = now.duration_since(ptx.t_submit).as_millis() as i64;
                    let record = TxLifecycle {
                        tx_hash: format!("{hash}"),
                        nonce: ptx.nonce,
                        operator: ptx.operator,
                        t_submit_ms: ptx.t_submit_epoch_ms,
                        t_included_ms: Some(now_epoch),
                        latency_ms: Some(latency),
                        block_number: receipt.block_number,
                        block_timestamp: None,
                        gas_used: Some(receipt.gas_used),
                        effective_gas_price: Some(receipt.effective_gas_price),
                        status: if receipt.status() {
                            "confirmed".into()
                        } else {
                            "reverted".into()
                        },
                        phase: ptx.phase,
                        burst_id: ptx.burst_id,
                        error_message: None,
                    };
                    let _ = record_tx.send(record).await;
                }
            }
        }

        // Check for timed-out txs
        let timed_out: Vec<TxHash> = pending
            .iter()
            .filter(|(_, ptx)| now.duration_since(ptx.t_submit) > DROP_TIMEOUT)
            .map(|(hash, _)| *hash)
            .collect();

        for hash in timed_out {
            if let Some(ptx) = pending.remove(&hash) {
                if let (Some(counters), Some(idx)) = (&inflight, ptx.operator_idx) {
                    if idx < counters.len() {
                        counters[idx].fetch_sub(1, Ordering::Relaxed);
                    }
                }
                warn!("tx {} dropped (timeout)", hash);
                let record = TxLifecycle {
                    tx_hash: format!("{hash}"),
                    nonce: ptx.nonce,
                    operator: ptx.operator,
                    t_submit_ms: ptx.t_submit_epoch_ms,
                    t_included_ms: None,
                    latency_ms: None,
                    block_number: None,
                    block_timestamp: None,
                    gas_used: None,
                    effective_gas_price: None,
                    status: "dropped".into(),
                    phase: ptx.phase,
                    burst_id: ptx.burst_id,
                    error_message: None,
                };
                let _ = record_tx.send(record).await;
            }
        }
    }
}

fn drain_pending(
    rx: &mut mpsc::Receiver<PendingTx>,
    pending: &mut HashMap<TxHash, PendingTx>,
    done: &mut bool,
) {
    loop {
        match rx.try_recv() {
            Ok(ptx) => {
                pending.insert(ptx.tx_hash, ptx);
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                *done = true;
                break;
            }
        }
    }
}
