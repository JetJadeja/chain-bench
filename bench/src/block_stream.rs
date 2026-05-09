use std::time::{Duration, Instant};

use alloy::providers::Provider;
use tokio::sync::broadcast;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct BlockNotification {
    pub number: u64,
    pub timestamp: u64,
    pub observed_at: Instant,
}

pub fn start_polling(
    provider: impl Provider + Clone + 'static,
    poll_interval: Duration,
) -> broadcast::Sender<BlockNotification> {
    let (tx, _) = broadcast::channel::<BlockNotification>(256);
    let tx_clone = tx.clone();

    tokio::spawn(async move {
        let mut last_block = 0u64;

        loop {
            match provider.get_block_number().await {
                Ok(tip) => {
                    if tip > last_block {
                        for n in (last_block + 1)..=tip {
                            let observed_at = Instant::now();
                            match provider
                                .get_block_by_number(n.into())
                                .await
                            {
                                Ok(Some(block)) => {
                                    let notif = BlockNotification {
                                        number: n,
                                        timestamp: block.header.timestamp,
                                        observed_at,
                                    };
                                    if tx_clone.send(notif).is_err() {
                                        debug!("no block stream receivers, stopping");
                                        return;
                                    }
                                }
                                Ok(None) => {
                                    warn!("block {n} returned None");
                                }
                                Err(e) => {
                                    warn!("failed to fetch block {n}: {e}");
                                }
                            }
                        }
                        last_block = tip;
                    }
                }
                Err(e) => {
                    warn!("eth_blockNumber failed: {e}");
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    });

    tx
}
