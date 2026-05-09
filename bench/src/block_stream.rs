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
        // Initialize to current tip so we don't try to fetch historical blocks
        let mut last_block = match provider.get_block_number().await {
            Ok(n) => n,
            Err(e) => {
                warn!("initial eth_blockNumber failed: {e}, starting from 0");
                0
            }
        };
        debug!("block stream initialized at block {last_block}");

        loop {
            tokio::time::sleep(poll_interval).await;

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
                                    let _ = tx_clone.send(notif);
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
        }
    });

    tx
}
