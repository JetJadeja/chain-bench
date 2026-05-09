use alloy::primitives::Address;
use alloy::providers::Provider;
use eyre::Result;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct NonceTracker {
    next: u64,
}

impl NonceTracker {
    pub async fn new(provider: &dyn Provider, address: Address) -> Result<Self> {
        let nonce = provider.get_transaction_count(address).await?;
        Ok(Self { next: nonce })
    }

    pub fn next(&mut self) -> u64 {
        let n = self.next;
        self.next += 1;
        n
    }
}

pub struct AtomicNonce {
    next: AtomicU64,
}

impl AtomicNonce {
    pub async fn new(provider: &dyn Provider, address: Address) -> Result<Self> {
        let nonce = provider.get_transaction_count(address).await?;
        Ok(Self {
            next: AtomicU64::new(nonce),
        })
    }

    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Ordering::SeqCst)
    }
}
