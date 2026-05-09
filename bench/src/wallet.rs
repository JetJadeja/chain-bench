use alloy::primitives::Address;
use alloy::signers::local::{coins_bip39::English, MnemonicBuilder, PrivateKeySigner};
use eyre::Result;

/// Derive counterparty wallets starting at index 1 (index 0 is the deployer/operator).
pub fn derive_wallets(mnemonic: &str, count: u32) -> Result<Vec<PrivateKeySigner>> {
    (1..=count)
        .map(|i| {
            MnemonicBuilder::<English>::default()
                .phrase(mnemonic)
                .index(i)
                .map_err(|e| eyre::eyre!("invalid mnemonic config: {e}"))?
                .build()
                .map_err(|e| eyre::eyre!("failed to derive wallet {i}: {e}"))
        })
        .collect()
}

pub fn addresses(wallets: &[PrivateKeySigner]) -> Vec<Address> {
    wallets.iter().map(|w| w.address()).collect()
}

const OPERATOR_INDEX_OFFSET: u32 = 1000;

/// Derive operator wallets at indices 1000+ to avoid collision with counterparty wallets.
pub fn derive_operators(mnemonic: &str, count: u32) -> Result<Vec<PrivateKeySigner>> {
    (0..count)
        .map(|i| {
            let idx = OPERATOR_INDEX_OFFSET + i;
            MnemonicBuilder::<English>::default()
                .phrase(mnemonic)
                .index(idx)
                .map_err(|e| eyre::eyre!("invalid mnemonic config: {e}"))?
                .build()
                .map_err(|e| eyre::eyre!("failed to derive operator wallet {idx}: {e}"))
        })
        .collect()
}
