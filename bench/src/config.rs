use alloy::primitives::Address;
use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use eyre::{Result, WrapErr};
use std::str::FromStr;

use crate::cli::{Cli, FundArgs, MarketArgs, SimulateArgs};

pub struct Config {
    pub rpc_url: String,
    pub chain_id: u64,
    pub vault: Address,
    pub token: Address,
    pub deployer_key: PrivateKeySigner,
    pub mnemonic: String,
    pub num_accounts: u32,
    pub gas_price: Option<u128>,
}

pub struct FundConfig {
    pub base: Config,
    pub token_amount: U256,
    pub eth_amount_wei: U256,
    pub num_operators: u32,
    pub operator_eth_wei: U256,
}

pub struct SimulateConfig {
    pub base: Config,
    pub rate: f64,
    pub duration: u64,
    pub warmup: u64,
    pub ramp_step: u64,
    pub ramp_multiplier: f64,
    pub spike_multiplier: f64,
    pub spike_duration: u64,
    pub recovery: u64,
    pub output: std::path::PathBuf,
    pub match_amount_min: U256,
    pub match_amount_max: U256,
}

impl Config {
    pub fn from_cli(cli: &Cli) -> Result<Self> {
        let deployer_key: PrivateKeySigner = cli
            .deployer_key
            .parse()
            .wrap_err("invalid deployer private key")?;

        Ok(Self {
            rpc_url: cli.rpc.clone(),
            chain_id: cli.chain_id,
            vault: Address::from_str(&cli.vault).wrap_err("invalid vault address")?,
            token: Address::from_str(&cli.token).wrap_err("invalid token address")?,
            deployer_key,
            mnemonic: cli.mnemonic.clone(),
            num_accounts: cli.num_accounts,
            gas_price: cli.gas_price,
        })
    }
}

impl FundConfig {
    pub fn new(cli: &Cli, args: &FundArgs) -> Result<Self> {
        let base = Config::from_cli(cli)?;
        let token_amount =
            U256::from_str(&args.token_amount).wrap_err("invalid token amount")?;
        let eth_wei = (args.eth_amount * 1e18) as u128;
        let op_eth_wei = (args.operator_eth * 1e18) as u128;
        Ok(Self {
            base,
            token_amount,
            eth_amount_wei: U256::from(eth_wei),
            num_operators: args.num_operators,
            operator_eth_wei: U256::from(op_eth_wei),
        })
    }
}

pub struct MarketConfig {
    pub base: Config,
    pub num_operators: u32,
    pub burst_size: usize,
    pub steady_rate: f64,
    pub steady_duration: u64,
    pub ramp_duration: u64,
    pub output: std::path::PathBuf,
    pub poll_interval_ms: u64,
    pub match_amount_min: U256,
    pub match_amount_max: U256,
    pub burst_chunk: usize,
}

impl MarketConfig {
    pub fn new(cli: &Cli, args: &MarketArgs) -> Result<Self> {
        let base = Config::from_cli(cli)?;
        Ok(Self {
            base,
            num_operators: args.num_operators,
            burst_size: args.burst_size,
            steady_rate: args.steady_rate,
            steady_duration: args.steady_duration,
            ramp_duration: args.ramp_duration,
            output: args.output.clone(),
            poll_interval_ms: args.poll_interval_ms,
            match_amount_min: U256::from_str(&args.match_amount_min)
                .wrap_err("invalid match amount min")?,
            match_amount_max: U256::from_str(&args.match_amount_max)
                .wrap_err("invalid match amount max")?,
            burst_chunk: args.burst_chunk,
        })
    }
}

impl SimulateConfig {
    pub fn new(cli: &Cli, args: &SimulateArgs) -> Result<Self> {
        let base = Config::from_cli(cli)?;
        Ok(Self {
            base,
            rate: args.rate,
            duration: args.duration,
            warmup: args.warmup,
            ramp_step: args.ramp_step,
            ramp_multiplier: args.ramp_multiplier,
            spike_multiplier: args.spike_multiplier,
            spike_duration: args.spike_duration,
            recovery: args.recovery,
            output: args.output.clone(),
            match_amount_min: U256::from_str(&args.match_amount_min)
                .wrap_err("invalid match amount min")?,
            match_amount_max: U256::from_str(&args.match_amount_max)
                .wrap_err("invalid match amount max")?,
        })
    }
}
