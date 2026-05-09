use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bench", about = "EVM chain benchmark tool")]
pub struct Cli {
    #[arg(long, env = "MEGAETH_RPC")]
    pub rpc: String,

    #[arg(long, env = "CHAIN_ID", default_value = "4326")]
    pub chain_id: u64,

    #[arg(long, env = "MEGAETH_VAULT")]
    pub vault: String,

    #[arg(long, env = "MEGAETH_MOCK_TOKEN")]
    pub token: String,

    #[arg(long, env = "DEPLOYER_PRIVATE_KEY")]
    pub deployer_key: String,

    #[arg(long, env = "MNEMONIC")]
    pub mnemonic: String,

    #[arg(long)]
    pub num_accounts: u32,

    /// Gas price in wei. If omitted, fetched from the RPC.
    #[arg(long)]
    pub gas_price: Option<u128>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Fund(FundArgs),
    Simulate(SimulateArgs),
    Market(MarketArgs),
}

#[derive(Parser)]
pub struct FundArgs {
    #[arg(long, default_value = "1000000000000000000000000")]
    pub token_amount: String,

    #[arg(long, default_value = "0.0001")]
    pub eth_amount: f64,

    /// Number of operator wallets to set up (ETH + OPERATOR_ROLE). 0 = skip.
    #[arg(long, default_value = "0")]
    pub num_operators: u32,

    /// ETH to send each operator (enough for gas). Scales with gas price if omitted.
    #[arg(long, default_value = "0.1")]
    pub operator_eth: f64,
}

#[derive(Parser)]
pub struct SimulateArgs {
    #[arg(long, default_value = "10.0")]
    pub rate: f64,

    #[arg(long, default_value = "120")]
    pub duration: u64,

    #[arg(long, default_value = "10")]
    pub warmup: u64,

    #[arg(long, default_value = "10")]
    pub ramp_step: u64,

    #[arg(long, default_value = "1.5")]
    pub ramp_multiplier: f64,

    #[arg(long, default_value = "2.0")]
    pub spike_multiplier: f64,

    #[arg(long, default_value = "10")]
    pub spike_duration: u64,

    #[arg(long, default_value = "10")]
    pub recovery: u64,

    #[arg(long, default_value = "results.csv")]
    pub output: PathBuf,

    #[arg(long, default_value = "1000000000000000000")]
    pub match_amount_min: String,

    #[arg(long, default_value = "10000000000000000000")]
    pub match_amount_max: String,
}

#[derive(Parser)]
pub struct MarketArgs {
    /// Number of operator wallets for parallel burst submission.
    #[arg(long, default_value = "1")]
    pub num_operators: u32,

    /// Number of txs in the burst.
    #[arg(long, default_value = "1000")]
    pub burst_size: usize,

    /// Steady-state TPS during the first phase.
    #[arg(long, default_value = "100.0")]
    pub steady_rate: f64,

    /// Duration of the steady phase in seconds.
    #[arg(long, default_value = "30")]
    pub steady_duration: u64,

    /// Duration of the ramp phase in seconds.
    #[arg(long, default_value = "10")]
    pub ramp_duration: u64,

    /// Output CSV path.
    #[arg(long, default_value = "market_results.csv")]
    pub output: PathBuf,

    /// Block poll interval in milliseconds.
    #[arg(long, default_value = "50")]
    pub poll_interval_ms: u64,

    /// Min match amount in token base units.
    #[arg(long, default_value = "1000000000000000000")]
    pub match_amount_min: String,

    /// Max match amount in token base units.
    #[arg(long, default_value = "10000000000000000000")]
    pub match_amount_max: String,

    /// Max txs per JSON-RPC batch in burst mode. 0 = send all in one batch (targets single-block inclusion).
    #[arg(long, default_value = "0")]
    pub burst_chunk: usize,
}
