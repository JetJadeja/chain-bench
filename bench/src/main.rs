mod batch_rpc;
mod block_stream;
mod cli;
mod config;
mod contracts;
mod fund;
mod metrics;
mod nonce;
mod ramp;
mod simulate;
mod tx_tracker;
mod wallet;

use clap::Parser;
use cli::{Cli, Command};
mod market;
mod market_stats;

use config::{FundConfig, MarketConfig, SimulateConfig};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    dotenvy::from_path("../.env").ok();
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    match &cli.command {
        Command::Fund(args) => {
            let config = FundConfig::new(&cli, args)?;
            fund::run(config).await?;
        }
        Command::Simulate(args) => {
            let config = SimulateConfig::new(&cli, args)?;
            simulate::run(config).await?;
        }
        Command::Market(args) => {
            let config = MarketConfig::new(&cli, args)?;
            market::run(config).await?;
        }
    }

    Ok(())
}
