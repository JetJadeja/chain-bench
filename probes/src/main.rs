mod block_time;
mod mempool;
mod rpc;
mod throughput;

use clap::{Parser, Subcommand};
use eyre::Result;

#[derive(Parser)]
#[command(name = "probes", about = "Chain diagnostic probes")]
struct Cli {
    #[arg(long, env = "MEGAETH_RPC")]
    rpc: String,

    #[arg(long, env = "CHAIN_ID", default_value = "4326")]
    chain_id: u64,

    #[arg(long, env = "DEPLOYER_PRIVATE_KEY")]
    deployer_key: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Test mempool pending-tx limit per sender
    Mempool {
        /// Total transactions to submit
        #[arg(long, default_value = "300")]
        count: usize,

        /// Transactions per RPC batch
        #[arg(long, default_value = "20")]
        batch: usize,

        /// Concurrent batch submissions
        #[arg(long, default_value = "4")]
        workers: usize,

        /// Seconds to wait for confirmations
        #[arg(long, default_value = "15")]
        wait: u64,
    },

    /// Measure block production interval
    BlockTime {
        /// Duration in seconds
        #[arg(long, default_value = "60")]
        duration: u64,
    },

    /// Measure sequential vs parallel HTTP submission throughput
    Throughput {
        /// Total transactions to submit
        #[arg(long, default_value = "200")]
        count: usize,

        /// Transactions per RPC batch
        #[arg(long, default_value = "50")]
        batch: usize,

        /// Parallel workers
        #[arg(long, default_value = "4")]
        workers: usize,

        /// Seconds to wait for confirmations
        #[arg(long, default_value = "15")]
        wait: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    dotenvy::from_filename("../.env").ok();
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    match cli.command {
        Command::Mempool {
            count,
            batch,
            workers,
            wait,
        } => {
            let signer = cli
                .deployer_key
                .parse()
                .map_err(|e| eyre::eyre!("invalid deployer key: {e}"))?;
            mempool::run(mempool::MempoolProbeConfig {
                rpc_url: cli.rpc,
                chain_id: cli.chain_id,
                signer,
                count,
                batch_size: batch,
                workers,
                wait_secs: wait,
            })
            .await
        }

        Command::BlockTime { duration } => {
            block_time::run(block_time::BlockTimeConfig {
                rpc_url: cli.rpc,
                duration_secs: duration,
            })
            .await
        }

        Command::Throughput {
            count,
            batch,
            workers,
            wait,
        } => {
            let signer = cli
                .deployer_key
                .parse()
                .map_err(|e| eyre::eyre!("invalid deployer key: {e}"))?;
            throughput::run(throughput::ThroughputConfig {
                rpc_url: cli.rpc,
                chain_id: cli.chain_id,
                signer,
                count,
                batch_size: batch,
                workers,
                wait_secs: wait,
            })
            .await
        }
    }
}
