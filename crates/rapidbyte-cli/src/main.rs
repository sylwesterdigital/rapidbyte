//! Rapidbyte CLI binary — parse arguments, dispatch subcommands.

#![warn(clippy::pedantic)]

mod commands;
mod logging;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "rapidbyte",
    version,
    about = "The single-binary data ingestion engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info", global = true)]
    log_level: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a data pipeline
    Run {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
        /// Preview mode: skip destination, print output to stdout
        #[arg(long)]
        dry_run: bool,
        /// Maximum rows to read per stream (implies --dry-run)
        #[arg(long)]
        limit: Option<u64>,
    },
    /// Validate pipeline configuration and connectivity
    Check {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
    /// Discover available streams from a source connector
    Discover {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
    /// List available connectors
    Connectors,
    /// Scaffold a new connector project
    Scaffold {
        /// Connector name (e.g., "source-mysql", "dest-snowflake")
        name: String,
        /// Output directory (default: `./connectors/<name>`)
        #[arg(short, long)]
        output: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    logging::init(&cli.log_level);

    match cli.command {
        Commands::Run {
            pipeline,
            dry_run,
            limit,
        } => commands::run::execute(&pipeline, dry_run, limit).await,
        Commands::Check { pipeline } => commands::check::execute(&pipeline).await,
        Commands::Discover { pipeline } => commands::discover::execute(&pipeline).await,
        Commands::Connectors => commands::connectors::execute(),
        Commands::Scaffold { name, output } => {
            commands::scaffold::run(&name, output.as_deref())?;
            Ok(())
        }
    }
}
