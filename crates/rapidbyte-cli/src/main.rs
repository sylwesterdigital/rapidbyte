//! Rapidbyte CLI binary — parse arguments, dispatch subcommands.

#![warn(clippy::pedantic)]
#![recursion_limit = "256"]

mod commands;
mod logging;
pub(crate) mod output;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verbosity {
    Quiet,
    Default,
    Verbose,
    Diagnostic,
}

impl Verbosity {
    fn from_flags(quiet: bool, verbose: u8) -> Self {
        if quiet {
            return Self::Quiet;
        }
        match verbose {
            0 => Self::Default,
            1 => Self::Verbose,
            _ => Self::Diagnostic,
        }
    }
}

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

    /// Increase output verbosity (-v for detailed, -vv for diagnostic)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// Suppress all output (exit code only, errors on stderr)
    #[arg(short, long, global = true)]
    quiet: bool,
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

    let verbosity = Verbosity::from_flags(cli.quiet, cli.verbose);

    logging::init(&cli.log_level, verbosity);

    match cli.command {
        Commands::Run {
            pipeline,
            dry_run,
            limit,
        } => commands::run::execute(&pipeline, dry_run, limit, verbosity).await,
        Commands::Check { pipeline } => commands::check::execute(&pipeline, verbosity).await,
        Commands::Discover { pipeline } => commands::discover::execute(&pipeline, verbosity).await,
        Commands::Connectors => commands::connectors::execute(verbosity),
        Commands::Scaffold { name, output } => {
            commands::scaffold::run(&name, output.as_deref())?;
            Ok(())
        }
    }
}
