//! Rapidbyte CLI binary — parse arguments, dispatch subcommands.

#![warn(clippy::pedantic)]
#![recursion_limit = "256"]

mod commands;
mod logging;
pub(crate) mod output;

use std::path::PathBuf;
use std::process::ExitCode;

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

    /// Controller gRPC endpoint (enables distributed mode)
    #[arg(long, global = true, env = "RAPIDBYTE_CONTROLLER")]
    controller: Option<String>,

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
    /// Discover available streams from a source plugin
    Discover {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
    /// List available plugins
    Plugins,
    /// Scaffold a new plugin project
    Scaffold {
        /// Plugin name (e.g., "source-mysql", "dest-snowflake")
        name: String,
        /// Output directory (default: `./plugins/<kind>/<name>`)
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Launch interactive dev shell
    Dev,
    /// Start the controller server (long-running)
    Controller {
        /// gRPC listen address
        #[arg(long, default_value = "[::]:9090")]
        listen: String,
        /// Shared signing key for preview tickets (hex or raw string)
        #[arg(long, env = "RAPIDBYTE_SIGNING_KEY")]
        signing_key: Option<String>,
    },
    /// Start an agent worker (long-running)
    Agent {
        /// Controller endpoint to connect to
        #[arg(long)]
        controller: String,
        /// Flight server bind address (data plane)
        #[arg(long, default_value = "[::]:9091")]
        flight_listen: String,
        /// Flight endpoint advertised to clients (must be reachable)
        #[arg(long)]
        flight_advertise: String,
        /// Maximum concurrent tasks
        #[arg(long, default_value = "1")]
        max_tasks: u32,
        /// Shared signing key for preview tickets (must match controller)
        #[arg(long, env = "RAPIDBYTE_SIGNING_KEY")]
        signing_key: Option<String>,
    },
}

/// Resolve controller URL from config file (`~/.rapidbyte/config.yaml`).
/// CLI flag and env var are already handled by clap's `env` attribute.
fn controller_url_from_config() -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let config_path = std::path::PathBuf::from(home)
        .join(".rapidbyte")
        .join("config.yaml");
    let contents = std::fs::read_to_string(config_path).ok()?;
    let val: serde_yaml::Value = serde_yaml::from_str(&contents).ok()?;
    val.get("controller")?
        .get("url")?
        .as_str()
        .map(String::from)
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let verbosity = Verbosity::from_flags(cli.quiet, cli.verbose);

    logging::init(&cli.log_level);

    let controller_url = cli.controller.or_else(controller_url_from_config);

    let result = match cli.command {
        Commands::Run {
            pipeline,
            dry_run,
            limit,
        } => {
            commands::run::execute(
                &pipeline,
                dry_run,
                limit,
                verbosity,
                controller_url.as_deref(),
            )
            .await
        }
        Commands::Check { pipeline } => commands::check::execute(&pipeline, verbosity).await,
        Commands::Discover { pipeline } => commands::discover::execute(&pipeline, verbosity).await,
        Commands::Plugins => commands::plugins::execute(verbosity),
        Commands::Scaffold { name, output } => commands::scaffold::run(&name, output.as_deref()),
        Commands::Dev => commands::dev::execute().await,
        Commands::Controller {
            listen,
            signing_key,
        } => commands::controller::execute(&listen, signing_key.as_deref()).await,
        Commands::Agent {
            controller,
            flight_listen,
            flight_advertise,
            max_tasks,
            signing_key,
        } => {
            commands::agent::execute(
                &controller,
                &flight_listen,
                &flight_advertise,
                max_tasks,
                signing_key.as_deref(),
            )
            .await
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if verbosity != Verbosity::Quiet {
                eprintln!("{} {e:#}", console::style("\u{2718}").red().bold(),);
            }
            ExitCode::FAILURE
        }
    }
}
