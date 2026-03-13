//! Rapidbyte CLI binary — parse arguments, dispatch subcommands.

#![warn(clippy::pedantic)]
#![recursion_limit = "256"]

mod commands;
mod logging;
pub(crate) mod output;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rustls::crypto::ring::default_provider;

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

    /// Bearer token for authenticated controller RPCs
    #[arg(long, global = true, env = "RAPIDBYTE_AUTH_TOKEN")]
    auth_token: Option<String>,

    /// Custom CA certificate for TLS controller/Flight connections
    #[arg(long, global = true, env = "RAPIDBYTE_TLS_CA_CERT")]
    tls_ca_cert: Option<PathBuf>,

    /// Optional TLS server name override for controller/Flight connections
    #[arg(long, global = true, env = "RAPIDBYTE_TLS_DOMAIN")]
    tls_domain: Option<String>,

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
    /// Show the current status of a distributed run
    Status {
        /// Controller run ID
        run_id: String,
    },
    /// Stream progress for a distributed run until it reaches a terminal state
    Watch {
        /// Controller run ID
        run_id: String,
    },
    /// List recent distributed runs
    ListRuns {
        /// Maximum number of runs to return
        #[arg(long, default_value_t = 20)]
        limit: i32,
        /// Optional state filter (pending, assigned, running, `preview_ready`, completed, failed, cancelled)
        #[arg(long)]
        state: Option<String>,
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
        /// Postgres connection string for durable controller metadata
        #[arg(long, env = "RAPIDBYTE_CONTROLLER_METADATA_DATABASE_URL")]
        metadata_database_url: Option<String>,
        /// Shared signing key for preview tickets (hex or raw string)
        #[arg(long, env = "RAPIDBYTE_SIGNING_KEY")]
        signing_key: Option<String>,
        /// Explicitly disable controller auth for local development only
        #[arg(long)]
        allow_unauthenticated: bool,
        /// Explicitly allow the built-in insecure development signing key
        #[arg(long)]
        allow_insecure_signing_key: bool,
        /// Max time to wait for restart reconciliation before failing recovery
        #[arg(long, env = "RAPIDBYTE_CONTROLLER_RECONCILIATION_TIMEOUT_SECONDS")]
        reconciliation_timeout_seconds: Option<u64>,
        /// PEM certificate for TLS server mode
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        /// PEM private key for TLS server mode
        #[arg(long)]
        tls_key: Option<PathBuf>,
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
        /// Explicitly allow the built-in insecure development signing key
        #[arg(long)]
        allow_insecure_signing_key: bool,
        /// PEM certificate for the agent Flight server
        #[arg(long)]
        flight_tls_cert: Option<PathBuf>,
        /// PEM private key for the agent Flight server
        #[arg(long)]
        flight_tls_key: Option<PathBuf>,
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

fn resolve_controller_url_with<F>(
    cli_controller: Option<String>,
    allow_config_fallback: bool,
    config_loader: F,
) -> Option<String>
where
    F: FnOnce() -> Option<String>,
{
    cli_controller.or_else(|| {
        if allow_config_fallback {
            config_loader()
        } else {
            None
        }
    })
}

fn resolve_controller_url(
    cli_controller: Option<String>,
    allow_config_fallback: bool,
) -> Option<String> {
    resolve_controller_url_with(
        cli_controller,
        allow_config_fallback,
        controller_url_from_config,
    )
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> ExitCode {
    let _ = default_provider().install_default();
    let cli = Cli::parse();

    let verbosity = Verbosity::from_flags(cli.quiet, cli.verbose);

    logging::init(verbosity, &cli.log_level);

    let tls = commands::transport::TlsClientConfig {
        ca_cert_path: cli.tls_ca_cert.clone(),
        domain_name: cli.tls_domain.clone(),
    };
    let tls = tls.is_configured().then_some(tls);

    let result = match cli.command {
        Commands::Run {
            pipeline,
            dry_run,
            limit,
        } => {
            let controller_url = resolve_controller_url(cli.controller.clone(), false);
            commands::run::execute(
                &pipeline,
                dry_run,
                limit,
                verbosity,
                controller_url.as_deref(),
                cli.auth_token.as_deref(),
                tls.as_ref(),
            )
            .await
        }
        Commands::Status { run_id } => {
            let controller_url = resolve_controller_url(cli.controller.clone(), true);
            commands::status::execute(
                controller_url.as_deref(),
                &run_id,
                verbosity,
                cli.auth_token.as_deref(),
                tls.as_ref(),
            )
            .await
        }
        Commands::Watch { run_id } => {
            let controller_url = resolve_controller_url(cli.controller.clone(), true);
            commands::watch::execute(
                controller_url.as_deref(),
                &run_id,
                verbosity,
                cli.auth_token.as_deref(),
                tls.as_ref(),
            )
            .await
        }
        Commands::ListRuns { limit, state } => {
            let controller_url = resolve_controller_url(cli.controller.clone(), true);
            commands::list_runs::execute(
                controller_url.as_deref(),
                limit,
                state.as_deref(),
                verbosity,
                cli.auth_token.as_deref(),
                tls.as_ref(),
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
            metadata_database_url,
            signing_key,
            allow_unauthenticated,
            allow_insecure_signing_key,
            reconciliation_timeout_seconds,
            tls_cert,
            tls_key,
        } => {
            commands::controller::execute(
                &listen,
                metadata_database_url.as_deref(),
                signing_key.as_deref(),
                cli.auth_token.as_deref(),
                allow_unauthenticated,
                allow_insecure_signing_key,
                reconciliation_timeout_seconds.map(std::time::Duration::from_secs),
                tls_cert.as_deref(),
                tls_key.as_deref(),
            )
            .await
        }
        Commands::Agent {
            controller,
            flight_listen,
            flight_advertise,
            max_tasks,
            signing_key,
            allow_insecure_signing_key,
            flight_tls_cert,
            flight_tls_key,
        } => {
            commands::agent::execute(
                &controller,
                &flight_listen,
                &flight_advertise,
                max_tasks,
                signing_key.as_deref(),
                allow_insecure_signing_key,
                cli.auth_token.as_deref(),
                cli.tls_ca_cert.as_deref(),
                cli.tls_domain.as_deref(),
                flight_tls_cert.as_deref(),
                flight_tls_key.as_deref(),
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

#[cfg(test)]
mod tests {
    use super::resolve_controller_url_with;

    #[test]
    fn controller_url_resolution_run_ignores_config_fallback() {
        let resolved = resolve_controller_url_with(None, false, || Some("http://cfg".into()));
        assert_eq!(resolved, None);
    }

    #[test]
    fn controller_url_resolution_control_plane_uses_config_fallback() {
        let resolved = resolve_controller_url_with(None, true, || Some("http://cfg".into()));
        assert_eq!(resolved.as_deref(), Some("http://cfg"));
    }

    #[test]
    fn controller_url_resolution_prefers_explicit_value() {
        let resolved = resolve_controller_url_with(Some("http://explicit".into()), true, || {
            Some("http://cfg".into())
        });
        assert_eq!(resolved.as_deref(), Some("http://explicit"));
    }
}
