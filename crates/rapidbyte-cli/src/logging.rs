//! Structured logging initialization via tracing-subscriber.

use tracing_subscriber::EnvFilter;

/// Initialize structured logging with tracing-subscriber.
///
/// Tracing is suppressed by default — the progress spinner and summary
/// formatter handle all user-facing output. Set `RUST_LOG` to enable
/// raw tracing output (e.g., `RUST_LOG=info` or `RUST_LOG=debug`).
pub fn init(log_level: &str) {
    let env_filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level))
    } else {
        EnvFilter::new("off")
    };

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}
