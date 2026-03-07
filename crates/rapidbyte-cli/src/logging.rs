//! Structured logging initialization via tracing-subscriber.

use tracing_subscriber::EnvFilter;

/// Initialize structured logging with tracing-subscriber.
///
/// Adjusts the effective log level based on verbosity so that info-level
/// tracing lines do not visually corrupt the progress spinner in normal mode.
/// When `RUST_LOG` is explicitly set, it is always respected as a user override.
pub fn init(log_level: &str, verbosity: crate::Verbosity) {
    // If RUST_LOG is explicitly set, always use it (user override)
    if std::env::var("RUST_LOG").is_ok() {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .with_writer(std::io::stderr)
            .init();
        return;
    }

    // Otherwise, adjust based on verbosity
    let effective_level = match verbosity {
        crate::Verbosity::Quiet => "error",
        crate::Verbosity::Default => "warn",
        crate::Verbosity::Verbose => "warn",
        crate::Verbosity::Diagnostic => log_level,
    };

    let env_filter = EnvFilter::new(effective_level);
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}
