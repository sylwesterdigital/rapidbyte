//! Interactive dev shell subcommand.

use anyhow::Result;

/// Launch the interactive dev shell.
///
/// # Errors
///
/// Returns `Err` if the REPL encounters a fatal error.
pub async fn execute() -> Result<()> {
    rapidbyte_dev::run().await
}
