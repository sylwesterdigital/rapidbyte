//! CLI subcommand implementations.

use std::path::Path;

use anyhow::{Context, Result};

use rapidbyte_engine::config::{parser, validator};
use rapidbyte_engine::config::types::PipelineConfig;

pub mod check;
pub mod connectors;
pub mod discover;
pub mod run;
pub mod scaffold;

/// Parse and validate a pipeline YAML file.
///
/// # Errors
///
/// Returns `Err` if parsing or validation fails.
pub fn load_pipeline(path: &Path) -> Result<PipelineConfig> {
    let config = parser::parse_pipeline(path)
        .with_context(|| format!("Failed to parse pipeline: {}", path.display()))?;
    validator::validate_pipeline(&config)?;
    Ok(config)
}
