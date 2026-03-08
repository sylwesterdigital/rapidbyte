//! CLI subcommand implementations.

use std::path::Path;

use anyhow::{Context, Result};

use rapidbyte_engine::config::types::PipelineConfig;
use rapidbyte_engine::config::{parser, validator};

pub mod check;
pub mod dev;
pub mod discover;
pub mod plugins;
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
