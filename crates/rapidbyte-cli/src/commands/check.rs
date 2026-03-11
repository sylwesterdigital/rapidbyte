//! Pipeline validation subcommand (check).

use std::path::Path;

use anyhow::Result;
use console::style;
use rapidbyte_engine::CheckItemResult;
use rapidbyte_types::error::{ValidationResult, ValidationStatus};

use rapidbyte_engine::orchestrator;

use crate::Verbosity;

/// Execute the `check` command: validate pipeline config and plugin connectivity.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, validation, or connectivity check fails.
pub async fn execute(pipeline_path: &Path, verbosity: Verbosity) -> Result<()> {
    let config = super::load_pipeline(pipeline_path)?;

    let result = orchestrator::check_pipeline(&config).await?;

    if verbosity != Verbosity::Quiet {
        if let Some(outcome) = &result.source_manifest {
            print_check_item("source manifest", outcome);
        }
        if let Some(outcome) = &result.destination_manifest {
            print_check_item("dest manifest", outcome);
        }
        if let Some(outcome) = &result.source_config {
            print_check_item("source config", outcome);
        }
        if let Some(outcome) = &result.destination_config {
            print_check_item("dest config", outcome);
        }
        for (i, outcome) in result.transform_configs.iter().enumerate() {
            let label = config.transforms.get(i).map_or_else(
                || format!("transform config[{i}]"),
                |t| format!("transform config ({})", t.use_ref),
            );
            print_check_item(&label, outcome);
        }
        print_validation(&config.source.use_ref, &result.source_validation);
        print_validation(&config.destination.use_ref, &result.destination_validation);

        for (i, tv) in result.transform_validations.iter().enumerate() {
            let label = config
                .transforms
                .get(i)
                .map_or_else(|| format!("transform[{i}]"), |t| t.use_ref.clone());
            print_validation(&label, tv);
        }

        print_check_item("state", &result.state);
    }

    // Return error if anything failed
    let source_ok = check_item_passes(result.source_manifest.as_ref())
        && check_item_passes(result.source_config.as_ref())
        && validation_passes(&result.source_validation);
    let dest_ok = check_item_passes(result.destination_manifest.as_ref())
        && check_item_passes(result.destination_config.as_ref())
        && validation_passes(&result.destination_validation);
    let transform_configs_ok = result.transform_configs.iter().all(|item| item.ok);
    let transforms_ok = result.transform_validations.iter().all(validation_passes);

    if source_ok && dest_ok && transform_configs_ok && transforms_ok && result.state.ok {
        Ok(())
    } else {
        Err(anyhow::anyhow!("pipeline validation failed"))
    }
}

fn print_check_item(label: &str, result: &CheckItemResult) {
    if result.ok {
        eprintln!("{} {:<20} valid", style("\u{2713}").green().bold(), label);
    } else {
        eprintln!("{} {:<20} failed", style("\u{2717}").red().bold(), label);
        if !result.message.is_empty() {
            eprintln!("  {}", result.message);
        }
    }
}

fn print_validation(label: &str, result: &ValidationResult) {
    match result.status {
        ValidationStatus::Success => {
            eprintln!("{} {:<20} valid", style("\u{2713}").green().bold(), label);
        }
        ValidationStatus::Failed => {
            eprintln!("{} {:<20} failed", style("\u{2717}").red().bold(), label);
            print_validation_details(result);
        }
        ValidationStatus::Warning => {
            eprintln!("{} {:<20} warning", style("!").yellow().bold(), label);
            print_validation_details(result);
        }
    }
}

fn validation_passes(result: &ValidationResult) -> bool {
    matches!(
        result.status,
        ValidationStatus::Success | ValidationStatus::Warning
    )
}

fn check_item_passes(result: Option<&CheckItemResult>) -> bool {
    result.is_none_or(|item| item.ok)
}

fn print_validation_details(result: &ValidationResult) {
    if !result.message.is_empty() {
        eprintln!("  {}", result.message);
    }
    for warning in &result.warnings {
        eprintln!("  warning: {warning}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warnings_count_as_non_fatal_validation() {
        let result = ValidationResult {
            status: ValidationStatus::Warning,
            message: "Validation not implemented".to_string(),
            warnings: vec!["live connectivity check missing".to_string()],
        };

        assert!(validation_passes(&result));
    }

    #[test]
    fn failures_remain_fatal_validation() {
        let result = ValidationResult {
            status: ValidationStatus::Failed,
            message: "connection refused".to_string(),
            warnings: vec!["retry disabled".to_string()],
        };

        assert!(!validation_passes(&result));
    }

    #[test]
    fn failed_check_item_is_fatal() {
        let result = CheckItemResult {
            ok: false,
            message: "invalid schema".to_string(),
        };

        assert!(!check_item_passes(Some(&result)));
    }
}
