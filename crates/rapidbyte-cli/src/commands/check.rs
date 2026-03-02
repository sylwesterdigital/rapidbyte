//! Pipeline validation subcommand (check).

use std::path::Path;

use anyhow::Result;
use rapidbyte_types::error::ValidationStatus;

use rapidbyte_engine::orchestrator;

/// Execute the `check` command: validate pipeline config and connector connectivity.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, validation, or connectivity check fails.
pub async fn execute(pipeline_path: &Path) -> Result<()> {
    let config = super::load_pipeline(pipeline_path)?;
    println!("Pipeline structure: OK");

    // 3. Check connectors and state
    let result = orchestrator::check_pipeline(&config).await?;

    // 4. Report results
    print_validation("Source", &result.source_validation);
    print_validation("Destination", &result.destination_validation);

    for (i, tv) in result.transform_validations.iter().enumerate() {
        let label = format!("Transform[{i}]");
        print_validation(&label, tv);
    }

    if result.state_ok {
        println!("State backend:     OK");
    } else {
        println!("State backend:     FAILED");
    }

    // Return error if anything failed
    let source_ok = result.source_validation.status == ValidationStatus::Success;
    let dest_ok = result.destination_validation.status == ValidationStatus::Success;
    let transforms_ok = result
        .transform_validations
        .iter()
        .all(|v| v.status == ValidationStatus::Success);

    if source_ok && dest_ok && transforms_ok && result.state_ok {
        println!("\nAll checks passed.");
        Ok(())
    } else {
        anyhow::bail!("One or more checks failed")
    }
}

fn print_validation(label: &str, result: &rapidbyte_types::error::ValidationResult) {
    let status = match result.status {
        ValidationStatus::Success => "OK",
        ValidationStatus::Failed => "FAILED",
        ValidationStatus::Warning => "WARNING",
    };
    println!("{:18} {}", format!("{}:", label), status);
    if !result.message.is_empty() {
        println!("  {}", result.message);
    }
}
