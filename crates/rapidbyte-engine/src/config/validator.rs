//! Semantic validation for parsed pipeline configuration values.

use anyhow::{bail, Result};
use rapidbyte_types::wire::SyncMode;

use crate::config::types::{
    parse_byte_size, PipelineConfig, PipelineLimits, PipelineParallelism, PipelinePermissions,
    PipelineWriteMode, MAX_COPY_FLUSH_BYTES, MIN_COPY_FLUSH_BYTES,
};

/// Validate host patterns in `allowed_hosts` lists.
/// Only `*.domain` single-level wildcards are supported.
fn validate_host_patterns(hosts: &[String], context: &str, errors: &mut Vec<String>) {
    for host in hosts {
        let trimmed = host.trim();
        if trimmed.is_empty() {
            errors.push(format!("{context}: empty host pattern"));
            continue;
        }
        if trimmed.contains('*') && !trimmed.starts_with("*.") {
            errors.push(format!(
                "{context}: Invalid host pattern '{host}' \u{2014} only '*.domain' wildcards supported"
            ));
        }
        if trimmed.starts_with("*.") && trimmed[2..].contains('*') {
            errors.push(format!(
                "{context}: Invalid host pattern '{host}' \u{2014} nested wildcards not supported"
            ));
        }
    }
}

/// Validate plugin-level permission and limit overrides.
fn validate_plugin_overrides(
    permissions: Option<&PipelinePermissions>,
    limits: Option<&PipelineLimits>,
    context: &str,
    errors: &mut Vec<String>,
) {
    if let Some(perms) = permissions {
        if let Some(ref hosts) = perms.network.allowed_hosts {
            validate_host_patterns(hosts, context, errors);
        }
    }
    if let Some(lim) = limits {
        if let Some(ref mem) = lim.max_memory {
            if parse_byte_size(mem).is_err() {
                errors.push(format!("{context}: invalid max_memory '{mem}'"));
            }
        }
        if let Some(timeout) = lim.timeout_seconds {
            if timeout == 0 {
                errors.push(format!("{context}: timeout_seconds must be > 0"));
            }
        }
    }
}

/// Validate a parsed pipeline configuration.
/// Returns `Ok(())` if valid, Err with all validation errors if not.
///
/// # Errors
///
/// Returns an error listing all validation failures found in the pipeline config.
#[allow(clippy::too_many_lines)]
pub fn validate_pipeline(config: &PipelineConfig) -> Result<()> {
    let mut errors = Vec::new();

    if config.version != "1.0" {
        errors.push(format!(
            "Unsupported pipeline version '{}', expected '1.0'",
            config.version
        ));
    }

    if config.pipeline.trim().is_empty() {
        errors.push("Pipeline name must not be empty".to_string());
    }

    if config.source.use_ref.trim().is_empty() {
        errors.push("Source plugin reference (use) must not be empty".to_string());
    }

    if config.source.streams.is_empty() {
        errors.push("Source must define at least one stream".to_string());
    }

    for (i, stream) in config.source.streams.iter().enumerate() {
        if stream.name.trim().is_empty() {
            errors.push(format!("Stream {i} has an empty name"));
        }
        if stream.sync_mode == SyncMode::Incremental && stream.cursor_field.is_none() {
            errors.push(format!(
                "Stream '{}' uses incremental sync but has no cursor_field",
                stream.name
            ));
        }
        if stream
            .tie_breaker_field
            .as_deref()
            .is_some_and(str::is_empty)
        {
            errors.push(format!(
                "Stream '{}' has an empty tie_breaker_field",
                stream.name
            ));
        }
        if stream.tie_breaker_field.is_some() && stream.sync_mode != SyncMode::Incremental {
            errors.push(format!(
                "Stream '{}' sets tie_breaker_field but is not incremental",
                stream.name
            ));
        }
        if stream.partition_key.as_deref().is_some_and(str::is_empty) {
            errors.push(format!(
                "Stream '{}' has an empty partition_key",
                stream.name
            ));
        }
        if stream.partition_key.is_some() && stream.sync_mode != SyncMode::FullRefresh {
            errors.push(format!(
                "Stream '{}' sets partition_key but is not full_refresh",
                stream.name
            ));
        }
        if let (Some(cursor_field), Some(tie_breaker_field)) = (
            stream.cursor_field.as_deref(),
            stream.tie_breaker_field.as_deref(),
        ) {
            if cursor_field == tie_breaker_field {
                errors.push(format!(
                    "Stream '{}' tie_breaker_field must differ from cursor_field",
                    stream.name
                ));
            }
            if let Some(columns) = stream.columns.as_ref() {
                if !columns.iter().any(|c| c == cursor_field) {
                    errors.push(format!(
                        "Stream '{}' columns must include cursor_field '{}'",
                        stream.name, cursor_field
                    ));
                }
                if !columns.iter().any(|c| c == tie_breaker_field) {
                    errors.push(format!(
                        "Stream '{}' columns must include tie_breaker_field '{}'",
                        stream.name, tie_breaker_field
                    ));
                }
            }
        }
    }

    if config.destination.use_ref.trim().is_empty() {
        errors.push("Destination plugin reference (use) must not be empty".to_string());
    }

    if config.destination.write_mode == PipelineWriteMode::Upsert
        && config.destination.primary_key.is_empty()
    {
        errors.push(
            "Destination write_mode 'upsert' requires at least one primary_key field".to_string(),
        );
    }

    if config.resources.max_inflight_batches == 0 {
        errors.push("max_inflight_batches must be at least 1".to_string());
    }

    if matches!(config.resources.parallelism, PipelineParallelism::Manual(0)) {
        errors.push("parallelism must be at least 1".to_string());
    }

    if matches!(config.resources.autotune.pin_parallelism, Some(0)) {
        errors.push("autotune.pin_parallelism must be at least 1".to_string());
    }

    if let Some(copy_flush_bytes) = config.resources.autotune.pin_copy_flush_bytes {
        if !(MIN_COPY_FLUSH_BYTES..=MAX_COPY_FLUSH_BYTES).contains(&copy_flush_bytes) {
            errors.push(format!(
                "autotune.pin_copy_flush_bytes must be between {MIN_COPY_FLUSH_BYTES} and {MAX_COPY_FLUSH_BYTES}"
            ));
        }
    }

    // Validate plugin-level permission/limit overrides
    validate_plugin_overrides(
        config.source.permissions.as_ref(),
        config.source.limits.as_ref(),
        "source",
        &mut errors,
    );
    validate_plugin_overrides(
        config.destination.permissions.as_ref(),
        config.destination.limits.as_ref(),
        "destination",
        &mut errors,
    );
    for (i, transform) in config.transforms.iter().enumerate() {
        validate_plugin_overrides(
            transform.permissions.as_ref(),
            transform.limits.as_ref(),
            &format!("transforms[{i}]"),
            &mut errors,
        );
    }

    if errors.is_empty() {
        Ok(())
    } else {
        bail!("Pipeline validation failed:\n  - {}", errors.join("\n  - "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parser::parse_pipeline_str;

    fn valid_yaml() -> &'static str {
        r#"
version: "1.0"
pipeline: test_pipeline
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
"#
    }

    #[test]
    fn test_valid_pipeline_passes() {
        let config = parse_pipeline_str(valid_yaml()).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_wrong_version_fails() {
        let yaml = valid_yaml().replace("\"1.0\"", "\"2.0\"");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("Unsupported pipeline version"));
    }

    #[test]
    fn test_empty_pipeline_name_fails() {
        let yaml = valid_yaml().replace("test_pipeline", "");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("Pipeline name must not be empty"));
    }

    #[test]
    fn test_incremental_without_cursor_fails() {
        let yaml = valid_yaml().replace("full_refresh", "incremental");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("no cursor_field"));
    }

    #[test]
    fn test_upsert_without_primary_key_fails() {
        let yaml = valid_yaml().replace("append", "upsert");
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("requires at least one primary_key"));
    }

    #[test]
    fn test_max_inflight_batches_zero_fails() {
        let yaml = r#"
version: "1.0"
pipeline: test_pipeline
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
resources:
  max_inflight_batches: 0
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("max_inflight_batches"));
    }

    #[test]
    fn test_parallelism_zero_fails() {
        let yaml = r#"
version: "1.0"
pipeline: test_pipeline
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
resources:
  parallelism: 0
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("parallelism"));
    }

    #[test]
    fn test_parallelism_auto_passes() {
        let yaml = format!(
            "{}\nresources:\n  parallelism: auto\n",
            valid_yaml().trim_end()
        );
        let config = parse_pipeline_str(&yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_parallelism_manual_one_passes() {
        let yaml = format!(
            "{}\nresources:\n  parallelism: 1\n",
            valid_yaml().trim_end()
        );
        let config = parse_pipeline_str(&yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_autotune_pin_parallelism_zero_fails() {
        let yaml = format!(
            "{}\nresources:\n  autotune:\n    pin_parallelism: 0\n",
            valid_yaml().trim_end()
        );
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("autotune.pin_parallelism"));
    }

    #[test]
    fn test_autotune_pin_copy_flush_bytes_below_min_fails() {
        let yaml = format!(
            "{}\nresources:\n  autotune:\n    pin_copy_flush_bytes: 1024\n",
            valid_yaml().trim_end()
        );
        let config = parse_pipeline_str(&yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("autotune.pin_copy_flush_bytes"));
    }

    #[test]
    fn test_autotune_pin_copy_flush_bytes_in_range_passes() {
        let yaml = format!(
            "{}\nresources:\n  autotune:\n    pin_copy_flush_bytes: 8388608\n",
            valid_yaml().trim_end()
        );
        let config = parse_pipeline_str(&yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_upsert_with_primary_key_passes() {
        let yaml = r#"
version: "1.0"
pipeline: test_upsert
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config:
    host: localhost
  write_mode: upsert
  primary_key: [id]
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_incremental_with_cursor_passes() {
        let yaml = r#"
version: "1.0"
pipeline: test_incr
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: id
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_tie_breaker_requires_incremental_sync() {
        let yaml = r#"
version: "1.0"
pipeline: test_tie_breaker_full_refresh
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
      tie_breaker_field: id
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("sets tie_breaker_field but is not incremental"));
    }

    #[test]
    fn test_tie_breaker_must_differ_from_cursor_field() {
        let yaml = r#"
version: "1.0"
pipeline: test_tie_breaker_same_field
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
      tie_breaker_field: updated_at
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("tie_breaker_field must differ from cursor_field"));
    }

    #[test]
    fn test_columns_must_include_tie_breaker_field() {
        let yaml = r#"
version: "1.0"
pipeline: test_tie_breaker_projection
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
      tie_breaker_field: id
      columns: [updated_at, email]
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("columns must include tie_breaker_field 'id'"));
    }

    #[test]
    fn test_partition_key_requires_full_refresh() {
        let yaml = r#"
version: "1.0"
pipeline: test_partition_key_incremental
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
      partition_key: tenant_id
destination:
  use: postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("sets partition_key but is not full_refresh"));
    }

    #[test]
    fn test_replace_write_mode_passes() {
        let yaml = r#"
version: "1.0"
pipeline: test_replace
source:
  use: postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config:
    host: localhost
  write_mode: replace
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_valid_pipeline_permissions_passes() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
  permissions:
    network:
      allowed_hosts: [db.example.com, "*.internal.corp", "127.0.0.1"]
  limits:
    max_memory: 128mb
    timeout_seconds: 60
destination:
  use: postgres
  config: {}
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        assert!(validate_pipeline(&config).is_ok());
    }

    #[test]
    fn test_nested_wildcard_host_fails() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
  permissions:
    network:
      allowed_hosts: ["*.*.example.com"]
destination:
  use: postgres
  config: {}
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("Invalid host pattern"));
    }

    #[test]
    fn test_invalid_max_memory_fails() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
  limits:
    max_memory: not-a-size
destination:
  use: postgres
  config: {}
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("max_memory"));
    }

    #[test]
    fn test_zero_timeout_fails() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
  limits:
    timeout_seconds: 0
destination:
  use: postgres
  config: {}
  write_mode: append
"#;
        let config = parse_pipeline_str(yaml).unwrap();
        let err = validate_pipeline(&config).unwrap_err().to_string();
        assert!(err.contains("timeout_seconds"));
    }
}
