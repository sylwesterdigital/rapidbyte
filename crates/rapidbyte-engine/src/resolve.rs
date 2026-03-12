//! Plugin resolution, manifest validation, and state backend creation.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rapidbyte_runtime::{resolve_min_limit, SandboxOverrides};
use rapidbyte_state::{SqliteStateBackend, StateBackend};
use rapidbyte_types::manifest::{Permissions, PluginManifest};
use rapidbyte_types::wire::{PluginKind, ProtocolVersion};

use crate::config::types::{parse_byte_size, PipelineConfig, StateBackendKind};
use crate::error::PipelineError;
use crate::result::CheckItemResult;

pub struct ResolvedPlugins {
    pub source_wasm: PathBuf,
    pub dest_wasm: PathBuf,
    pub source_manifest: Option<PluginManifest>,
    pub dest_manifest: Option<PluginManifest>,
    pub source_permissions: Option<Permissions>,
    pub dest_permissions: Option<Permissions>,
}

pub fn resolve_plugins(config: &PipelineConfig) -> Result<ResolvedPlugins, PipelineError> {
    let source_wasm =
        rapidbyte_runtime::resolve_plugin_path(&config.source.use_ref, PluginKind::Source)
            .map_err(PipelineError::Infrastructure)?;
    let dest_wasm = rapidbyte_runtime::resolve_plugin_path(
        &config.destination.use_ref,
        PluginKind::Destination,
    )
    .map_err(PipelineError::Infrastructure)?;

    let source_manifest =
        load_and_validate_manifest(&source_wasm, &config.source.use_ref, PluginKind::Source)
            .map_err(PipelineError::Infrastructure)?;
    let dest_manifest = load_and_validate_manifest(
        &dest_wasm,
        &config.destination.use_ref,
        PluginKind::Destination,
    )
    .map_err(PipelineError::Infrastructure)?;

    Ok(ResolvedPlugins {
        source_permissions: source_manifest.as_ref().map(|m| m.permissions.clone()),
        dest_permissions: dest_manifest.as_ref().map(|m| m.permissions.clone()),
        source_wasm,
        dest_wasm,
        source_manifest,
        dest_manifest,
    })
}

pub fn load_and_validate_manifest(
    wasm_path: &Path,
    plugin_ref: &str,
    expected_kind: PluginKind,
) -> Result<Option<PluginManifest>> {
    let manifest = rapidbyte_runtime::load_plugin_manifest(wasm_path)?;

    if let Some(ref m) = manifest {
        if !m.supports_kind(expected_kind) {
            anyhow::bail!("Plugin '{plugin_ref}' does not support {expected_kind:?} kind");
        }

        if m.protocol_version != ProtocolVersion::V5 {
            anyhow::bail!(
                "Plugin '{plugin_ref}' protocol version mismatch: manifest={:?}, host={:?}",
                m.protocol_version,
                ProtocolVersion::V5
            );
        }

        tracing::info!(plugin = m.id, version = m.version, "Loaded plugin manifest");
    } else {
        tracing::debug!(
            plugin = plugin_ref,
            "No manifest found, skipping pre-flight validation"
        );
    }

    Ok(manifest)
}

pub fn validate_config_against_schema(
    plugin_ref: &str,
    config: &serde_json::Value,
    manifest: &PluginManifest,
) -> Result<()> {
    let Some(schema_value) = &manifest.config_schema else {
        return Ok(());
    };

    let validator = jsonschema::validator_for(schema_value)
        .with_context(|| format!("Invalid JSON Schema in manifest for plugin '{plugin_ref}'"))?;

    let errors: Vec<String> = validator
        .iter_errors(config)
        .map(|e| format!("  - {e}"))
        .collect();

    if !errors.is_empty() {
        anyhow::bail!(
            "Configuration validation failed for plugin '{}':\n{}",
            plugin_ref,
            errors.join("\n"),
        );
    }

    tracing::debug!(plugin = plugin_ref, "Config schema validation passed");
    Ok(())
}

pub fn create_state_backend(config: &PipelineConfig) -> Result<Arc<dyn StateBackend>> {
    match config.state.backend {
        StateBackendKind::Sqlite => {
            let backend = if let Some(path) = &config.state.connection {
                SqliteStateBackend::open(Path::new(path)).context("Failed to open state DB")?
            } else {
                let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
                let state_path = PathBuf::from(home).join(".rapidbyte").join("state.db");
                SqliteStateBackend::open(&state_path).context("Failed to open default state DB")?
            };
            Ok(Arc::new(backend) as Arc<dyn StateBackend>)
        }
        StateBackendKind::Postgres => {
            let connstr = config
                .state
                .connection
                .as_deref()
                .unwrap_or("host=localhost dbname=rapidbyte_state");
            let backend = rapidbyte_state::PostgresStateBackend::open(connstr)
                .map_err(|e| anyhow::anyhow!("failed to open Postgres state backend: {e}"))?;
            Ok(Arc::new(backend) as Arc<dyn StateBackend>)
        }
    }
}

pub fn check_state_backend(config: &PipelineConfig) -> CheckItemResult {
    match create_state_backend(config) {
        Ok(_) => {
            tracing::info!("State backend: OK");
            CheckItemResult {
                ok: true,
                message: String::new(),
            }
        }
        Err(e) => {
            tracing::error!("State backend: FAILED — {}", e);
            CheckItemResult {
                ok: false,
                message: e.to_string(),
            }
        }
    }
}

/// Build `SandboxOverrides` from pipeline permissions/limits and manifest resource limits.
/// Returns `None` if no overrides are specified from either side.
pub fn build_sandbox_overrides(
    pipeline_perms: Option<&crate::config::types::PipelinePermissions>,
    pipeline_limits: Option<&crate::config::types::PipelineLimits>,
    manifest_limits: &rapidbyte_types::manifest::ResourceLimits,
) -> Option<SandboxOverrides> {
    let manifest_mem = manifest_limits
        .max_memory
        .as_ref()
        .and_then(|s| parse_byte_size(s).ok());
    let pipeline_mem = pipeline_limits
        .and_then(|l| l.max_memory.as_ref())
        .and_then(|s| parse_byte_size(s).ok());

    let manifest_timeout = manifest_limits.timeout_seconds;
    let pipeline_timeout = pipeline_limits.and_then(|l| l.timeout_seconds);

    let has_overrides = pipeline_perms.is_some()
        || pipeline_limits.is_some()
        || manifest_limits.max_memory.is_some()
        || manifest_limits.timeout_seconds.is_some();

    if has_overrides {
        Some(SandboxOverrides {
            allowed_hosts: pipeline_perms.and_then(|p| p.network.allowed_hosts.clone()),
            allowed_vars: pipeline_perms.and_then(|p| p.env.allowed_vars.clone()),
            allowed_preopens: pipeline_perms.and_then(|p| p.fs.allowed_preopens.clone()),
            max_memory_bytes: resolve_min_limit(manifest_mem, pipeline_mem),
            timeout_seconds: resolve_min_limit(manifest_timeout, pipeline_timeout),
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::state::{PipelineId, StreamName};
    use tempfile::tempdir;

    #[test]
    fn test_create_state_backend_custom_path() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.db");
        let config = PipelineConfig {
            version: "1.0".to_string(),
            pipeline: "test".to_string(),
            source: crate::config::types::SourceConfig {
                use_ref: "source".to_string(),
                config: serde_json::json!({}),
                streams: vec![],
                permissions: None,
                limits: None,
            },
            transforms: vec![],
            destination: crate::config::types::DestinationConfig {
                use_ref: "dest".to_string(),
                config: serde_json::json!({}),
                write_mode: crate::config::types::PipelineWriteMode::Append,
                primary_key: vec![],
                on_data_error: rapidbyte_types::stream::DataErrorPolicy::Fail,
                schema_evolution: None,
                permissions: None,
                limits: None,
            },
            state: crate::config::types::StateConfig {
                backend: StateBackendKind::Sqlite,
                connection: Some(db_path.to_string_lossy().to_string()),
            },
            resources: crate::config::types::ResourceConfig::default(),
        };

        let backend = create_state_backend(&config).unwrap();
        let run_id = backend
            .start_run(&PipelineId::new("test"), &StreamName::new("all"))
            .unwrap();
        assert!(run_id > 0);
    }
}
