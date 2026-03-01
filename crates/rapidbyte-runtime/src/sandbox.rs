//! WASI sandbox configuration: overrides, context building, and store limits.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use rapidbyte_types::manifest::Permissions;
use wasmtime::{StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder};

/// Pipeline-level sandbox overrides passed alongside manifest permissions.
#[derive(Debug, Clone, Default)]
pub struct SandboxOverrides {
    pub allowed_hosts: Option<Vec<String>>,
    pub allowed_vars: Option<Vec<String>>,
    pub allowed_preopens: Option<Vec<String>>,
    pub max_memory_bytes: Option<u64>,
    pub timeout_seconds: Option<u64>,
}

/// Resolve a limit by taking the minimum of manifest and pipeline values.
/// When only one side provides a value, that value is used.
/// When neither provides a value, returns `None`.
#[must_use]
pub fn resolve_min_limit(manifest: Option<u64>, pipeline: Option<u64>) -> Option<u64> {
    [manifest, pipeline].into_iter().flatten().min()
}

fn intersect_string_lists(manifest: &[String], pipeline: Option<&[String]>) -> Vec<String> {
    match pipeline {
        None => manifest.to_vec(),
        Some(allowed) => {
            let allowed_set: HashSet<&str> = allowed.iter().map(String::as_str).collect();
            manifest
                .iter()
                .filter(|item| allowed_set.contains(item.as_str()))
                .cloned()
                .collect()
        }
    }
}

/// Intersect manifest-declared env vars with pipeline-allowed vars.
#[must_use]
pub fn intersect_env_vars(
    manifest_vars: &[String],
    pipeline_vars: Option<&[String]>,
) -> Vec<String> {
    intersect_string_lists(manifest_vars, pipeline_vars)
}

/// Intersect manifest-declared preopens with pipeline-allowed preopens.
#[must_use]
pub fn intersect_preopens(
    manifest_preopens: &[String],
    pipeline_preopens: Option<&[String]>,
) -> Vec<String> {
    intersect_string_lists(manifest_preopens, pipeline_preopens)
}

/// Build a WASI context with network disabled; connectors use host `connect-tcp`.
///
/// # Errors
///
/// Returns an error if a declared preopen directory cannot be opened.
pub fn build_wasi_ctx(
    permissions: Option<&Permissions>,
    overrides: Option<&SandboxOverrides>,
) -> Result<WasiCtx> {
    let mut builder = WasiCtxBuilder::new();
    builder.allow_blocking_current_thread(true);

    // WASI-level network is disabled; connectors must use host `connect-tcp`.
    builder.allow_tcp(false);
    builder.allow_udp(false);
    builder.allow_ip_name_lookup(false);

    if let Some(perms) = permissions {
        let effective_vars = intersect_env_vars(
            &perms.env.allowed_vars,
            overrides.and_then(|o| o.allowed_vars.as_deref()),
        );
        for var in &effective_vars {
            if let Ok(value) = std::env::var(var) {
                builder.env(var, &value);
            }
        }

        let effective_preopens = intersect_preopens(
            &perms.fs.preopens,
            overrides.and_then(|o| o.allowed_preopens.as_deref()),
        );
        for dir in &effective_preopens {
            let path = Path::new(dir);
            if !path.exists() {
                tracing::warn!(
                    path = dir,
                    "Declared preopen path does not exist on host, skipping"
                );
                continue;
            }

            builder
                .preopened_dir(path, dir, DirPerms::all(), FilePerms::all())
                .with_context(|| format!("failed to preopen directory '{dir}'"))?;
        }
    }

    Ok(builder.build())
}

/// Build Wasmtime store limits from sandbox overrides.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn build_store_limits(overrides: Option<&SandboxOverrides>) -> StoreLimits {
    let mut builder = StoreLimitsBuilder::new();
    if let Some(max_bytes) = overrides.and_then(|o| o.max_memory_bytes) {
        builder = builder.memory_size(max_bytes as usize);
    }
    builder = builder.trap_on_grow_failure(true);
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersect_env_vars_filters() {
        let manifest = vec!["A".into(), "B".into(), "C".into()];
        let pipeline = vec!["A".into(), "C".into()];
        assert_eq!(
            intersect_env_vars(&manifest, Some(&pipeline)),
            vec!["A".to_string(), "C".to_string()]
        );
    }

    #[test]
    fn intersect_env_vars_none_preserves_all() {
        let manifest = vec!["A".into(), "B".into()];
        assert_eq!(
            intersect_env_vars(&manifest, None),
            vec!["A".to_string(), "B".to_string()]
        );
    }

    #[test]
    fn intersect_env_vars_empty_blocks_all() {
        let manifest = vec!["A".into(), "B".into()];
        assert!(intersect_env_vars(&manifest, Some(&[])).is_empty());
    }

    #[test]
    fn intersect_env_vars_pipeline_cannot_widen() {
        let manifest = vec!["A".into()];
        let pipeline = vec!["A".into(), "SECRET".into()];
        assert_eq!(
            intersect_env_vars(&manifest, Some(&pipeline)),
            vec!["A".to_string()]
        );
    }

    #[test]
    fn intersect_preopens_filters() {
        let manifest = vec!["/data".into(), "/tmp".into()];
        let pipeline = vec!["/data".into()];
        assert_eq!(
            intersect_preopens(&manifest, Some(&pipeline)),
            vec!["/data".to_string()]
        );
    }

    #[test]
    fn intersect_preopens_none_preserves_all() {
        let manifest = vec!["/data".into(), "/tmp".into()];
        assert_eq!(
            intersect_preopens(&manifest, None),
            vec!["/data".to_string(), "/tmp".to_string()]
        );
    }

    #[test]
    fn intersect_preopens_empty_blocks_all() {
        let manifest = vec!["/data".into()];
        assert!(intersect_preopens(&manifest, Some(&[])).is_empty());
    }

    #[test]
    fn resolve_limit_min_of_both() {
        assert_eq!(resolve_min_limit(Some(100), Some(50)), Some(50));
        assert_eq!(resolve_min_limit(Some(50), Some(100)), Some(50));
    }

    #[test]
    fn resolve_limit_one_side_only() {
        assert_eq!(resolve_min_limit(Some(100), None), Some(100));
        assert_eq!(resolve_min_limit(None, Some(50)), Some(50));
    }

    #[test]
    fn resolve_limit_neither() {
        assert_eq!(resolve_min_limit(None, None), None);
    }
}
