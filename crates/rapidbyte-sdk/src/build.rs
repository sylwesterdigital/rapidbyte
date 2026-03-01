//! Build-time helpers for connector authors.
//!
//! Used in connector `build.rs` to declare and emit manifest metadata.
//! Requires the `build` feature.

use crate::manifest::*;
use crate::wire::{Feature, ProtocolVersion, SyncMode, WriteMode};

/// Fluent builder for declaring and emitting a connector manifest at build time.
///
/// # Example (source connector `build.rs`)
///
/// ```ignore
/// use rapidbyte_sdk::build::ManifestBuilder;
/// use rapidbyte_sdk::wire::SyncMode;
///
/// fn main() {
///     ManifestBuilder::source("rapidbyte/source-postgres")
///         .name("PostgreSQL Source")
///         .sync_modes(&[SyncMode::FullRefresh, SyncMode::Incremental])
///         .allow_runtime_network()
///         .emit();
/// }
/// ```
pub struct ManifestBuilder {
    manifest: ConnectorManifest,
    rerun_files: Vec<String>,
}

impl ManifestBuilder {
    fn with_roles(id: impl Into<String>, roles: Roles) -> Self {
        Self {
            manifest: ConnectorManifest {
                id: id.into(),
                name: String::new(),
                version: String::new(),
                description: String::new(),
                author: None,
                license: None,
                protocol_version: ProtocolVersion::V4,
                permissions: Permissions::default(),
                limits: ResourceLimits::default(),
                roles,
                config_schema: None,
            },
            rerun_files: vec!["build.rs".to_string()],
        }
    }

    /// Start building a source connector manifest.
    pub fn source(id: impl Into<String>) -> Self {
        Self::with_roles(
            id,
            Roles {
                source: Some(SourceCapabilities {
                    supported_sync_modes: vec![],
                    features: vec![],
                }),
                ..Default::default()
            },
        )
    }

    /// Start building a destination connector manifest.
    pub fn destination(id: impl Into<String>) -> Self {
        Self::with_roles(
            id,
            Roles {
                destination: Some(DestinationCapabilities {
                    supported_write_modes: vec![],
                    features: vec![],
                }),
                ..Default::default()
            },
        )
    }

    /// Start building a transform connector manifest.
    pub fn transform(id: impl Into<String>) -> Self {
        Self::with_roles(
            id,
            Roles {
                transform: Some(TransformCapabilities {}),
                ..Default::default()
            },
        )
    }

    // -- Metadata ----------------------------------------------------

    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.manifest.name = n.into();
        self
    }

    pub fn version(mut self, v: impl Into<String>) -> Self {
        self.manifest.version = v.into();
        self
    }

    pub fn description(mut self, d: impl Into<String>) -> Self {
        self.manifest.description = d.into();
        self
    }

    pub fn author(mut self, a: impl Into<String>) -> Self {
        self.manifest.author = Some(a.into());
        self
    }

    pub fn license(mut self, l: impl Into<String>) -> Self {
        self.manifest.license = Some(l.into());
        self
    }

    // -- Capabilities ------------------------------------------------

    /// Set supported sync modes (source connectors).
    pub fn sync_modes(mut self, modes: &[SyncMode]) -> Self {
        if let Some(ref mut src) = self.manifest.roles.source {
            src.supported_sync_modes = modes.to_vec();
        }
        self
    }

    /// Set supported write modes (destination connectors).
    pub fn write_modes(mut self, modes: &[WriteMode]) -> Self {
        if let Some(ref mut dst) = self.manifest.roles.destination {
            dst.supported_write_modes = modes.to_vec();
        }
        self
    }

    /// Set source features (e.g., `Feature::Cdc`).
    pub fn source_features(mut self, features: Vec<Feature>) -> Self {
        if let Some(ref mut src) = self.manifest.roles.source {
            src.features = features;
        }
        self
    }

    /// Set destination features (e.g., `Feature::BulkLoadCopy`).
    pub fn dest_features(mut self, features: Vec<Feature>) -> Self {
        if let Some(ref mut dst) = self.manifest.roles.destination {
            dst.features = features;
        }
        self
    }

    // -- Permissions -------------------------------------------------

    /// Allow network access to hosts from runtime pipeline config.
    pub fn allow_runtime_network(mut self) -> Self {
        self.manifest
            .permissions
            .network
            .allow_runtime_config_domains = true;
        self
    }

    /// Set statically allowed network domains.
    pub fn allowed_domains(mut self, domains: &[&str]) -> Self {
        self.manifest.permissions.network.allowed_domains =
            Some(domains.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Set allowed environment variables.
    pub fn env_vars(mut self, vars: &[&str]) -> Self {
        self.manifest.permissions.env.allowed_vars = vars.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Add filesystem preopens.
    pub fn fs_preopens(mut self, paths: &[&str]) -> Self {
        self.manifest.permissions.fs.preopens = paths.iter().map(|s| s.to_string()).collect();
        self
    }

    // -- Limits ------------------------------------------------------

    pub fn max_memory(mut self, m: impl Into<String>) -> Self {
        self.manifest.limits.max_memory = Some(m.into());
        self
    }

    pub fn min_memory(mut self, m: impl Into<String>) -> Self {
        self.manifest.limits.min_memory = Some(m.into());
        self
    }

    pub fn timeout_seconds(mut self, t: u64) -> Self {
        self.manifest.limits.timeout_seconds = Some(t);
        self
    }

    // -- Emit --------------------------------------------------------

    /// Validate, serialize, and write manifest files to OUT_DIR.
    ///
    /// Writes:
    /// - `OUT_DIR/rapidbyte_manifest.json` -- manifest data
    /// - `OUT_DIR/rapidbyte_manifest_embed.rs` -- `#[link_section]` static for embedding
    ///
    /// Panics with a clear message if required fields are missing.
    pub fn emit(mut self) {
        // Default version from Cargo.toml if not set
        if self.manifest.version.is_empty() {
            self.manifest.version = std::env::var("CARGO_PKG_VERSION")
                .expect("version not set and CARGO_PKG_VERSION not available");
        }

        // Validate required fields
        assert!(!self.manifest.id.is_empty(), "manifest: id is required");
        assert!(!self.manifest.name.is_empty(), "manifest: name is required");

        // Validate role-specific requirements
        if let Some(ref src) = self.manifest.roles.source {
            assert!(
                !src.supported_sync_modes.is_empty(),
                "manifest: source connectors must declare at least one sync mode"
            );
        }
        if let Some(ref dst) = self.manifest.roles.destination {
            assert!(
                !dst.supported_write_modes.is_empty(),
                "manifest: destination connectors must declare at least one write mode"
            );
        }

        let json = serde_json::to_vec_pretty(&self.manifest).expect("failed to serialize manifest");

        let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR not set");
        let out_path = std::path::Path::new(&out_dir);

        // Write manifest JSON
        let json_path = out_path.join("rapidbyte_manifest.json");
        std::fs::write(&json_path, &json).expect("failed to write manifest JSON");

        // Generate #[link_section] embed file
        let embed_code = format!(
            r#"#[link_section = "rapidbyte_manifest_v1"]
#[used]
static __RAPIDBYTE_MANIFEST: [u8; {}] = *include_bytes!("{}");"#,
            json.len(),
            json_path.display().to_string().replace('\\', "\\\\"),
        );
        let embed_path = out_path.join("rapidbyte_manifest_embed.rs");
        std::fs::write(&embed_path, embed_code).expect("failed to write embed file");

        // Cargo rerun directives
        for file in &self.rerun_files {
            println!("cargo::rerun-if-changed={}", file);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_emitter_constructs_manifest() {
        let emitter = ManifestBuilder::source("test/source")
            .name("Test Source")
            .version("1.0.0")
            .description("A test source")
            .sync_modes(&[SyncMode::FullRefresh]);

        assert_eq!(emitter.manifest.id, "test/source");
        assert_eq!(emitter.manifest.name, "Test Source");
        assert_eq!(emitter.manifest.version, "1.0.0");
        assert!(emitter.manifest.roles.source.is_some());
        assert!(emitter.manifest.roles.destination.is_none());
        let src = emitter.manifest.roles.source.as_ref().unwrap();
        assert_eq!(src.supported_sync_modes, vec![SyncMode::FullRefresh]);
    }

    #[test]
    fn test_destination_emitter_constructs_manifest() {
        let emitter = ManifestBuilder::destination("test/dest")
            .name("Test Dest")
            .version("2.0.0")
            .write_modes(&[WriteMode::Append, WriteMode::Replace])
            .dest_features(vec![Feature::BulkLoadCopy]);

        assert_eq!(emitter.manifest.id, "test/dest");
        assert!(emitter.manifest.roles.destination.is_some());
        let dst = emitter.manifest.roles.destination.as_ref().unwrap();
        assert_eq!(
            dst.supported_write_modes,
            vec![WriteMode::Append, WriteMode::Replace]
        );
        assert_eq!(dst.features, vec![Feature::BulkLoadCopy]);
    }

    #[test]
    fn test_permissions_builder() {
        let emitter = ManifestBuilder::source("test/source")
            .name("Test")
            .version("1.0.0")
            .sync_modes(&[SyncMode::FullRefresh])
            .allow_runtime_network()
            .env_vars(&["FOO", "BAR"])
            .allowed_domains(&["example.com"]);

        assert!(
            emitter
                .manifest
                .permissions
                .network
                .allow_runtime_config_domains
        );
        assert_eq!(
            emitter.manifest.permissions.env.allowed_vars,
            vec!["FOO", "BAR"]
        );
        assert_eq!(
            emitter.manifest.permissions.network.allowed_domains,
            Some(vec!["example.com".to_string()])
        );
    }

    #[test]
    fn test_transform_emitter() {
        let emitter = ManifestBuilder::transform("test/transform")
            .name("Test Transform")
            .version("1.0.0");

        assert!(emitter.manifest.roles.transform.is_some());
        assert!(emitter.manifest.roles.source.is_none());
    }

    #[test]
    fn test_limits_builder() {
        let emitter = ManifestBuilder::source("test/source")
            .name("Test")
            .version("1.0.0")
            .sync_modes(&[SyncMode::FullRefresh])
            .max_memory("256mb")
            .min_memory("64mb")
            .timeout_seconds(300);

        assert_eq!(
            emitter.manifest.limits.max_memory,
            Some("256mb".to_string())
        );
        assert_eq!(emitter.manifest.limits.min_memory, Some("64mb".to_string()));
        assert_eq!(emitter.manifest.limits.timeout_seconds, Some(300));
    }
}
