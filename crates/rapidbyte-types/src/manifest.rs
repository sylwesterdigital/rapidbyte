//! Connector manifest and WASI sandbox configuration.
//!
//! A [`ConnectorManifest`] is embedded in each connector's WASM binary
//! as a custom section. The host reads it at load time to discover
//! capabilities, configure permissions, and enforce resource limits.

use crate::wire::{ConnectorRole, Feature, ProtocolVersion, SyncMode, WriteMode};
use serde::{Deserialize, Serialize};

// ── Permissions ─────────────────────────────────────────────────────

/// Network access permissions for the WASI sandbox.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkPermissions {
    /// Static list of allowed domains/IPs. Use `["*"]` for unrestricted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_domains: Option<Vec<String>>,
    /// If true, the host may inspect connector config for dynamic domains.
    #[serde(default)]
    pub allow_runtime_config_domains: bool,
}

/// Filesystem access permissions for the WASI sandbox.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsPermissions {
    /// Directories the connector needs mounted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preopens: Vec<String>,
}

/// Environment variable access permissions for the WASI sandbox.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvPermissions {
    /// Environment variables the connector is allowed to read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_vars: Vec<String>,
}

/// Combined WASI sandbox permissions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Permissions {
    #[serde(default)]
    pub network: NetworkPermissions,
    #[serde(default)]
    pub fs: FsPermissions,
    #[serde(default)]
    pub env: EnvPermissions,
}

/// Resource limits for the WASI sandbox.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Maximum WASI linear memory (e.g., `"256mb"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,
    /// Maximum execution time in seconds (epoch interruption).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    /// Minimum recommended memory (e.g., `"128mb"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_memory: Option<String>,
}

// ── Roles & Capabilities ────────────────────────────────────────────

/// Capabilities declared when a connector supports the Source role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCapabilities {
    /// Sync modes this source supports.
    pub supported_sync_modes: Vec<SyncMode>,
    /// Feature flags for this source.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<Feature>,
}

/// Capabilities declared when a connector supports the Destination role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCapabilities {
    /// Write modes this destination supports.
    pub supported_write_modes: Vec<WriteMode>,
    /// Feature flags for this destination.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<Feature>,
}

/// Capabilities declared when a connector supports the Transform role.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransformCapabilities {}

/// Role-specific capability declarations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Roles {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceCapabilities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<DestinationCapabilities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<TransformCapabilities>,
}

// ── Manifest ────────────────────────────────────────────────────────

/// Connector metadata embedded in the WASM binary.
///
/// Read by the host at load time to discover capabilities and configure
/// the WASI sandbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorManifest {
    /// Connector identifier (e.g., `"rapidbyte/source-postgres"`).
    pub id: String,
    /// Human-readable display name.
    pub name: String,
    /// Semantic version (e.g., `"0.1.0"`).
    pub version: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Author or organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    /// SPDX license identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    /// Protocol version this connector implements.
    pub protocol_version: ProtocolVersion,
    /// WASI sandbox permissions.
    #[serde(default)]
    pub permissions: Permissions,
    /// WASI resource limits.
    #[serde(default)]
    pub limits: ResourceLimits,
    /// Role-specific capability declarations.
    #[serde(default)]
    pub roles: Roles,
    /// JSON Schema (Draft 7) for connector config validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema: Option<serde_json::Value>,
}

impl ConnectorManifest {
    /// Check if this connector supports the given role.
    #[must_use]
    pub fn supports_role(&self, role: ConnectorRole) -> bool {
        match role {
            ConnectorRole::Source => self.roles.source.is_some(),
            ConnectorRole::Destination => self.roles.destination.is_some(),
            ConnectorRole::Transform => self.roles.transform.is_some(),
        }
    }

    /// Check whether the source role declares a given feature.
    pub fn has_source_feature(&self, feature: Feature) -> bool {
        self.roles
            .source
            .as_ref()
            .is_some_and(|s| s.features.contains(&feature))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_manifest() -> ConnectorManifest {
        ConnectorManifest {
            id: "rapidbyte/source-postgres".into(),
            name: "PostgreSQL Source".into(),
            version: "0.1.0".into(),
            description: "Reads from PostgreSQL".into(),
            author: None,
            license: Some("MIT".into()),
            protocol_version: ProtocolVersion::V4,
            permissions: Permissions {
                network: NetworkPermissions {
                    allowed_domains: None,
                    allow_runtime_config_domains: true,
                },
                ..Permissions::default()
            },
            limits: ResourceLimits {
                max_memory: Some("256mb".into()),
                timeout_seconds: Some(300),
                min_memory: None,
            },
            roles: Roles {
                source: Some(SourceCapabilities {
                    supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental],
                    features: vec![Feature::Stateful],
                }),
                ..Roles::default()
            },
            config_schema: None,
        }
    }

    #[test]
    fn manifest_roundtrip() {
        let m = test_manifest();
        let json = serde_json::to_string(&m).unwrap();
        let back: ConnectorManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn supports_role() {
        let m = test_manifest();
        assert!(m.supports_role(ConnectorRole::Source));
        assert!(!m.supports_role(ConnectorRole::Destination));
        assert!(!m.supports_role(ConnectorRole::Transform));
    }

    #[test]
    fn optional_fields_skipped_in_json() {
        let m = test_manifest();
        let json = serde_json::to_value(&m).unwrap();
        assert!(json.get("author").is_none());
        assert!(json.get("config_schema").is_none());
    }

    #[test]
    fn permissions_default_is_empty() {
        let p = Permissions::default();
        assert!(p.network.allowed_domains.is_none());
        assert!(!p.network.allow_runtime_config_domains);
        assert!(p.fs.preopens.is_empty());
        assert!(p.env.allowed_vars.is_empty());
    }

    #[test]
    fn manifest_has_source_feature() {
        let manifest = ConnectorManifest {
            id: "test/pg".to_string(),
            name: "Test".to_string(),
            version: "0.1.0".to_string(),
            description: String::new(),
            author: None,
            license: None,
            protocol_version: ProtocolVersion::V4,
            roles: Roles {
                source: Some(SourceCapabilities {
                    supported_sync_modes: vec![SyncMode::FullRefresh],
                    features: vec![Feature::PartitionedRead],
                }),
                destination: None,
                transform: None,
            },
            permissions: Permissions::default(),
            limits: ResourceLimits::default(),
            config_schema: None,
        };

        assert!(manifest.has_source_feature(Feature::PartitionedRead));
        assert!(!manifest.has_source_feature(Feature::Cdc));
    }
}
