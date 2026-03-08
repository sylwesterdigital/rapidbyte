//! Core protocol types shared across the host/connector boundary.
//!
//! Contains the fundamental enums and structs that define how connectors
//! communicate with the host runtime: sync modes, write modes, connector
//! roles, feature flags, and connector self-description.

use serde::{Deserialize, Serialize};

/// Rapidbyte protocol version.
///
/// Determines serialization format and available host imports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolVersion {
    /// Version 4 — current stable protocol.
    #[default]
    #[serde(rename = "4")]
    V4,
}

/// How data is read from a source stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    /// One-time full read of all records.
    FullRefresh,
    /// Cursor-based incremental reads since last checkpoint.
    Incremental,
    /// Change data capture via database replication.
    Cdc,
}

/// How data is written to a destination stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum WriteMode {
    /// Insert all records (no deduplication).
    Append,
    /// Replace the entire destination table each run.
    Replace,
    /// Merge records by primary key (insert or update).
    Upsert {
        /// Columns forming the primary key for merge matching.
        primary_key: Vec<String>,
    },
}

/// Role a connector fulfills in a pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorRole {
    /// Reads data from an external system.
    Source,
    /// Writes data to an external system.
    Destination,
    /// Transforms data in-flight between source and destination.
    Transform,
}

/// Capability flag declared by a connector.
///
/// Used in both runtime [`ConnectorInfo`] and manifest capability declarations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    /// Change data capture support.
    Cdc,
    /// Maintains cursor state across runs.
    Stateful,
    /// Exactly-once delivery guarantees.
    ExactlyOnce,
    /// Automatic schema migration on destination.
    SchemaAutoMigrate,
    /// Bulk load support (COPY, multipart upload, load jobs, etc.).
    #[serde(alias = "bulk_load_copy")]
    BulkLoad,
    /// Source supports parallel partitioned reads (mod/range sharding).
    PartitionedRead,
}

/// Connector self-description returned at initialization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorInfo {
    /// Protocol version this connector implements.
    pub protocol_version: ProtocolVersion,
    /// Feature flags supported by this connector.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<Feature>,
    /// Default maximum batch size in bytes.
    pub default_max_batch_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_mode_roundtrip() {
        for mode in [SyncMode::FullRefresh, SyncMode::Incremental, SyncMode::Cdc] {
            let json = serde_json::to_string(&mode).unwrap();
            let back: SyncMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, back);
        }
    }

    #[test]
    fn write_mode_upsert_json_format() {
        let mode = WriteMode::Upsert {
            primary_key: vec!["id".into()],
        };
        let json = serde_json::to_value(&mode).unwrap();
        assert_eq!(json["mode"], "upsert");
        assert_eq!(json["primary_key"], serde_json::json!(["id"]));
    }

    #[test]
    fn protocol_version_default_is_v4() {
        let v = ProtocolVersion::default();
        assert_eq!(v, ProtocolVersion::V4);
        assert_eq!(serde_json::to_string(&v).unwrap(), "\"4\"");
    }

    #[test]
    fn feature_serde_snake_case() {
        let f = Feature::SchemaAutoMigrate;
        assert_eq!(
            serde_json::to_string(&f).unwrap(),
            "\"schema_auto_migrate\""
        );
    }

    #[test]
    fn feature_partitioned_read_serde_roundtrip() {
        let feature = Feature::PartitionedRead;
        let json = serde_json::to_string(&feature).unwrap();
        assert_eq!(json, "\"partitioned_read\"");
        let back: Feature = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Feature::PartitionedRead);
    }

    #[test]
    fn feature_bulk_load_serde_and_alias() {
        // New name serializes as "bulk_load"
        let f = Feature::BulkLoad;
        assert_eq!(serde_json::to_string(&f).unwrap(), "\"bulk_load\"");

        // New name deserializes
        let back: Feature = serde_json::from_str("\"bulk_load\"").unwrap();
        assert_eq!(back, Feature::BulkLoad);

        // Old name still deserializes (backward compat)
        let old: Feature = serde_json::from_str("\"bulk_load_copy\"").unwrap();
        assert_eq!(old, Feature::BulkLoad);
    }

    #[test]
    fn connector_info_roundtrip() {
        let info = ConnectorInfo {
            protocol_version: ProtocolVersion::V4,
            features: vec![Feature::Cdc, Feature::Stateful],
            default_max_batch_bytes: 64 * 1024 * 1024,
        };
        let json = serde_json::to_string(&info).unwrap();
        let back: ConnectorInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info, back);
    }
}
