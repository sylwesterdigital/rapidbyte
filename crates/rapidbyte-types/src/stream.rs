//! Stream execution context, limits, and policies.
//!
//! [`StreamContext`] is the central type passed to connectors for each
//! stream operation. It bundles schema, sync configuration, resource
//! limits, and error handling policies.

use crate::catalog::SchemaHint;
use crate::cursor::{CursorInfo, CursorType, CursorValue};
use crate::wire::{SyncMode, WriteMode};
use serde::{Deserialize, Serialize};

// ── Policies ────────────────────────────────────────────────────────

/// How to handle records that fail validation or conversion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataErrorPolicy {
    /// Skip the invalid record and continue.
    Skip,
    /// Fail the entire batch.
    #[default]
    Fail,
    /// Route the record to the dead-letter queue.
    Dlq,
}

/// How to handle new or removed columns between runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnPolicy {
    /// Automatically add new columns / silently ignore removed ones.
    #[default]
    Add,
    /// Ignore the schema change entirely.
    Ignore,
    /// Fail on any column change.
    Fail,
}

/// How to handle type changes between runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeChangePolicy {
    /// Attempt to coerce the value to the new type.
    Coerce,
    /// Fail on any type change.
    #[default]
    Fail,
    /// Convert the value to null.
    Null,
}

/// How to handle nullability changes between runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullabilityPolicy {
    /// Allow nullability changes.
    #[default]
    Allow,
    /// Fail on nullability changes.
    Fail,
}

/// Partition strategy for full-refresh source fan-out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    Mod,
    Range,
}

/// Typed partition coordinates passed to sources that declare `PartitionedRead`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCoordinates {
    pub count: u32,
    pub index: u32,
    pub strategy: PartitionStrategy,
}

/// Typed CDC resume token passed to sources that declare `Cdc`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcResumeToken {
    /// The opaque resume value (LSN, offset, etc.). `None` on first run.
    pub value: Option<String>,
    /// The cursor type hint for the source.
    pub cursor_type: CursorType,
}

/// Schema evolution behavior when source schema changes between runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaEvolutionPolicy {
    #[serde(default)]
    pub new_column: ColumnPolicy,
    #[serde(default = "default_removed_column")]
    pub removed_column: ColumnPolicy,
    #[serde(default)]
    pub type_change: TypeChangePolicy,
    #[serde(default)]
    pub nullability_change: NullabilityPolicy,
}

fn default_removed_column() -> ColumnPolicy {
    ColumnPolicy::Ignore
}

impl Default for SchemaEvolutionPolicy {
    fn default() -> Self {
        Self {
            new_column: ColumnPolicy::Add,
            removed_column: ColumnPolicy::Ignore,
            type_change: TypeChangePolicy::Fail,
            nullability_change: NullabilityPolicy::Allow,
        }
    }
}

// ── Stream Policies ─────────────────────────────────────────────────

/// Combined error handling and schema evolution policies for a stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPolicies {
    #[serde(default)]
    pub on_data_error: DataErrorPolicy,
    #[serde(default)]
    pub schema_evolution: SchemaEvolutionPolicy,
}

// ── Stream Limits ───────────────────────────────────────────────────

/// Resource and batching limits for a stream operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamLimits {
    /// Maximum batch size in bytes.
    pub max_batch_bytes: u64,
    /// Maximum single record size in bytes.
    pub max_record_bytes: u64,
    /// Maximum batches in flight between source and destination.
    pub max_inflight_batches: u32,
    /// Maximum parallel requests to the external system.
    pub max_parallel_requests: u32,
    /// Checkpoint after this many bytes processed.
    pub checkpoint_interval_bytes: u64,
    /// Checkpoint after this many rows (0 = disabled).
    #[serde(default)]
    pub checkpoint_interval_rows: u64,
    /// Checkpoint after this many seconds (0 = disabled).
    #[serde(default)]
    pub checkpoint_interval_seconds: u64,
    /// Maximum number of records to read (None = unlimited).
    /// Used by dry-run mode to cap source reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records: Option<u64>,
}

impl StreamLimits {
    /// 64 MiB default batch size.
    pub const DEFAULT_MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;
    /// 16 MiB default record size.
    pub const DEFAULT_MAX_RECORD_BYTES: u64 = 16 * 1024 * 1024;
    /// 64 MiB default checkpoint interval.
    pub const DEFAULT_CHECKPOINT_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;
    /// 16 default inflight batches.
    pub const DEFAULT_MAX_INFLIGHT_BATCHES: u32 = 16;
    /// 1 default parallel request.
    pub const DEFAULT_MAX_PARALLEL_REQUESTS: u32 = 1;
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            max_batch_bytes: Self::DEFAULT_MAX_BATCH_BYTES,
            max_record_bytes: Self::DEFAULT_MAX_RECORD_BYTES,
            max_inflight_batches: Self::DEFAULT_MAX_INFLIGHT_BATCHES,
            max_parallel_requests: Self::DEFAULT_MAX_PARALLEL_REQUESTS,
            checkpoint_interval_bytes: Self::DEFAULT_CHECKPOINT_INTERVAL_BYTES,
            checkpoint_interval_rows: 0,
            checkpoint_interval_seconds: 0,
            max_records: None,
        }
    }
}

// ── Stream Context ──────────────────────────────────────────────────

/// Execution context passed to a connector for a single stream operation.
///
/// Bundles everything a connector needs: stream identity, schema, sync
/// configuration, resource limits, and error handling policies.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamContext {
    /// Name of the stream being processed.
    pub stream_name: String,
    /// Physical source table/view name when execution stream naming differs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_stream_name: Option<String>,
    /// Schema of the stream.
    pub schema: SchemaHint,
    /// How data is read from the source.
    pub sync_mode: SyncMode,
    /// Cursor tracking state for incremental sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_info: Option<CursorInfo>,
    /// Resource and batching limits.
    #[serde(default)]
    pub limits: StreamLimits,
    /// Error handling and schema evolution policies.
    #[serde(default)]
    pub policies: StreamPolicies,
    /// How data is written to the destination (`None` for source/transform).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_mode: Option<WriteMode>,
    /// Column projection (`None` = all columns).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_columns: Option<Vec<String>>,
    /// Total number of source partitions for this stream execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_count: Option<u32>,
    /// Zero-based partition index for this stream execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_index: Option<u32>,
    /// Effective worker parallelism selected for this stream execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_parallelism: Option<u32>,
    /// Source partition strategy override selected for this stream execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_strategy: Option<PartitionStrategy>,
    /// Destination COPY flush threshold override in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_flush_bytes_override: Option<u64>,
}

impl StreamContext {
    /// Returns the physical source stream/table name used by source connectors.
    ///
    /// Falls back to [`Self::stream_name`] when no explicit source name is set.
    #[must_use]
    pub fn source_stream_or_stream_name(&self) -> &str {
        self.source_stream_name
            .as_deref()
            .unwrap_or(&self.stream_name)
    }

    /// Create a minimal context for testing, with only a stream name required.
    #[cfg(test)]
    #[must_use]
    pub fn test_default(stream_name: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            source_stream_name: None,
            schema: SchemaHint::Columns(Vec::new()),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
            partition_count: None,
            partition_index: None,
            effective_parallelism: None,
            partition_strategy: None,
            copy_flush_bytes_override: None,
        }
    }

    /// Returns validated partition coordinates when both values are present.
    ///
    /// Validation rules:
    /// - `partition_count > 0`
    /// - `partition_index < partition_count`
    #[must_use]
    pub fn partition_coordinates(&self) -> Option<(u32, u32)> {
        match (self.partition_count, self.partition_index) {
            (Some(partition_count), Some(partition_index))
                if partition_count > 0 && partition_index < partition_count =>
            {
                Some((partition_count, partition_index))
            }
            _ => None,
        }
    }

    /// Extract typed partition coordinates if present and valid.
    #[must_use]
    pub fn partition_coordinates_typed(&self) -> Option<PartitionCoordinates> {
        let (count, index) = self.partition_coordinates()?;
        Some(PartitionCoordinates {
            count,
            index,
            strategy: self.partition_strategy.unwrap_or(PartitionStrategy::Mod),
        })
    }

    /// Extract a typed CDC resume token from cursor info.
    #[must_use]
    pub fn cdc_resume_token(&self) -> Option<CdcResumeToken> {
        if self.sync_mode != SyncMode::Cdc {
            return None;
        }
        let cursor = self.cursor_info.as_ref()?;
        Some(CdcResumeToken {
            value: cursor.last_value.as_ref().map(|v| match v {
                CursorValue::Utf8 { value }
                | CursorValue::Lsn { value }
                | CursorValue::Decimal { value, .. } => value.clone(),
                CursorValue::Int64 { value }
                | CursorValue::TimestampMillis { value }
                | CursorValue::TimestampMicros { value } => value.to_string(),
                CursorValue::Json { value } => value.to_string(),
                // Null and any future variants map to empty string.
                _ => String::new(),
            }),
            cursor_type: cursor.cursor_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowDataType;
    use crate::catalog::ColumnSchema;

    #[test]
    fn stream_limits_defaults() {
        let limits = StreamLimits::default();
        assert_eq!(limits.max_batch_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.max_record_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.max_inflight_batches, 16);
        assert_eq!(limits.max_parallel_requests, 1);
        assert_eq!(limits.checkpoint_interval_rows, 0);
    }

    #[test]
    fn schema_evolution_defaults() {
        let policy = SchemaEvolutionPolicy::default();
        assert_eq!(policy.new_column, ColumnPolicy::Add);
        assert_eq!(policy.removed_column, ColumnPolicy::Ignore);
        assert_eq!(policy.type_change, TypeChangePolicy::Fail);
        assert_eq!(policy.nullability_change, NullabilityPolicy::Allow);
    }

    #[test]
    fn stream_context_roundtrip() {
        let ctx = StreamContext {
            schema: SchemaHint::Columns(vec![ColumnSchema {
                name: "id".into(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            }]),
            sync_mode: SyncMode::Incremental,
            write_mode: Some(WriteMode::Append),
            partition_count: Some(4),
            partition_index: Some(2),
            effective_parallelism: Some(4),
            partition_strategy: Some(PartitionStrategy::Range),
            copy_flush_bytes_override: Some(8 * 1024 * 1024),
            ..StreamContext::test_default("public.users")
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let back: StreamContext = serde_json::from_str(&json).unwrap();
        assert_eq!(ctx, back);
    }

    #[test]
    fn data_error_policy_default_is_fail() {
        assert_eq!(DataErrorPolicy::default(), DataErrorPolicy::Fail);
    }

    #[test]
    fn stream_limits_max_records_serde_roundtrip() {
        let limits = StreamLimits {
            max_records: Some(500),
            ..StreamLimits::default()
        };
        let json = serde_json::to_string(&limits).unwrap();
        let back: StreamLimits = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_records, Some(500));
    }

    #[test]
    fn stream_limits_max_records_default_is_none() {
        let limits = StreamLimits::default();
        assert_eq!(limits.max_records, None);
    }

    #[test]
    fn source_stream_or_stream_name_uses_source_override_when_present() {
        let ctx = StreamContext {
            source_stream_name: Some("users".into()),
            ..StreamContext::test_default("users_shard_0")
        };
        assert_eq!(ctx.source_stream_or_stream_name(), "users");
    }

    #[test]
    fn source_stream_or_stream_name_falls_back_to_stream_name() {
        let ctx = StreamContext::test_default("users");
        assert_eq!(ctx.source_stream_or_stream_name(), "users");
    }

    #[test]
    fn partition_coordinates_returns_some_for_valid_values() {
        let ctx = StreamContext {
            partition_count: Some(4),
            partition_index: Some(2),
            ..StreamContext::test_default("users")
        };
        assert_eq!(ctx.partition_coordinates(), Some((4, 2)));
    }

    #[test]
    fn partition_coordinates_returns_none_for_invalid_values() {
        let zero_count = StreamContext {
            partition_count: Some(0),
            partition_index: Some(0),
            ..StreamContext::test_default("users")
        };
        assert_eq!(zero_count.partition_coordinates(), None);

        let out_of_bounds = StreamContext {
            partition_count: Some(2),
            partition_index: Some(2),
            ..zero_count
        };
        assert_eq!(out_of_bounds.partition_coordinates(), None);
    }

    #[test]
    fn partition_coordinates_typed_returns_struct() {
        let ctx = StreamContext {
            partition_count: Some(4),
            partition_index: Some(2),
            partition_strategy: Some(PartitionStrategy::Range),
            ..StreamContext::test_default("users")
        };
        let coords = ctx.partition_coordinates_typed().unwrap();
        assert_eq!(coords.count, 4);
        assert_eq!(coords.index, 2);
        assert_eq!(coords.strategy, PartitionStrategy::Range);
    }

    #[test]
    fn partition_coordinates_typed_defaults_to_mod() {
        let ctx = StreamContext {
            partition_count: Some(4),
            partition_index: Some(0),
            ..StreamContext::test_default("users")
        };
        let coords = ctx.partition_coordinates_typed().unwrap();
        assert_eq!(coords.strategy, PartitionStrategy::Mod);
    }

    #[test]
    fn cdc_resume_token_returns_none_for_non_cdc() {
        let ctx = StreamContext::test_default("users");
        assert!(ctx.cdc_resume_token().is_none());
    }
}
