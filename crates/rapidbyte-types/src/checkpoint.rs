//! Checkpoint and state scope types.
//!
//! [`Checkpoint`]s record progress during pipeline execution. The host
//! correlates source and destination checkpoints to safely persist cursor
//! positions for incremental sync resume.

use crate::cursor::CursorValue;
use serde::{Deserialize, Serialize};

/// Which pipeline stage emitted a checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointKind {
    /// Emitted by the source after reading records.
    Source,
    /// Emitted by the destination after writing records.
    Dest,
    /// Emitted by a transform connector.
    Transform,
}

impl TryFrom<u32> for CheckpointKind {
    type Error = u32;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Source),
            1 => Ok(Self::Dest),
            2 => Ok(Self::Transform),
            other => Err(other),
        }
    }
}

/// Scope for key-value state operations.
///
/// Connectors can store arbitrary state at different scopes via
/// host `state_get` / `state_put` imports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateScope {
    /// Shared state for the entire pipeline.
    Pipeline,
    /// Per-stream state.
    Stream,
    /// Per-connector-instance state.
    ConnectorInstance,
}

/// Progress marker emitted during pipeline execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Monotonically increasing checkpoint sequence number.
    pub id: u64,
    /// Which pipeline stage emitted this checkpoint.
    pub kind: CheckpointKind,
    /// Stream this checkpoint belongs to.
    pub stream: String,
    /// Cursor column name (if cursor-based sync).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_field: Option<String>,
    /// Cursor position at this checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_value: Option<CursorValue>,
    /// Total records processed up to this checkpoint.
    pub records_processed: u64,
    /// Total bytes processed up to this checkpoint.
    pub bytes_processed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_roundtrip() {
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: "public.users".into(),
            cursor_field: Some("id".into()),
            cursor_value: Some(CursorValue::Int64 { value: 1000 }),
            records_processed: 500,
            bytes_processed: 65536,
        };
        let json = serde_json::to_string(&cp).unwrap();
        let back: Checkpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(cp, back);
    }

    #[test]
    fn checkpoint_no_cursor_skips_fields() {
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Dest,
            stream: "users".into(),
            cursor_field: None,
            cursor_value: None,
            records_processed: 100,
            bytes_processed: 4096,
        };
        let json = serde_json::to_value(&cp).unwrap();
        assert!(json.get("cursor_field").is_none());
        assert!(json.get("cursor_value").is_none());
    }

    #[test]
    fn state_scope_serde() {
        for (scope, expected) in [
            (StateScope::Pipeline, "\"pipeline\""),
            (StateScope::Stream, "\"stream\""),
            (StateScope::ConnectorInstance, "\"connector_instance\""),
        ] {
            assert_eq!(serde_json::to_string(&scope).unwrap(), expected);
        }
    }
}
