//! State backend model types.
//!
//! Pure data types used by `StateBackend`
//! implementations. Kept in the types crate so both host and state crates
//! can share them without circular dependencies.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Newtypes
// ---------------------------------------------------------------------------

/// Opaque pipeline identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PipelineId(String);

impl PipelineId {
    /// Create a new pipeline identifier.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PipelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl<S: Into<String>> From<S> for PipelineId {
    fn from(value: S) -> Self {
        Self(value.into())
    }
}

/// Opaque stream name (e.g. `"public.users"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamName(String);

impl StreamName {
    /// Create a new stream name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StreamName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl<S: Into<String>> From<S> for StreamName {
    fn from(value: S) -> Self {
        Self(value.into())
    }
}

// ---------------------------------------------------------------------------
// Run tracking
// ---------------------------------------------------------------------------

/// Terminal status of a sync run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
}

impl RunStatus {
    /// Wire-format string for storage.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Aggregate statistics for a completed sync run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStats {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

// ---------------------------------------------------------------------------
// Cursor state
// ---------------------------------------------------------------------------

/// Snapshot of a persisted cursor for a (pipeline, stream) pair.
///
/// `updated_at` is an ISO-8601 UTC string (e.g. `"2026-01-15T10:00:00Z"`).
/// Backends handle timestamp formatting internally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorState {
    /// Column used for incremental sync (e.g. `"updated_at"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_field: Option<String>,
    /// Last-seen value of the cursor column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_value: Option<String>,
    /// ISO-8601 UTC timestamp of when this cursor was last written.
    pub updated_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_id_display_and_as_str() {
        let pid = PipelineId::new("my-pipeline");
        assert_eq!(pid.as_str(), "my-pipeline");
        assert_eq!(pid.to_string(), "my-pipeline");
    }

    #[test]
    fn stream_name_from_and_display() {
        let sn = StreamName::from("public.users");
        assert_eq!(sn.as_str(), "public.users");
        assert_eq!(sn.to_string(), "public.users");
    }

    #[test]
    fn pipeline_id_eq_and_hash() {
        use std::collections::HashSet;
        let a = PipelineId::new("p1");
        let b = PipelineId::new("p1");
        assert_eq!(a, b);
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn run_status_as_str() {
        assert_eq!(RunStatus::Running.as_str(), "running");
        assert_eq!(RunStatus::Completed.as_str(), "completed");
        assert_eq!(RunStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn run_status_serde_roundtrip() {
        let json = serde_json::to_string(&RunStatus::Completed).unwrap();
        assert_eq!(json, "\"completed\"");
        let back: RunStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, RunStatus::Completed);
    }

    #[test]
    fn run_stats_default_is_zeroed() {
        let stats = RunStats::default();
        assert_eq!(stats.records_read, 0);
        assert_eq!(stats.records_written, 0);
        assert_eq!(stats.bytes_read, 0);
        assert!(stats.error_message.is_none());
    }

    #[test]
    fn cursor_state_serde_roundtrip() {
        let cs = CursorState {
            cursor_field: Some("id".into()),
            cursor_value: Some("42".into()),
            updated_at: "2026-01-15T10:00:00Z".into(),
        };
        let json = serde_json::to_string(&cs).unwrap();
        let back: CursorState = serde_json::from_str(&json).unwrap();
        assert_eq!(cs, back);
    }

    #[test]
    fn pipeline_id_serde_transparent() {
        let pid = PipelineId::new("test");
        let json = serde_json::to_string(&pid).unwrap();
        assert_eq!(json, "\"test\"");
    }
}
