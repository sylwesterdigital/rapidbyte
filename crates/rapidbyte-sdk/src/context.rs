//! Plugin execution context.
//!
//! `Context` bundles stream metadata with host-FFI operations so plugin
//! authors no longer need to pass `connector_id` / `stream_name` to every call.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::checkpoint::{Checkpoint, StateScope};
use crate::error::{PluginError, ErrorCategory};
use crate::host_ffi;
use crate::metric::Metric;

/// Log severity levels used by [`Context::log`].
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogLevel {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

/// Execution context carrying stream metadata and host-FFI delegation.
///
/// Plugin authors receive a `Context` at each lifecycle entry-point.
/// Methods that require `connector_id` or `stream_name` (checkpoints,
/// metrics, DLQ records) automatically supply the values stored here.
///
/// ```ignore
/// ctx.log(LogLevel::Info, "starting read");
/// ctx.checkpoint(&cp)?;
/// ```
#[derive(Debug, Clone)]
pub struct Context {
    connector_id: String,
    stream_name: String,
}

impl Context {
    /// Create a new context for the given connector and stream.
    pub fn new(connector_id: impl Into<String>, stream_name: impl Into<String>) -> Self {
        Self {
            connector_id: connector_id.into(),
            stream_name: stream_name.into(),
        }
    }

    /// Returns the connector identifier.
    pub fn connector_id(&self) -> &str {
        &self.connector_id
    }

    /// Returns the current stream name.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Derive a new `Context` that targets a different stream while keeping the
    /// same connector identity.
    pub fn with_stream(&self, stream_name: impl Into<String>) -> Self {
        Self {
            connector_id: self.connector_id.clone(),
            stream_name: stream_name.into(),
        }
    }

    // ------------------------------------------------------------------
    // Host-FFI delegation
    // ------------------------------------------------------------------

    /// Write a log message at the given severity level.
    pub fn log(&self, level: LogLevel, message: &str) {
        host_ffi::log(level as i32, message);
    }

    /// Emit an Arrow `RecordBatch` to the host pipeline.
    pub fn emit_batch(&self, batch: &RecordBatch) -> Result<(), PluginError> {
        host_ffi::emit_batch(batch)
    }

    /// Receive the next Arrow `RecordBatch` from the host pipeline.
    ///
    /// Returns `None` when there are no more batches.
    #[allow(clippy::type_complexity)]
    pub fn next_batch(
        &self,
        max_bytes: u64,
    ) -> Result<Option<(Arc<Schema>, Vec<RecordBatch>)>, PluginError> {
        host_ffi::next_batch(max_bytes)
    }

    /// Read a value from the host state store.
    pub fn state_get(
        &self,
        scope: StateScope,
        key: &str,
    ) -> Result<Option<String>, PluginError> {
        host_ffi::state_get(scope, key)
    }

    /// Write a value to the host state store.
    pub fn state_put(
        &self,
        scope: StateScope,
        key: &str,
        value: &str,
    ) -> Result<(), PluginError> {
        host_ffi::state_put(scope, key, value)
    }

    /// Emit a checkpoint using the context's connector ID and stream name.
    pub fn checkpoint(&self, cp: &Checkpoint) -> Result<(), PluginError> {
        host_ffi::checkpoint(&self.connector_id, &self.stream_name, cp)
    }

    /// Emit a metric using the context's connector ID and stream name.
    pub fn metric(&self, m: &Metric) -> Result<(), PluginError> {
        host_ffi::metric(&self.connector_id, &self.stream_name, m)
    }

    /// Emit a dead-letter-queue record using the context's stream name.
    pub fn emit_dlq_record(
        &self,
        record_json: &str,
        error_message: &str,
        error_category: ErrorCategory,
    ) -> Result<(), PluginError> {
        host_ffi::emit_dlq_record(
            &self.stream_name,
            record_json,
            error_message,
            error_category,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stores_metadata() {
        let ctx = Context::new("my-connector", "users");
        assert_eq!(ctx.connector_id(), "my-connector");
        assert_eq!(ctx.stream_name(), "users");
    }

    #[test]
    fn test_with_stream_changes_stream() {
        let ctx = Context::new("my-connector", "users");
        let ctx2 = ctx.with_stream("orders");
        assert_eq!(ctx2.connector_id(), "my-connector");
        assert_eq!(ctx2.stream_name(), "orders");
        // Original is unchanged.
        assert_eq!(ctx.stream_name(), "users");
    }

    #[test]
    fn test_with_stream_accepts_string() {
        let ctx = Context::new("c", "a");
        let ctx2 = ctx.with_stream(String::from("b"));
        assert_eq!(ctx2.stream_name(), "b");
    }

    #[test]
    fn test_clone_is_independent() {
        let ctx = Context::new("c1", "s1");
        let ctx2 = ctx.clone();
        assert_eq!(ctx.connector_id(), ctx2.connector_id());
        assert_eq!(ctx.stream_name(), ctx2.stream_name());
    }

    #[test]
    fn test_log_level_repr() {
        assert_eq!(LogLevel::Error as i32, 0);
        assert_eq!(LogLevel::Warn as i32, 1);
        assert_eq!(LogLevel::Info as i32, 2);
        assert_eq!(LogLevel::Debug as i32, 3);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_log_does_not_panic() {
        let ctx = Context::new("c", "s");
        // Should delegate to the stub without panicking.
        ctx.log(LogLevel::Info, "hello");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_emit_batch_delegates() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

        let ctx = Context::new("c", "s");
        let result = ctx.emit_batch(&batch);
        assert!(result.is_ok());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_next_batch_returns_none() {
        let ctx = Context::new("c", "s");
        let result = ctx.next_batch(1024).expect("next batch");
        assert!(result.is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_state_roundtrip_with_stub() {
        let ctx = Context::new("c", "s");
        // Stub state_put succeeds.
        ctx.state_put(StateScope::Stream, "key", "value").unwrap();
        // Stub state_get always returns None.
        let val = ctx.state_get(StateScope::Stream, "key").unwrap();
        assert!(val.is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_checkpoint_delegates() {
        use crate::checkpoint::CheckpointKind;

        let ctx = Context::new("my-conn", "my-stream");
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: "my-stream".to_string(),
            cursor_field: None,
            cursor_value: None,
            records_processed: 0,
            bytes_processed: 0,
        };
        let result = ctx.checkpoint(&cp);
        assert!(result.is_ok());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_metric_delegates() {
        use crate::metric::MetricValue;

        let ctx = Context::new("my-conn", "my-stream");
        let m = Metric {
            name: "rows_read".to_string(),
            value: MetricValue::Counter(42),
            labels: vec![],
        };
        let result = ctx.metric(&m);
        assert!(result.is_ok());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_emit_dlq_record_delegates() {
        let ctx = Context::new("c", "s");
        let result = ctx.emit_dlq_record("{}", "bad record", ErrorCategory::Data);
        assert!(result.is_ok());
    }
}
