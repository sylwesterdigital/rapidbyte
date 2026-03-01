//! Guest-side host import wrappers for the component model.

use std::sync::Arc;
use std::sync::OnceLock;

#[cfg(target_arch = "wasm32")]
use crate::checkpoint::CheckpointKind;
use crate::checkpoint::{Checkpoint, StateScope};
#[cfg(target_arch = "wasm32")]
use crate::envelope::PayloadEnvelope;
#[cfg(target_arch = "wasm32")]
use crate::error::{BackoffClass, CommitState, ErrorScope};
use crate::error::{ConnectorError, ErrorCategory};
use crate::metric::Metric;
#[cfg(target_arch = "wasm32")]
use crate::wire::ProtocolVersion;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

#[cfg(target_arch = "wasm32")]
mod bindings {
    wit_bindgen::generate!({
        path: "../../wit",
        world: "rapidbyte-host",
    });
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketReadResult {
    Data(Vec<u8>),
    Eof,
    WouldBlock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketWriteResult {
    Written(u64),
    WouldBlock,
}

/// Host imports abstraction to make host FFI behavior testable on native targets.
pub trait HostImports: Send + Sync {
    fn log(&self, level: i32, message: &str);

    // V3 frame lifecycle
    fn frame_new(&self, capacity: u64) -> Result<u64, ConnectorError>;
    fn frame_write(&self, handle: u64, chunk: &[u8]) -> Result<u64, ConnectorError>;
    fn frame_seal(&self, handle: u64) -> Result<(), ConnectorError>;
    fn frame_len(&self, handle: u64) -> Result<u64, ConnectorError>;
    fn frame_read(&self, handle: u64, offset: u64, len: u64) -> Result<Vec<u8>, ConnectorError>;
    fn frame_drop(&self, handle: u64);

    // V3 batch transport (handle-based)
    fn emit_batch(&self, handle: u64) -> Result<(), ConnectorError>;
    fn next_batch(&self) -> Result<Option<u64>, ConnectorError>;

    fn state_get(&self, scope: StateScope, key: &str) -> Result<Option<String>, ConnectorError>;
    fn state_put(&self, scope: StateScope, key: &str, value: &str) -> Result<(), ConnectorError>;
    fn state_compare_and_set(
        &self,
        scope: StateScope,
        key: &str,
        expected: Option<&str>,
        new_value: &str,
    ) -> Result<bool, ConnectorError>;

    fn checkpoint(
        &self,
        connector_id: &str,
        stream_name: &str,
        cp: &Checkpoint,
    ) -> Result<(), ConnectorError>;
    fn metric(
        &self,
        connector_id: &str,
        stream_name: &str,
        m: &Metric,
    ) -> Result<(), ConnectorError>;
    fn emit_dlq_record(
        &self,
        stream_name: &str,
        record_json: &str,
        error_message: &str,
        error_category: ErrorCategory,
    ) -> Result<(), ConnectorError>;

    fn connect_tcp(&self, host: &str, port: u16) -> Result<u64, ConnectorError>;
    fn socket_read(&self, handle: u64, len: u64) -> Result<SocketReadResult, ConnectorError>;
    fn socket_write(&self, handle: u64, data: &[u8]) -> Result<SocketWriteResult, ConnectorError>;
    fn socket_close(&self, handle: u64);
}

/// FFI helper: convert `StateScope` to its integer representation for
/// host import calls. The types crate no longer carries this method
/// because FFI encoding belongs at the boundary.
#[cfg(target_arch = "wasm32")]
const fn state_scope_to_i32(scope: StateScope) -> i32 {
    match scope {
        StateScope::Pipeline => 0,
        StateScope::Stream => 1,
        StateScope::ConnectorInstance => 2,
    }
}

static HOST_IMPORTS: OnceLock<Box<dyn HostImports>> = OnceLock::new();

fn default_host_imports() -> Box<dyn HostImports> {
    #[cfg(target_arch = "wasm32")]
    {
        Box::new(WasmHostImports)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        Box::new(StubHostImports)
    }
}

fn host_imports() -> &'static dyn HostImports {
    HOST_IMPORTS.get_or_init(default_host_imports).as_ref()
}

/// Installs a custom host imports implementation.
///
/// This is primarily intended for tests and native simulation.
///
/// # Errors
///
/// Returns `Err` if the host imports have already been initialized.
pub fn set_host_imports(imports: Box<dyn HostImports>) -> Result<(), Box<dyn HostImports>> {
    HOST_IMPORTS.set(imports)
}

#[cfg(target_arch = "wasm32")]
fn from_component_error(
    err: bindings::rapidbyte::connector::types::ConnectorError,
) -> ConnectorError {
    use bindings::rapidbyte::connector::types as ct;

    ConnectorError {
        category: match err.category {
            ct::ErrorCategory::Config => ErrorCategory::Config,
            ct::ErrorCategory::Auth => ErrorCategory::Auth,
            ct::ErrorCategory::Permission => ErrorCategory::Permission,
            ct::ErrorCategory::RateLimit => ErrorCategory::RateLimit,
            ct::ErrorCategory::TransientNetwork => ErrorCategory::TransientNetwork,
            ct::ErrorCategory::TransientDb => ErrorCategory::TransientDb,
            ct::ErrorCategory::Data => ErrorCategory::Data,
            ct::ErrorCategory::Schema => ErrorCategory::Schema,
            ct::ErrorCategory::Internal => ErrorCategory::Internal,
            ct::ErrorCategory::Frame => ErrorCategory::Frame,
        },
        scope: match err.scope {
            ct::ErrorScope::PerStream => ErrorScope::Stream,
            ct::ErrorScope::PerBatch => ErrorScope::Batch,
            ct::ErrorScope::PerRecord => ErrorScope::Record,
        },
        code: err.code.into(),
        message: err.message,
        retryable: err.retryable,
        retry_after_ms: err.retry_after_ms,
        backoff_class: match err.backoff_class {
            ct::BackoffClass::Fast => BackoffClass::Fast,
            ct::BackoffClass::Normal => BackoffClass::Normal,
            ct::BackoffClass::Slow => BackoffClass::Slow,
        },
        safe_to_retry: err.safe_to_retry,
        commit_state: err.commit_state.map(|s| match s {
            ct::CommitState::BeforeCommit => CommitState::BeforeCommit,
            ct::CommitState::AfterCommitUnknown => CommitState::AfterCommitUnknown,
            ct::CommitState::AfterCommitConfirmed => CommitState::AfterCommitConfirmed,
        }),
        details: err
            .details_json
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok()),
    }
}

#[cfg(target_arch = "wasm32")]
pub struct WasmHostImports;

#[cfg(target_arch = "wasm32")]
impl HostImports for WasmHostImports {
    fn log(&self, level: i32, message: &str) {
        bindings::rapidbyte::connector::host::log(level as u32, message);
    }

    fn frame_new(&self, capacity: u64) -> Result<u64, ConnectorError> {
        bindings::rapidbyte::connector::host::frame_new(capacity).map_err(from_component_error)
    }

    fn frame_write(&self, handle: u64, chunk: &[u8]) -> Result<u64, ConnectorError> {
        bindings::rapidbyte::connector::host::frame_write(handle, chunk)
            .map_err(from_component_error)
    }

    fn frame_seal(&self, handle: u64) -> Result<(), ConnectorError> {
        bindings::rapidbyte::connector::host::frame_seal(handle).map_err(from_component_error)
    }

    fn frame_len(&self, handle: u64) -> Result<u64, ConnectorError> {
        bindings::rapidbyte::connector::host::frame_len(handle).map_err(from_component_error)
    }

    fn frame_read(&self, handle: u64, offset: u64, len: u64) -> Result<Vec<u8>, ConnectorError> {
        bindings::rapidbyte::connector::host::frame_read(handle, offset, len)
            .map_err(from_component_error)
    }

    fn frame_drop(&self, handle: u64) {
        bindings::rapidbyte::connector::host::frame_drop(handle);
    }

    fn emit_batch(&self, handle: u64) -> Result<(), ConnectorError> {
        bindings::rapidbyte::connector::host::emit_batch(handle).map_err(from_component_error)
    }

    fn next_batch(&self) -> Result<Option<u64>, ConnectorError> {
        bindings::rapidbyte::connector::host::next_batch().map_err(from_component_error)
    }

    fn state_get(&self, scope: StateScope, key: &str) -> Result<Option<String>, ConnectorError> {
        bindings::rapidbyte::connector::host::state_get(state_scope_to_i32(scope) as u32, key)
            .map_err(from_component_error)
    }

    fn state_put(&self, scope: StateScope, key: &str, value: &str) -> Result<(), ConnectorError> {
        bindings::rapidbyte::connector::host::state_put(
            state_scope_to_i32(scope) as u32,
            key,
            value,
        )
        .map_err(from_component_error)
    }

    fn state_compare_and_set(
        &self,
        scope: StateScope,
        key: &str,
        expected: Option<&str>,
        new_value: &str,
    ) -> Result<bool, ConnectorError> {
        bindings::rapidbyte::connector::host::state_cas(
            state_scope_to_i32(scope) as u32,
            key,
            expected,
            new_value,
        )
        .map_err(from_component_error)
    }

    fn checkpoint(
        &self,
        connector_id: &str,
        stream_name: &str,
        cp: &Checkpoint,
    ) -> Result<(), ConnectorError> {
        let kind = match cp.kind {
            CheckpointKind::Source => 0,
            CheckpointKind::Dest => 1,
            CheckpointKind::Transform => 2,
        };

        let envelope = PayloadEnvelope {
            protocol_version: ProtocolVersion::V4,
            connector_id: connector_id.to_string(),
            stream_name: stream_name.to_string(),
            payload: cp,
        };
        let payload_json = serde_json::to_string(&envelope)
            .map_err(|e| ConnectorError::internal("SERIALIZE_CHECKPOINT", e.to_string()))?;

        bindings::rapidbyte::connector::host::checkpoint(kind, &payload_json)
            .map_err(from_component_error)
    }

    fn metric(
        &self,
        connector_id: &str,
        stream_name: &str,
        m: &Metric,
    ) -> Result<(), ConnectorError> {
        let envelope = PayloadEnvelope {
            protocol_version: ProtocolVersion::V4,
            connector_id: connector_id.to_string(),
            stream_name: stream_name.to_string(),
            payload: m,
        };
        let payload_json = serde_json::to_string(&envelope)
            .map_err(|e| ConnectorError::internal("SERIALIZE_METRIC", e.to_string()))?;

        bindings::rapidbyte::connector::host::metric(&payload_json).map_err(from_component_error)
    }

    fn emit_dlq_record(
        &self,
        stream_name: &str,
        record_json: &str,
        error_message: &str,
        error_category: ErrorCategory,
    ) -> Result<(), ConnectorError> {
        bindings::rapidbyte::connector::host::emit_dlq_record(
            stream_name,
            record_json,
            error_message,
            &error_category.to_string(),
        )
        .map_err(from_component_error)
    }

    fn connect_tcp(&self, host: &str, port: u16) -> Result<u64, ConnectorError> {
        bindings::rapidbyte::connector::host::connect_tcp(host, port).map_err(from_component_error)
    }

    fn socket_read(&self, handle: u64, len: u64) -> Result<SocketReadResult, ConnectorError> {
        let result = bindings::rapidbyte::connector::host::socket_read(handle, len)
            .map_err(from_component_error)?;
        Ok(match result {
            bindings::rapidbyte::connector::types::SocketReadResult::Data(data) => {
                SocketReadResult::Data(data)
            }
            bindings::rapidbyte::connector::types::SocketReadResult::Eof => SocketReadResult::Eof,
            bindings::rapidbyte::connector::types::SocketReadResult::WouldBlock => {
                SocketReadResult::WouldBlock
            }
        })
    }

    fn socket_write(&self, handle: u64, data: &[u8]) -> Result<SocketWriteResult, ConnectorError> {
        let result = bindings::rapidbyte::connector::host::socket_write(handle, data)
            .map_err(from_component_error)?;
        Ok(match result {
            bindings::rapidbyte::connector::types::SocketWriteResult::Written(n) => {
                SocketWriteResult::Written(n)
            }
            bindings::rapidbyte::connector::types::SocketWriteResult::WouldBlock => {
                SocketWriteResult::WouldBlock
            }
        })
    }

    fn socket_close(&self, handle: u64) {
        bindings::rapidbyte::connector::host::socket_close(handle)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct StubHostImports;

#[cfg(not(target_arch = "wasm32"))]
impl HostImports for StubHostImports {
    fn log(&self, _level: i32, _message: &str) {}

    fn frame_new(&self, _capacity: u64) -> Result<u64, ConnectorError> {
        Ok(1)
    }

    fn frame_write(&self, _handle: u64, _chunk: &[u8]) -> Result<u64, ConnectorError> {
        Ok(0)
    }

    fn frame_seal(&self, _handle: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn frame_len(&self, _handle: u64) -> Result<u64, ConnectorError> {
        Ok(0)
    }

    fn frame_read(&self, _handle: u64, _offset: u64, _len: u64) -> Result<Vec<u8>, ConnectorError> {
        Ok(vec![])
    }

    fn frame_drop(&self, _handle: u64) {}

    fn emit_batch(&self, _handle: u64) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn next_batch(&self) -> Result<Option<u64>, ConnectorError> {
        Ok(None)
    }

    fn state_get(&self, _scope: StateScope, _key: &str) -> Result<Option<String>, ConnectorError> {
        Ok(None)
    }

    fn state_put(
        &self,
        _scope: StateScope,
        _key: &str,
        _value: &str,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn state_compare_and_set(
        &self,
        _scope: StateScope,
        _key: &str,
        _expected: Option<&str>,
        _new_value: &str,
    ) -> Result<bool, ConnectorError> {
        Ok(false)
    }

    fn checkpoint(
        &self,
        _connector_id: &str,
        _stream_name: &str,
        _cp: &Checkpoint,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn metric(
        &self,
        _connector_id: &str,
        _stream_name: &str,
        _m: &Metric,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn emit_dlq_record(
        &self,
        _stream_name: &str,
        _record_json: &str,
        _error_message: &str,
        _error_category: ErrorCategory,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn connect_tcp(&self, _host: &str, _port: u16) -> Result<u64, ConnectorError> {
        Err(ConnectorError::internal("STUB", "No-op stub"))
    }

    fn socket_read(&self, _handle: u64, _len: u64) -> Result<SocketReadResult, ConnectorError> {
        Ok(SocketReadResult::Eof)
    }

    fn socket_write(&self, _handle: u64, data: &[u8]) -> Result<SocketWriteResult, ConnectorError> {
        Ok(SocketWriteResult::Written(data.len() as u64))
    }

    fn socket_close(&self, _handle: u64) {}
}

pub fn log(level: i32, message: &str) {
    host_imports().log(level, message)
}

/// Emit an Arrow RecordBatch to the host pipeline via V3 frame transport.
///
/// Streams IPC encoding directly into a host frame via `FrameWriter`,
/// eliminating the guest-side `Vec<u8>` IPC buffer allocation.
///
/// # Errors
///
/// Returns `Err` if frame creation, IPC encoding, or frame sealing fails.
pub fn emit_batch(batch: &RecordBatch) -> Result<(), ConnectorError> {
    let imports = host_imports();

    let capacity = batch.get_array_memory_size() as u64 + 1024;
    let handle = imports.frame_new(capacity)?;

    // Stream IPC directly into host frame -- no guest Vec<u8>
    {
        let mut writer = crate::frame_writer::FrameWriter::new(handle, imports);
        let mut ipc_writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut writer, batch.schema().as_ref())
                .map_err(|e| {
                    ConnectorError::internal("ARROW_IPC_ENCODE", format!("IPC writer init: {e}"))
                })?;
        ipc_writer
            .write(batch)
            .map_err(|e| ConnectorError::internal("ARROW_IPC_ENCODE", format!("IPC write: {e}")))?;
        ipc_writer.finish().map_err(|e| {
            ConnectorError::internal("ARROW_IPC_ENCODE", format!("IPC finish: {e}"))
        })?;
    }

    imports.frame_seal(handle)?;
    imports.emit_batch(handle)?;
    Ok(())
}

fn decode_next_batch_frame(
    ipc_bytes: &[u8],
    frame_len: u64,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), ConnectorError> {
    crate::arrow::ipc::decode_ipc(ipc_bytes).map_err(|e| {
        ConnectorError::internal(
            "NEXT_BATCH_DECODE",
            format!("failed to decode next_batch frame (frame_len={frame_len}): {e}"),
        )
    })
}

/// Receive the next Arrow RecordBatch from the host pipeline.
///
/// Returns `None` when there are no more batches.
///
/// # Errors
///
/// Returns `Err` if frame reading or IPC decoding fails.
#[allow(clippy::type_complexity)]
pub fn next_batch(
    max_bytes: u64,
) -> Result<Option<(Arc<Schema>, Vec<RecordBatch>)>, ConnectorError> {
    let imports = host_imports();

    let Some(handle) = imports.next_batch()? else {
        return Ok(None);
    };

    let frame_len = imports.frame_len(handle)?;
    if frame_len > max_bytes {
        imports.frame_drop(handle);
        return Err(ConnectorError::internal(
            "BATCH_TOO_LARGE",
            format!("Batch {frame_len} exceeds max {max_bytes}"),
        ));
    }

    // Read entire frame (inherent WIT boundary copy)
    let ipc_bytes = imports.frame_read(handle, 0, frame_len)?;
    imports.frame_drop(handle);

    let (schema, batches) = decode_next_batch_frame(&ipc_bytes, frame_len)?;
    Ok(Some((schema, batches)))
}

/// Retrieve a value from the host state backend.
///
/// # Errors
///
/// Returns `Err` if the host state backend rejects the read.
pub fn state_get(scope: StateScope, key: &str) -> Result<Option<String>, ConnectorError> {
    host_imports().state_get(scope, key)
}

/// Store a value in the host state backend.
///
/// # Errors
///
/// Returns `Err` if the host state backend rejects the write.
pub fn state_put(scope: StateScope, key: &str, value: &str) -> Result<(), ConnectorError> {
    host_imports().state_put(scope, key, value)
}

/// Atomically compare-and-set a value in the host state backend.
///
/// # Errors
///
/// Returns `Err` if the host state backend rejects the CAS operation.
pub fn state_compare_and_set(
    scope: StateScope,
    key: &str,
    expected: Option<&str>,
    new_value: &str,
) -> Result<bool, ConnectorError> {
    host_imports().state_compare_and_set(scope, key, expected, new_value)
}

/// Submit a checkpoint to the host runtime.
///
/// # Errors
///
/// Returns `Err` if the host rejects the checkpoint.
pub fn checkpoint(
    connector_id: &str,
    stream_name: &str,
    cp: &Checkpoint,
) -> Result<(), ConnectorError> {
    host_imports().checkpoint(connector_id, stream_name, cp)
}

/// Emit a metric to the host runtime.
///
/// # Errors
///
/// Returns `Err` if metric emission fails.
pub fn metric(connector_id: &str, stream_name: &str, m: &Metric) -> Result<(), ConnectorError> {
    host_imports().metric(connector_id, stream_name, m)
}

/// Emit a dead-letter queue record to the host runtime.
///
/// # Errors
///
/// Returns `Err` if DLQ record emission fails.
pub fn emit_dlq_record(
    stream_name: &str,
    record_json: &str,
    error_message: &str,
    error_category: ErrorCategory,
) -> Result<(), ConnectorError> {
    host_imports().emit_dlq_record(stream_name, record_json, error_message, error_category)
}

/// Open a TCP connection through the host runtime.
///
/// # Errors
///
/// Returns `Err` if the host denies the connection or TCP connect fails.
pub fn connect_tcp(host: &str, port: u16) -> Result<u64, ConnectorError> {
    host_imports().connect_tcp(host, port)
}

/// Read data from a host-managed socket.
///
/// # Errors
///
/// Returns `Err` if the socket read operation fails.
pub fn socket_read(handle: u64, len: u64) -> Result<SocketReadResult, ConnectorError> {
    host_imports().socket_read(handle, len)
}

/// Write data to a host-managed socket.
///
/// # Errors
///
/// Returns `Err` if the socket write operation fails.
pub fn socket_write(handle: u64, data: &[u8]) -> Result<SocketWriteResult, ConnectorError> {
    host_imports().socket_write(handle, data)
}

pub fn socket_close(handle: u64) {
    host_imports().socket_close(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stub_socket_variants_construct() {
        let _ = SocketReadResult::Eof;
        let _ = SocketReadResult::WouldBlock;
        let _ = SocketWriteResult::WouldBlock;
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_native_stub_next_batch_none() {
        let result = next_batch(1024).expect("next batch");
        assert!(result.is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_emit_batch_encodes_and_calls_host() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

        let result = emit_batch(&batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_decode_next_batch_frame_invalid_payload_includes_context() {
        let err = decode_next_batch_frame(&[1, 2, 3], 3).expect_err("invalid ipc should fail");
        assert_eq!(err.code, "NEXT_BATCH_DECODE");
        assert!(err.message.contains("frame_len=3"));
    }
}
