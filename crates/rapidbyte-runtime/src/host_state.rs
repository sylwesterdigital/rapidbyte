//! Host-side runtime state and host import implementations for connector components.
//!
//! The `*_impl` methods mirror the WIT-generated calling convention (owned `String` / `Vec<u8>`),
//! and inner types are consumed by the bindings module.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use tokio::sync::mpsc;
use wasmtime::component::ResourceTable;
use wasmtime::StoreLimits;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use rapidbyte_state::StateBackend;
use rapidbyte_types::checkpoint::{Checkpoint, CheckpointKind, StateScope};
use rapidbyte_types::envelope::{DlqRecord, Timestamp};
use rapidbyte_types::error::{ErrorCategory, PluginError};
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::metric::{Metric, MetricValue};
use rapidbyte_types::state::{CursorState, PipelineId, StreamName};

use crate::acl::{derive_network_acl, NetworkAcl};
use crate::compression::CompressionCodec;
use crate::engine::HasStoreLimits;
use crate::frame::FrameTable;
use crate::sandbox::{build_store_limits, build_wasi_ctx, SandboxOverrides};
use crate::socket::{
    resolve_socket_addrs, should_activate_socket_poll, SocketEntry, SocketReadResult,
    SocketWriteResult,
};
#[cfg(unix)]
use crate::socket::{socket_poll_timeout_ms, wait_socket_ready, SocketInterest};

const MAX_SOCKET_READ_BYTES: u64 = 64 * 1024;
const MAX_STATE_KEY_LEN: usize = 1024;

/// Default maximum DLQ records kept in memory per run.
pub const DEFAULT_DLQ_LIMIT: usize = 10_000;

/// Channel frame type for batch routing between connector stages.
pub enum Frame {
    /// IPC-encoded Arrow `RecordBatch` (optionally compressed).
    Data(bytes::Bytes),
    /// End-of-stream marker.
    EndStream,
}

/// Cumulative timing counters for host function calls.
#[derive(Debug, Clone, Default)]
pub struct HostTimings {
    pub emit_batch_nanos: u64,
    pub next_batch_nanos: u64,
    pub next_batch_wait_nanos: u64,
    pub next_batch_process_nanos: u64,
    pub compress_nanos: u64,
    pub decompress_nanos: u64,
    pub emit_batch_count: u64,
    pub next_batch_count: u64,
    pub source_connect_secs: f64,
    pub source_query_secs: f64,
    pub source_fetch_secs: f64,
    pub source_arrow_encode_secs: f64,
    pub dest_connect_secs: f64,
    pub dest_flush_secs: f64,
    pub dest_commit_secs: f64,
    pub dest_arrow_decode_secs: f64,
}

// --- Inner types ---

pub(crate) struct ConnectorIdentity {
    pub pipeline: PipelineId,
    pub connector_id: String,
    pub stream: StreamName,
    pub state_backend: Arc<dyn StateBackend>,
}

pub(crate) struct BatchRouter {
    pub sender: Option<mpsc::Sender<Frame>>,
    pub receiver: Option<mpsc::Receiver<Frame>>,
    pub next_batch_id: u64,
    pub compression: Option<CompressionCodec>,
    /// Optional callback invoked after each `emit-batch` with the payload byte size.
    pub on_emit: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

pub(crate) struct CheckpointCollector {
    pub source: Arc<Mutex<Vec<Checkpoint>>>,
    pub dest: Arc<Mutex<Vec<Checkpoint>>>,
    pub dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    pub timings: Arc<Mutex<HostTimings>>,
    pub dlq_limit: usize,
}

pub(crate) struct SocketManager {
    pub acl: NetworkAcl,
    pub sockets: HashMap<u64, SocketEntry>,
    pub next_handle: u64,
}

/// Shared state passed to Wasmtime component host imports.
pub struct ComponentHostState {
    pub(crate) identity: ConnectorIdentity,
    pub(crate) batch: BatchRouter,
    pub(crate) checkpoints: CheckpointCollector,
    pub(crate) sockets: SocketManager,
    pub(crate) frames: FrameTable,
    pub(crate) store_limits: StoreLimits,
    ctx: WasiCtx,
    table: ResourceTable,
}

// --- Builder ---

/// Builder for [`ComponentHostState`].
pub struct HostStateBuilder {
    pipeline: Option<String>,
    connector_id: Option<String>,
    stream: Option<String>,
    state_backend: Option<Arc<dyn StateBackend>>,
    sender: Option<mpsc::Sender<Frame>>,
    receiver: Option<mpsc::Receiver<Frame>>,
    source_checkpoints: Option<Arc<Mutex<Vec<Checkpoint>>>>,
    dest_checkpoints: Option<Arc<Mutex<Vec<Checkpoint>>>>,
    dlq_records: Option<Arc<Mutex<Vec<DlqRecord>>>>,
    timings: Option<Arc<Mutex<HostTimings>>>,
    permissions: Option<Permissions>,
    config: serde_json::Value,
    compression: Option<CompressionCodec>,
    overrides: Option<SandboxOverrides>,
    dlq_limit: usize,
    on_emit: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

impl HostStateBuilder {
    fn new() -> Self {
        Self {
            pipeline: None,
            connector_id: None,
            stream: None,
            state_backend: None,
            sender: None,
            receiver: None,
            source_checkpoints: None,
            dest_checkpoints: None,
            dlq_records: None,
            timings: None,
            permissions: None,
            config: serde_json::Value::Null,
            compression: None,
            overrides: None,
            dlq_limit: DEFAULT_DLQ_LIMIT,
            on_emit: None,
        }
    }

    #[must_use]
    pub fn pipeline(mut self, name: impl Into<String>) -> Self {
        self.pipeline = Some(name.into());
        self
    }

    #[must_use]
    pub fn connector_id(mut self, id: impl Into<String>) -> Self {
        self.connector_id = Some(id.into());
        self
    }

    #[must_use]
    pub fn stream(mut self, name: impl Into<String>) -> Self {
        self.stream = Some(name.into());
        self
    }

    #[must_use]
    pub fn state_backend(mut self, backend: Arc<dyn StateBackend>) -> Self {
        self.state_backend = Some(backend);
        self
    }

    #[must_use]
    pub fn sender(mut self, tx: mpsc::Sender<Frame>) -> Self {
        self.sender = Some(tx);
        self
    }

    #[must_use]
    pub fn receiver(mut self, rx: mpsc::Receiver<Frame>) -> Self {
        self.receiver = Some(rx);
        self
    }

    #[must_use]
    pub fn source_checkpoints(mut self, cp: Arc<Mutex<Vec<Checkpoint>>>) -> Self {
        self.source_checkpoints = Some(cp);
        self
    }

    #[must_use]
    pub fn dest_checkpoints(mut self, cp: Arc<Mutex<Vec<Checkpoint>>>) -> Self {
        self.dest_checkpoints = Some(cp);
        self
    }

    #[must_use]
    pub fn dlq_records(mut self, records: Arc<Mutex<Vec<DlqRecord>>>) -> Self {
        self.dlq_records = Some(records);
        self
    }

    #[must_use]
    pub fn timings(mut self, t: Arc<Mutex<HostTimings>>) -> Self {
        self.timings = Some(t);
        self
    }

    #[must_use]
    pub fn permissions(mut self, perms: &Permissions) -> Self {
        self.permissions = Some(perms.clone());
        self
    }

    #[must_use]
    pub fn config(mut self, cfg: &serde_json::Value) -> Self {
        self.config = cfg.clone();
        self
    }

    #[must_use]
    pub fn compression(mut self, codec: Option<CompressionCodec>) -> Self {
        self.compression = codec;
        self
    }

    #[must_use]
    pub fn overrides(mut self, ovr: &SandboxOverrides) -> Self {
        self.overrides = Some(ovr.clone());
        self
    }

    #[must_use]
    pub fn dlq_limit(mut self, limit: usize) -> Self {
        self.dlq_limit = limit;
        self
    }

    #[must_use]
    pub fn on_emit(mut self, cb: Arc<dyn Fn(u64) + Send + Sync>) -> Self {
        self.on_emit = Some(cb);
        self
    }

    /// Build the host state. Fails if required fields are missing or WASI context fails.
    ///
    /// # Errors
    ///
    /// Returns an error if the WASI context cannot be constructed (e.g. invalid preopens).
    ///
    pub fn build(self) -> Result<ComponentHostState> {
        let pipeline = self
            .pipeline
            .ok_or_else(|| anyhow::anyhow!("pipeline is required"))?;
        let connector_id = self
            .connector_id
            .ok_or_else(|| anyhow::anyhow!("connector_id is required"))?;
        let stream = self
            .stream
            .ok_or_else(|| anyhow::anyhow!("stream is required"))?;
        let state_backend = self
            .state_backend
            .ok_or_else(|| anyhow::anyhow!("state_backend is required"))?;

        Ok(ComponentHostState {
            identity: ConnectorIdentity {
                pipeline: PipelineId::new(pipeline),
                connector_id,
                stream: StreamName::new(stream),
                state_backend,
            },
            batch: BatchRouter {
                sender: self.sender,
                receiver: self.receiver,
                next_batch_id: 1,
                compression: self.compression,
                on_emit: self.on_emit,
            },
            checkpoints: CheckpointCollector {
                source: self.source_checkpoints.unwrap_or_default(),
                dest: self.dest_checkpoints.unwrap_or_default(),
                dlq_records: self.dlq_records.unwrap_or_default(),
                timings: self
                    .timings
                    .unwrap_or_else(|| Arc::new(Mutex::new(HostTimings::default()))),
                dlq_limit: self.dlq_limit,
            },
            sockets: SocketManager {
                acl: derive_network_acl(
                    self.permissions.as_ref(),
                    &self.config,
                    self.overrides
                        .as_ref()
                        .and_then(|o| o.allowed_hosts.as_deref()),
                ),
                sockets: HashMap::new(),
                next_handle: 1,
            },
            frames: FrameTable::new(),
            store_limits: build_store_limits(self.overrides.as_ref()),
            ctx: build_wasi_ctx(self.permissions.as_ref(), self.overrides.as_ref())?,
            table: ResourceTable::new(),
        })
    }
}

// The `_impl` methods intentionally take owned `String`/`Vec<u8>` to match the
// WIT-generated calling convention used by the bindings module.
#[allow(clippy::needless_pass_by_value)]
impl ComponentHostState {
    /// Create a builder for `ComponentHostState`.
    #[must_use]
    pub fn builder() -> HostStateBuilder {
        HostStateBuilder::new()
    }

    /// Max frame capacity: 512 MB. Prevents guest-induced OOM.
    const MAX_FRAME_CAPACITY: u64 = 512 * 1024 * 1024;

    fn current_stream(&self) -> &str {
        self.identity.stream.as_str()
    }

    // ── Frame lifecycle host imports ────────────────────────────────

    pub(crate) fn frame_new_impl(&mut self, capacity: u64) -> u64 {
        let clamped = capacity.min(Self::MAX_FRAME_CAPACITY);
        if capacity > Self::MAX_FRAME_CAPACITY {
            tracing::warn!(
                requested = capacity,
                clamped = clamped,
                "frame-new capacity clamped to MAX_FRAME_CAPACITY"
            );
        }
        self.frames.alloc(clamped)
    }

    pub(crate) fn frame_write_impl(
        &mut self,
        handle: u64,
        chunk: Vec<u8>,
    ) -> Result<u64, PluginError> {
        self.frames
            .write(handle, &chunk)
            .map_err(|e| PluginError::frame("FRAME_WRITE_FAILED", e.to_string()))
    }

    pub(crate) fn frame_seal_impl(&mut self, handle: u64) -> Result<(), PluginError> {
        self.frames
            .seal(handle)
            .map_err(|e| PluginError::frame("FRAME_SEAL_FAILED", e.to_string()))
    }

    pub(crate) fn frame_len_impl(&mut self, handle: u64) -> Result<u64, PluginError> {
        self.frames
            .len(handle)
            .map_err(|e| PluginError::frame("FRAME_LEN_FAILED", e.to_string()))
    }

    pub(crate) fn frame_read_impl(
        &mut self,
        handle: u64,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, PluginError> {
        self.frames
            .read(handle, offset, len)
            .map_err(|e| PluginError::frame("FRAME_READ_FAILED", e.to_string()))
    }

    pub(crate) fn frame_drop_impl(&mut self, handle: u64) {
        self.frames.drop_frame(handle);
    }

    // ── Host import implementations ─────────────────────────────────

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn emit_batch_impl(&mut self, handle: u64) -> Result<(), PluginError> {
        let fn_start = Instant::now();

        // Consume the sealed frame -> Bytes (zero-copy)
        let payload = self
            .frames
            .consume(handle)
            .map_err(|e| PluginError::frame("EMIT_BATCH_FAILED", e.to_string()))?;

        if payload.is_empty() {
            return Err(PluginError::internal(
                "EMPTY_BATCH",
                "Connector emitted a zero-length batch; this is a protocol violation",
            ));
        }

        let (payload, compress_elapsed_nanos): (bytes::Bytes, u64) =
            if let Some(codec) = self.batch.compression {
                let start = Instant::now();
                let compressed = crate::compression::compress_bytes(codec, &payload)
                    .map_err(|e| PluginError::internal("COMPRESS_FAILED", e.to_string()))?;
                (compressed, start.elapsed().as_nanos() as u64)
            } else {
                (payload, 0)
            };

        let sender =
            self.batch.sender.as_ref().ok_or_else(|| {
                PluginError::internal("NO_SENDER", "No batch sender configured")
            })?;

        let payload_len = payload.len() as u64;

        sender
            .blocking_send(Frame::Data(payload))
            .map_err(|e| PluginError::internal("CHANNEL_SEND", e.to_string()))?;

        if let Some(cb) = &self.batch.on_emit {
            cb(payload_len);
        }

        self.batch.next_batch_id += 1;

        let mut t = lock_mutex(&self.checkpoints.timings, "timings")?;
        t.emit_batch_nanos += fn_start.elapsed().as_nanos() as u64;
        t.emit_batch_count += 1;
        t.compress_nanos += compress_elapsed_nanos;

        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn next_batch_impl(&mut self) -> Result<Option<u64>, PluginError> {
        let fn_start = Instant::now();

        let receiver = self.batch.receiver.as_mut().ok_or_else(|| {
            PluginError::internal("NO_RECEIVER", "No batch receiver configured")
        })?;

        let wait_start = Instant::now();
        let Some(frame) = receiver.blocking_recv() else {
            return Ok(None);
        };
        let wait_elapsed_nanos = wait_start.elapsed().as_nanos() as u64;

        let payload = match frame {
            Frame::Data(payload) => payload,
            Frame::EndStream => return Ok(None),
        };

        let (payload, decompress_elapsed_nanos): (bytes::Bytes, u64) =
            if let Some(codec) = self.batch.compression {
                let start = Instant::now();
                let decompressed = crate::compression::decompress(codec, &payload)
                    .map_err(|e| PluginError::internal("DECOMPRESS_FAILED", e.to_string()))?
                    .into();
                (decompressed, start.elapsed().as_nanos() as u64)
            } else {
                (payload, 0) // Bytes, zero-copy
            };

        // Insert as sealed read-only frame
        let handle = self.frames.insert_sealed(payload);

        let mut t = lock_mutex(&self.checkpoints.timings, "timings")?;
        let total_elapsed_nanos = fn_start.elapsed().as_nanos() as u64;
        t.next_batch_nanos += total_elapsed_nanos;
        t.next_batch_wait_nanos += wait_elapsed_nanos;
        t.next_batch_process_nanos += total_elapsed_nanos.saturating_sub(wait_elapsed_nanos);
        t.next_batch_count += 1;
        t.decompress_nanos += decompress_elapsed_nanos;

        Ok(Some(handle))
    }

    fn scoped_state_key(&self, scope: StateScope, key: &str) -> String {
        match scope {
            StateScope::Pipeline => key.to_string(),
            StateScope::Stream => format!("{}:{}", self.current_stream(), key),
            StateScope::ConnectorInstance => format!("{}:{}", self.identity.connector_id, key),
        }
    }

    pub(crate) fn state_get_impl(
        &mut self,
        scope: u32,
        key: String,
    ) -> Result<Option<String>, PluginError> {
        validate_state_key(&key)?;
        let scope = parse_state_scope(scope)?;
        let scoped_key = self.scoped_state_key(scope, &key);
        self.identity
            .state_backend
            .get_cursor(&self.identity.pipeline, &StreamName::new(scoped_key))
            .map_err(|e| PluginError::internal("STATE_BACKEND", e.to_string()))
            .map(|opt| opt.and_then(|cursor| cursor.cursor_value))
    }

    pub(crate) fn state_put_impl(
        &mut self,
        scope: u32,
        key: String,
        value: String,
    ) -> Result<(), PluginError> {
        validate_state_key(&key)?;
        let scope = parse_state_scope(scope)?;
        let scoped_key = self.scoped_state_key(scope, &key);
        let cursor = CursorState {
            cursor_field: Some(key),
            cursor_value: Some(value),
            updated_at: Utc::now().to_rfc3339(),
        };

        self.identity
            .state_backend
            .set_cursor(
                &self.identity.pipeline,
                &StreamName::new(scoped_key),
                &cursor,
            )
            .map_err(|e| PluginError::internal("STATE_BACKEND", e.to_string()))
    }

    pub(crate) fn state_cas_impl(
        &mut self,
        scope: u32,
        key: String,
        expected: Option<String>,
        new_value: String,
    ) -> Result<bool, PluginError> {
        validate_state_key(&key)?;
        let scope = parse_state_scope(scope)?;
        let scoped_key = self.scoped_state_key(scope, &key);
        self.identity
            .state_backend
            .compare_and_set(
                &self.identity.pipeline,
                &StreamName::new(scoped_key),
                expected.as_deref(),
                &new_value,
            )
            .map_err(|e| PluginError::internal("STATE_BACKEND", e.to_string()))
    }

    pub(crate) fn checkpoint_impl(
        &mut self,
        kind: u32,
        payload_json: String,
    ) -> Result<(), PluginError> {
        let envelope: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|e| PluginError::internal("PARSE_CHECKPOINT", e.to_string()))?;

        tracing::debug!(
            pipeline = self.identity.pipeline.as_str(),
            stream = %self.current_stream(),
            "Received checkpoint: {}",
            payload_json
        );

        let checkpoint_kind = CheckpointKind::try_from(kind).map_err(|k| {
            PluginError::config(
                "INVALID_CHECKPOINT_KIND",
                format!("Invalid checkpoint kind: {k}"),
            )
        })?;

        let payload = match envelope {
            serde_json::Value::Object(mut map) => map
                .remove("payload")
                .unwrap_or(serde_json::Value::Object(map)),
            other => other,
        };
        match checkpoint_kind {
            CheckpointKind::Source => {
                if let Ok(cp) = serde_json::from_value::<Checkpoint>(payload) {
                    lock_mutex(&self.checkpoints.source, "source_checkpoints")?.push(cp);
                }
            }
            CheckpointKind::Dest => {
                if let Ok(cp) = serde_json::from_value::<Checkpoint>(payload) {
                    lock_mutex(&self.checkpoints.dest, "dest_checkpoints")?.push(cp);
                }
            }
            CheckpointKind::Transform => {
                tracing::debug!(
                    pipeline = self.identity.pipeline.as_str(),
                    stream = %self.current_stream(),
                    "Received transform checkpoint"
                );
            }
        }

        Ok(())
    }

    pub(crate) fn metric_impl(&mut self, payload_json: String) -> Result<(), PluginError> {
        let metric_json: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|e| PluginError::internal("PARSE_METRIC", e.to_string()))?;

        tracing::debug!(
            pipeline = self.identity.pipeline.as_str(),
            stream = %self.current_stream(),
            "Received metric: {}",
            payload_json
        );

        let payload = match metric_json {
            serde_json::Value::Object(mut map) => map
                .remove("payload")
                .unwrap_or(serde_json::Value::Object(map)),
            other => other,
        };
        let metric = serde_json::from_value::<Metric>(payload)
            .map_err(|e| PluginError::internal("PARSE_METRIC", e.to_string()))?;

        let value = match metric.value {
            MetricValue::Gauge(v) | MetricValue::Histogram(v) => v,
            #[allow(clippy::cast_precision_loss)]
            MetricValue::Counter(v) => v as f64,
            _ => return Ok(()),
        };

        let mut timings = lock_mutex(&self.checkpoints.timings, "timings")?;
        match metric.name.as_str() {
            "source_connect_secs" => timings.source_connect_secs += value,
            "source_query_secs" => timings.source_query_secs += value,
            "source_fetch_secs" => timings.source_fetch_secs += value,
            "source_arrow_encode_secs" => timings.source_arrow_encode_secs += value,
            "dest_connect_secs" => timings.dest_connect_secs += value,
            "dest_flush_secs" => timings.dest_flush_secs += value,
            "dest_commit_secs" => timings.dest_commit_secs += value,
            "dest_arrow_decode_secs" => timings.dest_arrow_decode_secs += value,
            _ => {}
        }

        Ok(())
    }

    pub(crate) fn log_impl(&mut self, level: u32, msg: String) {
        let pipeline = self.identity.pipeline.as_str();
        let stream = self.current_stream();

        match level {
            0 => tracing::error!(pipeline, stream = %stream, "[connector] {}", msg),
            1 => tracing::warn!(pipeline, stream = %stream, "[connector] {}", msg),
            2 => tracing::info!(pipeline, stream = %stream, "[connector] {}", msg),
            3 => tracing::debug!(pipeline, stream = %stream, "[connector] {}", msg),
            _ => tracing::trace!(pipeline, stream = %stream, "[connector] {}", msg),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn connect_tcp_impl(
        &mut self,
        host: String,
        port: u16,
    ) -> Result<u64, PluginError> {
        if !self.sockets.acl.allows(&host) {
            return Err(PluginError::permission(
                "NETWORK_DENIED",
                format!("Host '{host}' is not allowed by connector permissions"),
            ));
        }

        let addrs = resolve_socket_addrs(&host, port).map_err(|e| {
            PluginError::transient_network("DNS_RESOLUTION_FAILED", e.to_string())
        })?;

        let mut last_error: Option<(SocketAddr, std::io::Error)> = None;
        let mut connected: Option<TcpStream> = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, Duration::from_secs(10)) {
                Ok(stream) => {
                    connected = Some(stream);
                    break;
                }
                Err(err) => {
                    last_error = Some((addr, err));
                }
            }
        }

        let stream = connected.ok_or_else(|| {
            let details = last_error.map_or_else(
                || "no resolved addresses available".to_string(),
                |(addr, err)| format!("last attempt {addr} failed: {err}"),
            );
            PluginError::transient_network(
                "TCP_CONNECT_FAILED",
                format!("{host}:{port} ({details})"),
            )
        })?;

        stream
            .set_nonblocking(true)
            .map_err(|e| PluginError::internal("SOCKET_CONFIG", e.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|e| PluginError::internal("SOCKET_CONFIG", e.to_string()))?;

        let handle = self.sockets.next_handle;
        self.sockets.next_handle = self.sockets.next_handle.wrapping_add(1);
        self.sockets.sockets.insert(
            handle,
            SocketEntry {
                stream,
                read_would_block_streak: 0,
                write_would_block_streak: 0,
            },
        );

        Ok(handle)
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn socket_read_impl(
        &mut self,
        handle: u64,
        len: u64,
    ) -> Result<SocketReadResult, PluginError> {
        let entry =
            self.sockets.sockets.get_mut(&handle).ok_or_else(|| {
                PluginError::internal("INVALID_SOCKET", "Invalid socket handle")
            })?;

        let read_len = len.clamp(1, MAX_SOCKET_READ_BYTES) as usize;
        let mut buf = vec![0u8; read_len];
        match entry.stream.read(&mut buf) {
            Ok(0) => {
                entry.read_would_block_streak = 0;
                Ok(SocketReadResult::Eof)
            }
            Ok(n) => {
                entry.read_would_block_streak = 0;
                buf.truncate(n);
                Ok(SocketReadResult::Data(buf))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if !should_activate_socket_poll(&mut entry.read_would_block_streak) {
                    return Ok(SocketReadResult::WouldBlock);
                }

                #[cfg(unix)]
                let ready = match wait_socket_ready(
                    &entry.stream,
                    SocketInterest::Read,
                    socket_poll_timeout_ms(),
                ) {
                    Ok(ready) => ready,
                    Err(poll_err) => {
                        tracing::warn!(handle, error = %poll_err, "poll() failed in socket_read");
                        true
                    }
                };
                #[cfg(not(unix))]
                let ready = false;

                if ready {
                    match entry.stream.read(&mut buf) {
                        Ok(0) => {
                            entry.read_would_block_streak = 0;
                            Ok(SocketReadResult::Eof)
                        }
                        Ok(n) => {
                            entry.read_would_block_streak = 0;
                            buf.truncate(n);
                            Ok(SocketReadResult::Data(buf))
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            Ok(SocketReadResult::WouldBlock)
                        }
                        Err(e) => Err(PluginError::transient_network(
                            "SOCKET_READ_FAILED",
                            e.to_string(),
                        )),
                    }
                } else {
                    tracing::trace!(handle, "socket_read: WouldBlock after poll timeout");
                    Ok(SocketReadResult::WouldBlock)
                }
            }
            Err(e) => Err(PluginError::transient_network(
                "SOCKET_READ_FAILED",
                e.to_string(),
            )),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn socket_write_impl(
        &mut self,
        handle: u64,
        data: Vec<u8>,
    ) -> Result<SocketWriteResult, PluginError> {
        let entry =
            self.sockets.sockets.get_mut(&handle).ok_or_else(|| {
                PluginError::internal("INVALID_SOCKET", "Invalid socket handle")
            })?;

        match entry.stream.write(&data) {
            Ok(n) => {
                entry.write_would_block_streak = 0;
                Ok(SocketWriteResult::Written(n as u64))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if !should_activate_socket_poll(&mut entry.write_would_block_streak) {
                    return Ok(SocketWriteResult::WouldBlock);
                }

                #[cfg(unix)]
                let ready = match wait_socket_ready(
                    &entry.stream,
                    SocketInterest::Write,
                    socket_poll_timeout_ms(),
                ) {
                    Ok(ready) => ready,
                    Err(poll_err) => {
                        tracing::warn!(handle, error = %poll_err, "poll() failed in socket_write");
                        true
                    }
                };
                #[cfg(not(unix))]
                let ready = false;

                if ready {
                    match entry.stream.write(&data) {
                        Ok(n) => {
                            entry.write_would_block_streak = 0;
                            Ok(SocketWriteResult::Written(n as u64))
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            Ok(SocketWriteResult::WouldBlock)
                        }
                        Err(e) => Err(PluginError::transient_network(
                            "SOCKET_WRITE_FAILED",
                            e.to_string(),
                        )),
                    }
                } else {
                    tracing::trace!(handle, "socket_write: WouldBlock after poll timeout");
                    Ok(SocketWriteResult::WouldBlock)
                }
            }
            Err(e) => Err(PluginError::transient_network(
                "SOCKET_WRITE_FAILED",
                e.to_string(),
            )),
        }
    }

    pub(crate) fn socket_close_impl(&mut self, handle: u64) {
        self.sockets.sockets.remove(&handle);
    }

    pub(crate) fn emit_dlq_record_impl(
        &mut self,
        stream_name: String,
        record_json: String,
        error_message: String,
        error_category: String,
    ) -> Result<(), PluginError> {
        let mut dlq = lock_mutex(&self.checkpoints.dlq_records, "dlq_records")?;
        if dlq.len() >= self.checkpoints.dlq_limit {
            tracing::warn!(
                max = self.checkpoints.dlq_limit,
                "DLQ record cap reached; dropping further records"
            );
            return Ok(());
        }
        dlq.push(DlqRecord {
            stream_name,
            record_json,
            error_message,
            error_category: error_category
                .parse::<ErrorCategory>()
                .unwrap_or(ErrorCategory::Internal),
            failed_at: Timestamp::new(Utc::now().to_rfc3339()),
        });
        Ok(())
    }
}

// --- HasStoreLimits ---

impl HasStoreLimits for ComponentHostState {
    fn store_limits(&mut self) -> &mut StoreLimits {
        &mut self.store_limits
    }
}

// --- WasiView ---

impl WasiView for ComponentHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

// --- Helpers ---

fn lock_mutex<'a, T>(
    mutex: &'a Mutex<T>,
    name: &str,
) -> std::result::Result<MutexGuard<'a, T>, PluginError> {
    mutex
        .lock()
        .map_err(|_| PluginError::internal("MUTEX_POISONED", format!("{name} mutex poisoned")))
}

fn validate_state_key(key: &str) -> Result<(), PluginError> {
    if key.len() > MAX_STATE_KEY_LEN {
        return Err(PluginError::config(
            "KEY_TOO_LONG",
            format!("Key length {} exceeds {MAX_STATE_KEY_LEN}", key.len()),
        ));
    }
    Ok(())
}

fn parse_state_scope(scope: u32) -> Result<StateScope, PluginError> {
    match scope {
        0 => Ok(StateScope::Pipeline),
        1 => Ok(StateScope::Stream),
        2 => Ok(StateScope::ConnectorInstance),
        _ => Err(PluginError::config(
            "INVALID_SCOPE",
            format!("Invalid scope: {scope}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_state::SqliteStateBackend;

    fn test_host_state() -> ComponentHostState {
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        ComponentHostState::builder()
            .pipeline("test-pipeline")
            .connector_id("source-postgres")
            .stream("users")
            .state_backend(state)
            .build()
            .unwrap()
    }

    #[test]
    fn builder_creates_valid_state() {
        let host = test_host_state();
        assert_eq!(host.identity.pipeline.as_str(), "test-pipeline");
        assert_eq!(host.identity.connector_id, "source-postgres");
        assert_eq!(host.current_stream(), "users");
    }

    #[test]
    fn builder_defaults_dlq_limit() {
        let host = test_host_state();
        assert_eq!(host.checkpoints.dlq_limit, DEFAULT_DLQ_LIMIT);
    }

    #[test]
    fn builder_custom_dlq_limit() {
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        let host = ComponentHostState::builder()
            .pipeline("p")
            .connector_id("c")
            .stream("s")
            .state_backend(state)
            .dlq_limit(500)
            .build()
            .unwrap();
        assert_eq!(host.checkpoints.dlq_limit, 500);
    }

    #[test]
    fn builder_missing_required_field_returns_error() {
        let state = Arc::new(SqliteStateBackend::in_memory().unwrap());
        let result = ComponentHostState::builder()
            .connector_id("c")
            .stream("s")
            .state_backend(state)
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn scoped_state_key_pipeline_scope() {
        let host = test_host_state();
        assert_eq!(
            host.scoped_state_key(StateScope::Pipeline, "offset"),
            "offset"
        );
    }

    #[test]
    fn scoped_state_key_stream_scope() {
        let host = test_host_state();
        assert_eq!(
            host.scoped_state_key(StateScope::Stream, "offset"),
            "users:offset"
        );
    }

    #[test]
    fn scoped_state_key_connector_scope() {
        let host = test_host_state();
        assert_eq!(
            host.scoped_state_key(StateScope::ConnectorInstance, "offset"),
            "source-postgres:offset"
        );
    }

    #[test]
    fn error_category_from_str_known() {
        assert_eq!("config".parse::<ErrorCategory>(), Ok(ErrorCategory::Config));
        assert_eq!("schema".parse::<ErrorCategory>(), Ok(ErrorCategory::Schema));
    }

    #[test]
    fn error_category_from_str_unknown() {
        assert!("bogus".parse::<ErrorCategory>().is_err());
    }

    #[test]
    fn frame_data_holds_bytes() {
        use bytes::Bytes;
        let payload = Bytes::from_static(b"test-ipc-payload");
        let frame = Frame::Data(payload.clone());
        match frame {
            Frame::Data(b) => assert_eq!(b, payload),
            Frame::EndStream => panic!("expected Data"),
        }
    }
}
