//! Shared Rapidbyte protocol, manifest, and error types.
//!
//! Dependency-boundary-safe for both host runtime and WASI plugin targets.
//! All types use serde for serialization across the host/guest boundary.
//!
//! | Module         | Responsibility |
//! |----------------|----------------|
//! | `arrow`        | Arrow data type mappings |
//! | `catalog`      | Catalog, stream, and column schema definitions |
//! | `checkpoint`   | Checkpoint and state scope types |
//! | `compression`  | Compression codec enum |
//! | `cursor`       | Cursor info and value types for incremental sync |
//! | `envelope`     | DLQ record and payload envelope types |
//! | `error`        | `PluginError`, `ValidationResult`, error categories |
//! | `manifest`     | Plugin manifest and permission types |
//! | `metric`       | Metric, summary types (read/write/transform) |
//! | `state`        | Run state, pipeline ID, cursor state types |
//! | `stream`       | Stream context, limits, policies |
//! | `wire`         | Wire protocol enums (sync mode, write mode, role) |

#![warn(clippy::pedantic)]

pub mod arrow;
pub mod catalog;
pub mod checkpoint;
pub mod compression;
pub mod cursor;
pub mod envelope;
pub mod error;
pub mod format;
pub mod manifest;
pub mod metric;
pub mod state;
pub mod stream;
pub mod wire;

/// Common imports for typical usage.
///
/// ```
/// use rapidbyte_types::prelude::*;
/// ```
pub mod prelude {
    pub use crate::arrow::ArrowDataType;
    pub use crate::catalog::{Catalog, ColumnSchema, SchemaHint, Stream};
    pub use crate::checkpoint::{Checkpoint, CheckpointKind, StateScope};
    pub use crate::compression::CompressionCodec;
    pub use crate::cursor::{CursorInfo, CursorType, CursorValue};
    pub use crate::envelope::{DlqRecord, PayloadEnvelope, Timestamp};
    pub use crate::error::{
        BackoffClass, CommitState, ErrorCategory, ErrorScope, PluginError, ValidationResult,
        ValidationStatus,
    };
    pub use crate::manifest::PluginManifest;
    pub use crate::metric::{Metric, MetricValue, ReadSummary, TransformSummary, WriteSummary};
    pub use crate::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
    pub use crate::stream::{StreamContext, StreamLimits, StreamPolicies};
    pub use crate::wire::{Feature, PluginInfo, PluginKind, ProtocolVersion, SyncMode, WriteMode};
}
