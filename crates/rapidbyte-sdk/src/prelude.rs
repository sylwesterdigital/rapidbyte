//! Convenience re-exports for plugin authors.
//!
//! ```ignore
//! use rapidbyte_sdk::prelude::*;
//! ```

// Plugin traits
pub use crate::plugin::{Destination, Source, Transform};

// Feature traits
pub use crate::features::{BulkLoadDestination, CdcSource, PartitionedSource};
pub use crate::stream::{CdcResumeToken, PartitionCoordinates};

// Context and logging
pub use crate::context::{Context, LogLevel};

// Errors
pub use crate::error::{CommitState, PluginError, ValidationResult, ValidationStatus};

// Protocol types — lifecycle
pub use crate::wire::{Feature, PluginInfo, ProtocolVersion};

// Protocol types — streams and catalog
pub use crate::catalog::{Catalog, ColumnSchema, Stream};
pub use crate::cursor::{CursorInfo, CursorValue};
pub use crate::stream::{StreamContext, StreamLimits};
pub use crate::wire::{SyncMode, WriteMode};

// Protocol types — data flow
pub use crate::arrow_types::ArrowDataType;
pub use crate::checkpoint::{Checkpoint, CheckpointKind};
pub use crate::metric::{Metric, MetricValue};

// Protocol types — summaries
pub use crate::metric::{ReadPerf, ReadSummary, TransformSummary, WritePerf, WriteSummary};

// Arrow helpers
pub use crate::arrow::{self, arrow_data_type, build_arrow_schema, decode_ipc, encode_ipc};

// Host interop
pub use crate::host_ffi;
pub use crate::host_tcp::HostTcpStream;
