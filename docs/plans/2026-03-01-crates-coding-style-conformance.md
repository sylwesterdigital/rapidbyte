# Host Crates Coding Style Conformance Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bring all 6 host crates (`types`, `state`, `sdk`, `runtime`, `engine`, `cli`) into full conformance with `CODING_STYLE.md` §4–§14 — documentation, annotations, and import ordering only. No behavioral changes.

**Architecture:** Pure documentation/annotation/ordering fixes. Each task targets one crate, progressing from leaf crates to dependents. Every task is a standalone commit that builds and passes `cargo clippy`.

**Tech Stack:** Rust, clippy pedantic, workspace crates

---

## Gap Analysis

| Crate | `#![pedantic]` | Module Docstrings | `# Errors` | `#[must_use]` | Import Ordering | Responsibility Table |
|-------|:-:|:-:|:-:|:-:|:-:|:-:|
| rapidbyte-types | yes | yes | n/a | n/a | good | **missing** (SHOULD) |
| rapidbyte-state | yes | yes | good | **2 missing** (SHOULD) | good | good |
| rapidbyte-sdk | **missing** | yes | **~15 missing** | n/a | good | **missing** feature table (SHOULD) |
| rapidbyte-runtime | yes | yes | good | good | **3 files** | good |
| rapidbyte-engine | yes | yes | good | good | **4 files** | good |
| rapidbyte-cli | **missing** | **all 7 files** | **4 missing** | n/a | good | n/a (binary) |

---

### Task 1: Add responsibility table to rapidbyte-types lib.rs

§4.1 SHOULD: Crates with 3+ modules should include a responsibility table.

**Files:**
- Modify: `crates/rapidbyte-types/src/lib.rs:1-5`

**Step 1: Add the responsibility table**

Insert after the existing `//!` docstring, before `#![warn(clippy::pedantic)]`:

```rust
//! Shared Rapidbyte protocol, manifest, and error types.
//!
//! Dependency-boundary-safe for both host runtime and WASI connector targets.
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
//! | `error`        | `ConnectorError`, `ValidationResult`, error categories |
//! | `manifest`     | Connector manifest and permission types |
//! | `metric`       | Metric, summary types (read/write/transform) |
//! | `state`        | Run state, pipeline ID, cursor state types |
//! | `stream`       | Stream context, limits, policies |
//! | `wire`         | Wire protocol enums (sync mode, write mode, role) |
```

**Step 2: Build and verify**

Run: `cargo clippy -p rapidbyte-types -- -D warnings`
Expected: PASS (no new warnings)

**Step 3: Commit**

```
docs(types): add responsibility table to lib.rs per coding style §4.1
```

---

### Task 2: Add `#[must_use]` to rapidbyte-state error factories

§6.2 MUST: Factory methods on error types must have `#[must_use]`.

**Files:**
- Modify: `crates/rapidbyte-state/src/error.rs:19-31`

**Step 1: Add annotations**

```rust
impl StateError {
    /// Wrap any backend-specific error into a [`StateError::Backend`].
    #[must_use]
    pub fn backend(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Backend(Box::new(err))
    }

    /// Wrap backend errors with operation context for easier diagnosis.
    #[must_use]
    pub fn backend_context(
        context: &str,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Backend(Box::new(std::io::Error::other(format!("{context}: {err}"))))
    }
}
```

**Step 2: Build and verify**

Run: `cargo clippy -p rapidbyte-state -- -D warnings`
Expected: PASS

**Step 3: Commit**

```
style(state): add #[must_use] to StateError factories per coding style §6.2
```

---

### Task 3: Add `# Errors` docs to rapidbyte-sdk public functions

§14 MUST: Every public function returning `Result` must have an `# Errors` section.

**Files:**
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs` (~14 functions)
- Modify: `crates/rapidbyte-sdk/src/host_tcp.rs:19` (1 function)
- Modify: `crates/rapidbyte-sdk/src/arrow/ipc.rs` (2 functions)

**Step 1: Add `# Errors` to host_ffi.rs functions**

Add `# Errors` doc sections to all public `Result`-returning functions:

- `set_host_imports()` (line 126) — "Returns `Err` if the host imports have already been initialized."
- `emit_batch()` (line 454) — "Returns `Err` if frame creation, IPC encoding, or frame sealing fails."
- `next_batch()` (line 497) — "Returns `Err` if frame reading or IPC decoding fails."
- `state_get()` (line 523) — "Returns `Err` if the host state backend rejects the read."
- `state_put()` (line 527) — "Returns `Err` if the host state backend rejects the write."
- `state_compare_and_set()` (line 531) — "Returns `Err` if the host state backend rejects the CAS operation."
- `checkpoint()` (line 540) — "Returns `Err` if the host rejects the checkpoint."
- `metric()` (line 548) — "Returns `Err` if metric emission fails."
- `emit_dlq_record()` (line 552) — "Returns `Err` if DLQ record emission fails."
- `connect_tcp()` (line 561) — "Returns `Err` if the host denies the connection or TCP connect fails."
- `socket_read()` (line 565) — "Returns `Err` if the socket read operation fails."
- `socket_write()` (line 569) — "Returns `Err` if the socket write operation fails."

**Step 2: Add `# Errors` to host_tcp.rs**

- `HostTcpStream::connect()` (line 19) — "Returns `Err` if the host denies the connection or TCP handshake fails."

**Step 3: Add `# Errors` to arrow/ipc.rs**

- `encode_ipc()` (line 17) — "Returns `Err` if Arrow IPC stream writer initialization or encoding fails."
- `decode_ipc()` (line 33) — "Returns `Err` if Arrow IPC stream reader initialization or batch deserialization fails."

**Step 4: Build and verify**

Run: `cargo clippy -p rapidbyte-sdk -- -D warnings`
Expected: PASS

**Step 5: Commit**

```
docs(sdk): add # Errors sections to public functions per coding style §14
```

---

### Task 4: Fix import ordering in rapidbyte-runtime

§5 MUST: Imports follow std → external → workspace → crate-local, separated by blank lines.

**Files:**
- Modify: `crates/rapidbyte-runtime/src/engine.rs:10-14`
- Modify: `crates/rapidbyte-runtime/src/host_state.rs:12-25`

**Step 1: Fix engine.rs — merge `sha2` into external group**

Before:
```rust
use anyhow::{Context, Result};
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store, StoreLimits};

use sha2::{Digest, Sha256};
```

After:
```rust
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store, StoreLimits};
```

**Step 2: Fix host_state.rs — regroup external and workspace**

Before (lines 12-25):
```rust
use anyhow::Result;
use chrono::Utc;
use rapidbyte_types::checkpoint::{Checkpoint, StateScope};
use rapidbyte_types::envelope::{DlqRecord, Timestamp};
use rapidbyte_types::error::{ConnectorError, ErrorCategory};
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::metric::{Metric, MetricValue};
use tokio::sync::mpsc;
use wasmtime::component::ResourceTable;
use wasmtime::StoreLimits;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use rapidbyte_state::StateBackend;
use rapidbyte_types::state::{CursorState, PipelineId, StreamName};
```

After:
```rust
use anyhow::Result;
use chrono::Utc;
use tokio::sync::mpsc;
use wasmtime::component::ResourceTable;
use wasmtime::StoreLimits;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use rapidbyte_state::StateBackend;
use rapidbyte_types::checkpoint::{Checkpoint, StateScope};
use rapidbyte_types::envelope::{DlqRecord, Timestamp};
use rapidbyte_types::error::{ConnectorError, ErrorCategory};
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::metric::{Metric, MetricValue};
use rapidbyte_types::state::{CursorState, PipelineId, StreamName};
```

**Step 3: Build and verify**

Run: `cargo clippy -p rapidbyte-runtime -- -D warnings`
Expected: PASS

**Step 4: Commit**

```
style(runtime): fix import ordering in engine.rs and host_state.rs per coding style §5
```

---

### Task 5: Fix import ordering in rapidbyte-engine

§5 MUST: Imports follow std → external → workspace → crate-local, separated by blank lines.

**Files:**
- Modify: `crates/rapidbyte-engine/src/runner.rs:18-27`
- Modify: `crates/rapidbyte-engine/src/checkpoint.rs:3-8`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs:7-39`
- Modify: `crates/rapidbyte-engine/src/config/types.rs:3-8`

**Step 1: Fix runner.rs — move crate-local after workspace**

Before (lines 18-27):
```rust
use crate::error::PipelineError;
use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{
    create_component_linker, dest_bindings, dest_error_to_sdk, dest_validation_to_sdk,
    source_bindings, source_error_to_sdk, source_validation_to_sdk, transform_bindings,
    transform_error_to_sdk, transform_validation_to_sdk, ComponentHostState, CompressionCodec,
    Frame, HostTimings, LoadedComponent, SandboxOverrides, WasmRuntime,
};
use rapidbyte_state::{SqliteStateBackend, StateBackend};
use rapidbyte_types::state::RunStats;
```

After:
```rust
use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{
    create_component_linker, dest_bindings, dest_error_to_sdk, dest_validation_to_sdk,
    source_bindings, source_error_to_sdk, source_validation_to_sdk, transform_bindings,
    transform_error_to_sdk, transform_validation_to_sdk, ComponentHostState, CompressionCodec,
    Frame, HostTimings, LoadedComponent, SandboxOverrides, WasmRuntime,
};
use rapidbyte_state::{SqliteStateBackend, StateBackend};
use rapidbyte_types::state::RunStats;

use crate::error::PipelineError;
```

**Step 2: Fix checkpoint.rs — add blank line between external and workspace**

Before (lines 3-8):
```rust
use anyhow::Result;
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::cursor::CursorValue;

use rapidbyte_state::StateBackend;
use rapidbyte_types::state::{CursorState, PipelineId, StreamName};
```

After:
```rust
use anyhow::Result;

use rapidbyte_state::StateBackend;
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::cursor::CursorValue;
use rapidbyte_types::state::{CursorState, PipelineId, StreamName};
```

**Step 3: Fix orchestrator.rs — separate external/workspace, move crate-local after workspace**

Before (lines 7-39):
```rust
use anyhow::Result;
use rapidbyte_types::catalog::{Catalog, SchemaHint};
use rapidbyte_types::cursor::{CursorInfo, CursorType, CursorValue};
...
use rapidbyte_types::wire::{ConnectorRole, SyncMode, WriteMode};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::arrow::ipc_to_record_batches;
...
use crate::runner::{...};
use rapidbyte_runtime::{...};
use rapidbyte_state::StateBackend;
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};
```

After:
```rust
use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use rapidbyte_runtime::{
    parse_connector_ref, Frame, HostTimings, LoadedComponent, SandboxOverrides, WasmRuntime,
};
use rapidbyte_state::StateBackend;
use rapidbyte_types::catalog::{Catalog, SchemaHint};
use rapidbyte_types::cursor::{CursorInfo, CursorType, CursorValue};
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::error::ValidationResult;
use rapidbyte_types::manifest::{Permissions, ResourceLimits};
use rapidbyte_types::metric::{ReadSummary, WriteSummary};
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};
use rapidbyte_types::stream::{PartitionStrategy, StreamContext, StreamLimits, StreamPolicies};
use rapidbyte_types::wire::{ConnectorRole, SyncMode, WriteMode};

use crate::arrow::ipc_to_record_batches;
use crate::checkpoint::correlate_and_persist_cursors;
use crate::config::types::{parse_byte_size, PipelineConfig, PipelineParallelism};
use crate::error::{compute_backoff, PipelineError};
use crate::execution::{DryRunResult, DryRunStreamResult, ExecutionOptions, PipelineOutcome};
use crate::resolve::{
    build_sandbox_overrides, check_state_backend, create_state_backend, load_and_validate_manifest,
    resolve_connectors, validate_config_against_schema, ResolvedConnectors,
};
use crate::result::{
    CheckResult, DestTiming, PipelineCounts, PipelineResult, SourceTiming, StreamShardMetric,
};
use crate::runner::{
    run_destination_stream, run_discover, run_source_stream, run_transform_stream,
    validate_connector,
};
```

**Step 4: Fix config/types.rs — move external before workspace**

Before (lines 3-8):
```rust
use rapidbyte_types::stream::{DataErrorPolicy, SchemaEvolutionPolicy};
use rapidbyte_types::wire::{SyncMode, WriteMode};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use rapidbyte_types::compression::CompressionCodec;
```

After:
```rust
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use rapidbyte_types::compression::CompressionCodec;
use rapidbyte_types::stream::{DataErrorPolicy, SchemaEvolutionPolicy};
use rapidbyte_types::wire::{SyncMode, WriteMode};
```

**Step 5: Build and verify**

Run: `cargo clippy -p rapidbyte-engine -- -D warnings`
Expected: PASS

**Step 6: Commit**

```
style(engine): fix import ordering in 4 files per coding style §5
```

---

### Task 6: Add module docstrings to rapidbyte-cli

§4.1/§14 MUST: Every file SHOULD start with a `//!` docstring; crate root MUST.

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs:1`
- Modify: `crates/rapidbyte-cli/src/logging.rs:1`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs:1`
- Modify: `crates/rapidbyte-cli/src/commands/run.rs:1`
- Modify: `crates/rapidbyte-cli/src/commands/check.rs:1`
- Modify: `crates/rapidbyte-cli/src/commands/discover.rs:1`
- Modify: `crates/rapidbyte-cli/src/commands/connectors.rs:1`
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs:1`

**Step 1: Add docstrings**

- `main.rs`: `//! Rapidbyte CLI binary — parse arguments, dispatch subcommands.`
- `logging.rs`: `//! Structured logging initialization via tracing-subscriber.`
- `commands/mod.rs`: `//! CLI subcommand implementations.`
- `commands/run.rs`: `//! Pipeline execution subcommand (run).`
- `commands/check.rs`: `//! Pipeline validation subcommand (check).`
- `commands/discover.rs`: `//! Source schema discovery subcommand (discover).`
- `commands/connectors.rs`: `//! Connector listing subcommand (connectors).`
- `commands/scaffold.rs`: `//! Connector project scaffolding subcommand (scaffold).`

**Step 2: Build and verify**

Run: `cargo clippy -p rapidbyte-cli -- -D warnings`
Expected: PASS

**Step 3: Commit**

```
docs(cli): add module-level docstrings per coding style §4.1
```

---

### Task 7: Add `# Errors` to rapidbyte-cli public functions

§14 MUST: Every public function returning `Result` must have an `# Errors` section.

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/run.rs:18`
- Modify: `crates/rapidbyte-cli/src/commands/check.rs:11`
- Modify: `crates/rapidbyte-cli/src/commands/discover.rs:10`
- Modify: `crates/rapidbyte-cli/src/commands/connectors.rs:4`
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs:23`

**Step 1: Add `# Errors` doc sections**

- `run::execute()` — "Returns `Err` if pipeline parsing, validation, or execution fails."
- `check::execute()` — "Returns `Err` if pipeline parsing, validation, or connectivity check fails."
- `discover::execute()` — "Returns `Err` if pipeline parsing, validation, or schema discovery fails."
- `connectors::execute()` — "Returns `Err` if directory scanning or manifest parsing fails."
- `scaffold::run()` — "Returns `Err` if the connector name is invalid, the output directory cannot be created, or file writing fails."

**Step 2: Build and verify**

Run: `cargo clippy -p rapidbyte-cli -- -D warnings`
Expected: PASS

**Step 3: Commit**

```
docs(cli): add # Errors sections to public functions per coding style §14
```

---

### Task 8: Add `#![warn(clippy::pedantic)]` to rapidbyte-cli

§4.1 MUST: Every host crate must include `#![warn(clippy::pedantic)]`.

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs`

**Step 1: Add the annotation**

Add `#![warn(clippy::pedantic)]` after the module docstring, before `mod commands;`.

**Step 2: Build and fix pedantic warnings**

Run: `cargo clippy -p rapidbyte-cli -- -D warnings`

This will likely produce pedantic warnings (e.g. `cast_possible_truncation`, `uninlined_format_args`, `needless_pass_by_value`). Fix each warning in the file where it occurs. Common fixes:
- `cast_possible_truncation`: add `#[allow(clippy::cast_possible_truncation)]` or use `.try_into()`
- `uninlined_format_args`: inline variables into format strings
- `needless_pass_by_value`: change to `&str` or add `#[allow(...)]` if trait-required

**Step 3: Verify clean build**

Run: `cargo clippy -p rapidbyte-cli -- -D warnings`
Expected: PASS (0 warnings)

**Step 4: Commit**

```
style(cli): add #![warn(clippy::pedantic)] per coding style §4.1
```

---

### Task 9: Full workspace verification

Run the complete build and lint suite to ensure no regressions.

**Step 1: Workspace build**

Run: `cargo build --workspace`
Expected: PASS

**Step 2: Workspace tests**

Run: `just test`
Expected: PASS

**Step 3: Format and lint**

Run: `just fmt && just lint`
Expected: PASS (clean)

---

## Out-of-Scope (Noted for Future)

1. **rapidbyte-sdk `#![warn(clippy::pedantic)]`** — SDK targets both host and `wasm32-wasip2`. Enabling pedantic may conflict with WIT-generated code and cross-compilation constraints. Requires careful evaluation in a dedicated PR.
2. **rapidbyte-sdk feature table in lib.rs docstring** — §4.3 SHOULD. The SDK has `runtime` and `build` features; a feature table would be helpful but is a SHOULD-level improvement.
3. **scaffold.rs glob imports** — Lines 286 and 334 use `use rapidbyte_sdk::prelude::*` but they are inside `format!(r#"..."#)` string templates for generated connector code, not actual production imports. No action needed.
4. **rapidbyte-types responsibility table** — Already covered in Task 1 as a SHOULD-level addition.
