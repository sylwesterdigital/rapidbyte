# Rapidbyte Coding Style Blueprint

Maintainer-focused standards for writing high-performance, correctness-critical Rust across engine, runtime, state, SDK, and connectors. Concrete patterns are sourced from the actual codebase — see cross-references.

## 1) Purpose and Non-Goals

### Purpose

- Define merge-quality coding standards for ingestion/runtime paths.
- Encode DRY, SOLID, and YAGNI in a way that improves throughput and correctness.
- Provide a consistent refactor rubric to prioritize technical debt reduction.
- Document concrete crate layout, error handling, serde, and concurrency patterns used across all 7 host crates and 3 connectors — see Sections 4–9 and 12.

### Non-Goals

- This is not a Rust language tutorial.
- This is not a preference guide for low-impact style choices.
- This does not replace protocol contracts in `docs/PROTOCOL.md`.

## 2) Rule Semantics

- `MUST`: Required for merge. Deviations require explicit maintainer exception.
- `MUST NOT`: Prohibited pattern.
- `SHOULD`: Strong default; deviation requires rationale in PR.
- `MAY`: Optional pattern for context-specific use.

Every `MUST`/`MUST NOT` rule includes:

- Failure mode prevented
- Scope (host, connector, tests, benches)
- Verification method

## 3) Core Principles for Rapidbyte

### DRY (Behavioral DRY)

- `MUST` deduplicate invariants and semantics, not just text.
- `MUST NOT` duplicate protocol mapping logic across crates.
- `SHOULD` share domain-specific helpers only when at least two concrete call sites exist.

Failure mode prevented: semantic drift across connectors/host boundaries.

### SOLID (Applied, Not Dogmatic)

- `MUST` keep external boundaries explicit: connector protocol, runtime host imports, state backends.
- `SHOULD` introduce traits/interfaces only for real substitution points already needed in code.
- `MUST NOT` add abstraction layers to satisfy hypothetical future connectors.

Failure mode prevented: indirection-heavy code that obscures correctness and slows hot paths.

### YAGNI (Perf-Aware)

- `MUST` implement only behavior required by protocol/spec and active roadmap.
- `MUST NOT` add speculative extension hooks in hot-path code.
- `SHOULD` favor simple concrete implementations until a second use case forces generalization.

Failure mode prevented: complexity debt and unnecessary runtime overhead.

## 4) Crate Layout and File Organization

### 4.1 `lib.rs` Canonical Structure

Every host crate `lib.rs` follows this order:

1. Module-level doc comment (`//!`) with responsibility table
2. `#![warn(clippy::pedantic)]`
3. Module declarations (public, `pub(crate)`, private)
4. Top-level re-exports
5. Optional `prelude` module

Example from `rapidbyte-engine/src/lib.rs`:

```rust
//! Pipeline orchestration engine for Rapidbyte.
//!
//! # Crate structure
//!
//! | Module         | Responsibility |
//! |----------------|----------------|
//! | `arrow`        | Arrow IPC encode/decode utilities |
//! | `orchestrator` | Pipeline execution, retry, stream dispatch |

#![warn(clippy::pedantic)]

pub mod arrow;
pub mod config;
pub(crate) mod dlq;
pub mod orchestrator;
pub(crate) mod resolve;

// Top-level re-exports for convenience.
pub use config::parser::parse_pipeline;
pub use config::types::PipelineConfig;
pub use error::PipelineError;
pub use orchestrator::{check_pipeline, run_pipeline};
```

- `MUST` include `#![warn(clippy::pedantic)]` in every host crate.
- `MUST` begin with a `//!` module docstring describing the crate's responsibility.
- `SHOULD` include a responsibility table for crates with 3+ modules.

### 4.2 Module Visibility

| Visibility   | When to use |
|-------------|-------------|
| `pub`        | Modules forming the crate's public API (consumed by other crates) |
| `pub(crate)` | Implementation modules not needed externally (`dlq`, `resolve`) |
| private      | Submodules within a `pub` module that are implementation details |

- `MUST` default internal modules to `pub(crate)`.
- `MUST NOT` make a module `pub` unless another crate depends on it.

### 4.3 Feature-Gated Modules

Backend/driver modules `SHOULD` be feature-gated. Feature flags `MUST` be documented in the crate's `//!` docstring with a table:

```rust
//! # Feature flags
//!
//! | Feature    | Default | Description |
//! |------------|---------|-------------|
//! | `sqlite`   | **yes** | SQLite backend via `rusqlite` |
//! | `postgres` | no      | PostgreSQL backend via `postgres` |

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

// Re-exports mirror feature gates.
#[cfg(feature = "postgres")]
pub use postgres::PostgresStateBackend;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStateBackend;
```

### 4.4 Constants

- `MUST` use `SCREAMING_SNAKE_CASE` for module-level constants.
- `SHOULD` use associated constants on the relevant type when the constant is tightly coupled:

```rust
impl StreamLimits {
    pub const DEFAULT_MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;
    pub const DEFAULT_MAX_RECORD_BYTES: u64 = 16 * 1024 * 1024;
}
```

- `SHOULD` use private module-level constants for internal tuning values:

```rust
const BACKOFF_FAST_BASE_MS: u64 = 100;
const BACKOFF_NORMAL_BASE_MS: u64 = 1_000;
const BACKOFF_MAX_MS: u64 = 60_000;
```

## 5) Import Ordering

Imports `MUST` follow four groups, separated by blank lines:

1. **`std`** — standard library
2. **External crates** — third-party dependencies (`anyhow`, `serde`, `tokio`, etc.)
3. **Workspace crates** — `rapidbyte_types`, `rapidbyte_runtime`, `rapidbyte_state`
4. **Crate-local** — `crate::` and `super::` imports

Example from `orchestrator.rs`:

```rust
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use rapidbyte_runtime::{LoadedComponent, SandboxOverrides, WasmRuntime};
use rapidbyte_state::StateBackend;
use rapidbyte_types::wire::{ConnectorRole, SyncMode, WriteMode};

use crate::config::types::PipelineConfig;
use crate::error::{compute_backoff, PipelineError};
use crate::runner::{run_destination_stream, run_source_stream};
```

- `MUST NOT` use glob imports (`use foo::*`) in production code.
- `MAY` use `use super::*` in `#[cfg(test)] mod tests` blocks.
- `SHOULD` alphabetize within each group.

## 6) Error Handling Patterns

### 6.1 Architecture Overview

| Crate | Error Type | Pattern | Scope |
|-------|-----------|---------|-------|
| `types` | `ConnectorError` | Factory + builder, `Serialize`/`Deserialize` | Cross-boundary (host ↔ connector) |
| `state` | `StateError` | `thiserror` enum + `Result<T>` alias | Crate-internal |
| `runtime` | `RuntimeError` | `thiserror` enum + `Result<T>` alias | Crate-internal |
| `engine` | `PipelineError` | Two-variant boundary enum (Connector / Infrastructure) | Orchestration boundary |

### 6.2 `ConnectorError` Factory + Builder

`ConnectorError` (in `rapidbyte-types`) uses a private `new` constructor with public category-specific factory methods. Each factory pre-sets defaults for `retryable`, `scope`, and `backoff_class`:

```rust
impl ConnectorError {
    // Private — callers use category factories below.
    fn new(category: ErrorCategory, scope: ErrorScope, retryable: bool, ...) -> Self { ... }

    /// Configuration error (not retryable).
    #[must_use]
    pub fn config(code: impl Into<String>, message: impl Into<String>) -> Self { ... }

    /// Transient network error (retryable, normal backoff).
    #[must_use]
    pub fn transient_network(code: impl Into<String>, message: impl Into<String>) -> Self { ... }
}
```

Builder methods chain optional metadata:

```rust
ConnectorError::transient_db("COMMIT_FAILED", "timeout")
    .with_commit_state(CommitState::AfterCommitUnknown)
    .with_details(serde_json::json!({"table": "users"}))
```

- `MUST` annotate factory methods with `#[must_use]`.
- `MUST` annotate builder methods with `#[must_use]`.
- `MUST NOT` expose `new` publicly — force callers through category factories.

### 6.3 Crate-Level Error Enum + Result Alias

Internal crates define a `thiserror` enum and a `Result<T>` alias:

```rust
/// Errors from state backend operations.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("state backend error: {0}")]
    Backend(Box<dyn std::error::Error + Send + Sync>),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("state backend lock poisoned")]
    LockPoisoned,
}

/// Convenience alias used throughout this crate.
pub type Result<T> = std::result::Result<T, StateError>;
```

- `MUST` keep variant messages lowercase and descriptive.
- `SHOULD` provide helper constructors for wrapping dynamic errors (`StateError::backend(err)`).

### 6.4 `PipelineError` Boundary Enum

The engine uses a two-variant enum to separate typed connector errors from opaque infrastructure failures:

```rust
pub enum PipelineError {
    /// Typed connector error with retry metadata.
    Connector(ConnectorError),
    /// Infrastructure error (WASM load, channel, state backend, etc.)
    Infrastructure(anyhow::Error),
}
```

- `MUST` preserve the Connector/Infrastructure boundary — do not collapse to a single `anyhow::Error`.
- `MUST` implement `From<anyhow::Error>` for Infrastructure only.

## 7) Serde and Protocol Type Conventions

### 7.1 Enum Attributes

Protocol-boundary enums `MUST` use:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    Cdc,
    Stateful,
    ExactlyOnce,
}
```

- `MUST` apply `#[non_exhaustive]` to enums that cross the host/connector boundary and may gain variants.
- `MUST` use `#[serde(rename_all = "snake_case")]` for consistent JSON keys.
- `SHOULD` use `#[serde(tag = "mode", rename_all = "snake_case")]` for internally-tagged enums with data:

```rust
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum WriteMode {
    Append,
    Replace,
    Upsert { primary_key: Vec<String> },
}
```

- `SHOULD` derive `#[default]` on the canonical default variant:

```rust
#[derive(Default)]
pub enum DataErrorPolicy {
    Skip,
    #[default]
    Fail,
    Dlq,
}
```

### 7.2 Optional Fields

- `MUST` use `#[serde(default, skip_serializing_if = "Option::is_none")]` for optional fields:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub retry_after_ms: Option<u64>,
```

- `SHOULD` use `skip_serializing_if = "Vec::is_empty"` for optional vectors:

```rust
#[serde(default, skip_serializing_if = "Vec::is_empty")]
pub features: Vec<Feature>,
```

### 7.3 Display vs Serde Separation

`Display` implementations `MUST` use explicit `match`, not `#[derive]`:

```rust
impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Config => "config",
            Self::Auth => "auth",
            Self::TransientNetwork => "transient_network",
        };
        f.write_str(s)
    }
}
```

This keeps `Display` output decoupled from serde serialization and ensures exhaustive handling.

### 7.4 IPC Types

Types that cross the host/connector boundary via JSON `MUST` derive both `Serialize` and `Deserialize`. All types in `rapidbyte-types` follow this rule.

## 8) Visibility and API Design

### 8.1 Visibility Rules

| Element | Default | Rationale |
|---------|---------|-----------|
| Structs used only within crate | `pub(crate)` | Prevents accidental API surface growth |
| Fields of crate-internal structs | `pub(crate)` | Allows crate-wide access without external exposure |
| Fields of cross-boundary types | `pub` | Connectors and external crates need direct access |
| Helper functions | `pub(crate)` | Exposed only if another crate needs them |
| Error types | `pub` | Callers must match and handle |

### 8.2 Re-Export Conventions

Each crate `SHOULD` re-export its most-used public types from `lib.rs` with a comment marker:

```rust
// Top-level re-exports for convenience.
pub use config::parser::parse_pipeline;
pub use config::types::PipelineConfig;
pub use error::PipelineError;
```

- `MUST NOT` re-export unstable internals.
- `MAY` re-export upstream crate types when downstream crates need them:

```rust
/// Re-export Wasmtime types needed by downstream crates.
pub mod wasmtime_reexport {
    pub use wasmtime::component::HasSelf;
    pub use wasmtime::Error;
}
```

### 8.3 Prelude Module Pattern

Leaf crates (`types`, `state`) `SHOULD` provide a `prelude` module for ergonomic glob imports:

```rust
/// Common imports for typical usage.
///
/// ```
/// use rapidbyte_types::prelude::*;
/// ```
pub mod prelude {
    pub use crate::error::{ConnectorError, ErrorCategory, ValidationResult};
    pub use crate::wire::{ConnectorInfo, ConnectorRole, Feature, SyncMode};
    // ...
}
```

- `MUST NOT` put items in the prelude that are only used by one consumer.
- `SHOULD` test that prelude re-exports resolve correctly (see Section 13).

## 9) Connector Conventions

### 9.1 Entry Point and Manifests

Every connector has two required parts: a `build.rs` manifest and a `main.rs` entry point.

**Manifest (`build.rs`)** — declares metadata, capabilities, and environment requirements via `ManifestBuilder`:

```rust
// connectors/dest-postgres/build.rs
use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::{Feature, WriteMode};

fn main() {
    ManifestBuilder::destination("rapidbyte/dest-postgres")
        .name("PostgreSQL Destination")
        .description("Writes data to PostgreSQL using INSERT or COPY")
        .write_modes(&[
            WriteMode::Append,
            WriteMode::Replace,
            WriteMode::Upsert { primary_key: vec![] },
        ])
        .dest_features(vec![Feature::BulkLoadCopy])
        .allow_runtime_network()
        .env_vars(&["PGSSLROOTCERT"])
        .emit();
}
```

```rust
// connectors/source-postgres/build.rs
use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::SyncMode;

fn main() {
    ManifestBuilder::source("rapidbyte/source-postgres")
        .name("PostgreSQL Source")
        .description("Reads data from PostgreSQL tables using streaming queries")
        .sync_modes(&[SyncMode::FullRefresh, SyncMode::Incremental])
        .allow_runtime_network()
        .env_vars(&["PGSSLROOTCERT"])
        .emit();
}
```

- `MUST` use `ManifestBuilder` to declare name, description, supported modes, and feature flags.
- `MUST` call `allow_runtime_network()` for connectors that establish TCP connections.
- `MUST` declare required environment variables (e.g., `PGSSLROOTCERT`) via `env_vars()`.
- Transform connectors that need no special capabilities `MAY` use a minimal manifest:

```rust
// connectors/transform-sql/build.rs
ManifestBuilder::transform("rapidbyte/transform-sql")
    .name("SQL Transform")
    .description("Executes SQL queries on Arrow batches using Apache DataFusion")
    .emit();
```

**Entry point (`main.rs`)** — the `#[connector(role)]` attribute and trait implementation:

```rust
use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(source)]
pub struct SourcePostgres {
    config: config::Config,
}

impl Source for SourcePostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        config.validate()?;
        Ok((Self { config }, ConnectorInfo { ... }))
    }

    async fn discover(&mut self, ctx: &Context) -> Result<Catalog, ConnectorError> { ... }
    async fn read(&mut self, ctx: &Context, stream: StreamContext) -> Result<ReadSummary, ConnectorError> { ... }
    async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> { ... }
}
```

- `MUST` call `config.validate()` in `init` before any I/O.
- `MUST` use `rapidbyte_sdk::connector` attribute, not manual WIT bindings.

### 9.2 Config Type

Connector config types `MUST` derive `Deserialize` and `ConfigSchema`, and `SHOULD` implement a `validate()` method:

```rust
#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    pub host: String,
    #[serde(default = "default_port")]
    #[schema(default = 5432)]
    pub port: u16,
    #[serde(default)]
    #[schema(secret)]
    pub password: String,
}

impl Config {
    /// # Errors
    /// Returns `Err` if required fields are invalid.
    pub fn validate(&self) -> Result<(), ConnectorError> { ... }
}
```

- `MUST` annotate secret fields with `#[schema(secret)]`.
- `SHOULD` document `# Errors` on `validate()`.

### 9.3 Module Layout

Connectors `SHOULD` follow this module pattern:

| Module | Role |
|--------|------|
| `config` | Config struct + `validate()` + defaults |
| `client` | Connection setup, connection string, `validate()` helper |
| `encode` | Arrow/IPC encoding/decoding helpers |
| `query` | SQL query builders |
| `metrics` | Metric emission helpers |
| `types` | Connector-specific types |

### 9.4 Network I/O and Connection Safety

- `MUST` use `HostTcpStream` (`rapidbyte_sdk::host_tcp`) for all TCP connections to ensure host ACL enforcement.
- `MUST NOT` use raw WASI sockets — the host enforces connector ACLs through `HostTcpStream`.
- `MUST` wrap raw connection handlers in `tokio::spawn` to manage background tasks without blocking the main connector logic.
- `MUST` log connection errors using `rapidbyte_sdk::host_ffi::log` for visibility in host logs.

Example from `source-postgres/src/client.rs`:

```rust
pub(crate) async fn connect(config: &Config) -> Result<Client, String> {
    let mut pg = PgConfig::new();
    pg.host(&config.host);
    pg.port(config.port);
    pg.user(&config.user);
    if !config.password.is_empty() {
        pg.password(&config.password);
    }
    pg.dbname(&config.database);

    let stream = rapidbyte_sdk::host_tcp::HostTcpStream::connect(&config.host, config.port)
        .map_err(|e| format!("Connection failed: {e}"))?;
    let (client, connection) = pg
        .connect_raw(stream, NoTls)
        .await
        .map_err(|e| format!("Connection failed: {e}"))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            rapidbyte_sdk::host_ffi::log(0, &format!("PostgreSQL connection error: {e}"));
        }
    });

    Ok(client)
}
```

### 9.5 Validation and Connectivity Tests

- `MUST` implement a `validate` helper that performs a live connectivity test (e.g., `SELECT 1`) before signaling `ValidationStatus::Success`.
- `SHOULD` provide descriptive success messages that include connection details (host, port, target database/schema).

Example from `dest-postgres/src/client.rs`:

```rust
pub(crate) async fn validate(config: &Config) -> Result<ValidationResult, ConnectorError> {
    let client = connect(config)
        .await
        .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;

    client.query_one("SELECT 1", &[]).await.map_err(|e| {
        ConnectorError::transient_network(
            "CONNECTION_TEST_FAILED",
            format!("Connection test failed: {e}"),
        )
    })?;

    let schema_check = client
        .query_one(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
            &[&config.schema],
        )
        .await;

    let message = match schema_check {
        Ok(_) => format!(
            "Connected to {}:{}/{} (schema: {})",
            config.host, config.port, config.database, config.schema
        ),
        Err(_) => format!(
            "Connected to {}:{}/{} (schema '{}' does not exist, will be created)",
            config.host, config.port, config.database, config.schema
        ),
    };

    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message,
    })
}
```

Failure mode prevented: silent misconfiguration reaching production; users get clear feedback about connectivity issues before pipeline execution.

Verification: e2e `check` tests that exercise `validate` against a live database.

## 10) Correctness Standards (Ingestion-Critical)

### 10.1 Data Semantics

- `MUST` make row drop behavior explicit and policy-driven (`fail`/`skip`/`dlq`).
- `MUST` keep batch/stream scope semantics deterministic and documented.
- `MUST` preserve all configured validation failure reasons when reporting invalid rows.

Failure mode prevented: silent data loss and nondeterministic pipeline outcomes.

Verification: unit tests + e2e policy matrix coverage.

### 10.2 Error Modeling

- `MUST` preserve error category boundaries (see Section 6).
- `MUST` include actionable context in public/boundary errors.
- `MUST NOT` collapse typed errors into opaque strings at boundary interfaces.
- `MUST NOT` panic in runtime data paths.

Failure mode prevented: retry misclassification, unrecoverable crashes, opaque incidents.

Verification: typed error assertions in tests, review checklist.

### 10.3 Numeric and Type Safety

- `MUST` define behavior for non-finite floats (`NaN`, `+/-Inf`) in rule evaluation.
- `MUST` preserve Decimal semantics (scale-aware comparisons).
- `MUST NOT` rely on lossy conversions without explicit acceptance of precision tradeoffs.

Failure mode prevented: acceptance/rejection drift and production data-quality regressions.

Verification: edge-case unit tests for numeric boundaries and decimal variants.

### 10.4 State, Checkpoints, and Idempotency

- `MUST` treat checkpoint writes and commit coordination as correctness-critical.
- `MUST` make idempotency assumptions explicit when retries can replay effects.
- `MUST NOT` advance cursor/state on partial or ambiguous destination outcomes.

Failure mode prevented: duplicate writes, data gaps, irreversible cursor corruption.

Verification: engine integration tests and checkpoint correlation tests.

### 10.5 Schema Drift and Evolution

- `MUST` implement explicit schema drift detection when writing to structured destinations.
- `MUST` support configurable `SchemaEvolutionPolicy` for handling new columns, removed columns, type changes, and nullability changes.
- `MUST` default to `ColumnPolicy::Fail` for unexpected schema changes unless otherwise configured.
- `SHOULD` use database-specific escaping (e.g., `quote_identifier`) for all DDL operations to prevent SQL injection or syntax errors from special characters.

Example from `dest-postgres/src/ddl/drift.rs`:

```rust
#[derive(Debug, Default)]
pub(crate) struct SchemaDrift {
    pub(crate) new_columns: Vec<(String, String)>,
    pub(crate) removed_columns: Vec<String>,
    pub(crate) type_changes: Vec<(String, String, String)>,
    pub(crate) nullability_changes: Vec<(String, bool, bool)>,
}

// Policy application:
for (col_name, pg_type) in &drift.new_columns {
    match policy.new_column {
        ColumnPolicy::Fail => {
            return Err(format!(
                "Schema evolution: new column '{col_name}' detected but policy is 'fail'"
            ));
        }
        ColumnPolicy::Add => {
            let sql = format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                qualified_table,
                quote_identifier(col_name),
                pg_type
            );
            client.execute(&sql, &[]).await.map_err(|e| {
                format!("ALTER TABLE ADD COLUMN '{col_name}' failed: {e}")
            })?;
        }
        ColumnPolicy::Ignore => {
            self.ignored_columns.insert(col_name.clone());
        }
    }
}
```

Failure mode prevented: silent column loss, type coercion bugs, and SQL injection in DDL statements.

Verification: e2e schema evolution tests for each policy combination + DDL escaping unit tests.

## 11) Performance Standards (Hot-Path Rust)

### 11.1 Allocation Discipline

- `MUST` avoid per-row heap allocation in tight loops unless justified.
- `SHOULD` preallocate with known capacities.
- `SHOULD` prefer borrowed/sliced data over cloning in data-path code.
- `MUST` annotate unavoidable allocation-heavy code with rationale.

Failure mode prevented: throughput collapse from allocator pressure.

Verification: benchmark evidence + targeted review.

### 11.2 Throughput and Backpressure

- `MUST` use bounded channels and explicit backpressure semantics.
- `MUST NOT` introduce unbounded buffering between stages.
- `SHOULD` keep stage coupling visible in orchestration code.

Failure mode prevented: memory blowups and unstable tail latency.

Verification: architecture review + load/e2e behavior checks.

### 11.3 Async/Blocking Boundaries

- `MUST` make blocking operations explicit in async contexts (see Section 12).
- `MUST NOT` hide blocking I/O in async hot paths.
- `SHOULD` keep CPU-heavy transforms off latency-sensitive control paths.

Failure mode prevented: scheduler starvation and unpredictable throughput.

Verification: review + focused perf runs.

### 11.4 Observability in Performance Work

- `MUST` emit metrics for critical failure and throughput dimensions.
- `SHOULD` include stable labels for high-value cardinality dimensions (for example `rule`, `field`).
- `MUST NOT` emit high-cardinality labels with uncontrolled user values.

Failure mode prevented: invisible regressions and unusable monitoring.

Verification: metrics unit tests + e2e assertions where practical.

### 11.5 Batch Processing and Buffering

- `MUST` use `Vec::with_capacity` when building binary buffers for batch writes to minimize reallocations.
- `MUST` implement a configurable flush threshold to bound memory usage during large data transfers.
- `SHOULD` use `std::mem::take` to efficiently move buffer ownership when sending data to async sinks.

Example from `dest-postgres/src/copy.rs`:

```rust
const DEFAULT_FLUSH_BYTES: usize = 4 * 1024 * 1024;

let flush_threshold = flush_bytes.unwrap_or(DEFAULT_FLUSH_BYTES).max(1);
let mut buf = Vec::with_capacity(flush_threshold);

// ... encode rows into buf ...

if buf.len() >= flush_threshold {
    sink.send(Bytes::from(std::mem::take(&mut buf)))
        .await
        .map_err(|e| format!("COPY send failed: {e}"))?;
    buf = Vec::with_capacity(flush_threshold);
}
```

Adaptive flush sizing based on row width from `dest-postgres/src/writer.rs`:

```rust
const COPY_FLUSH_1MB: usize = 1024 * 1024;
const COPY_FLUSH_4MB: usize = 4 * 1024 * 1024;
const COPY_FLUSH_16MB: usize = 16 * 1024 * 1024;
const COPY_FLUSH_MAX: usize = 32 * 1024 * 1024;

fn adaptive_copy_flush_bytes(configured: Option<usize>, avg_row_bytes: Option<usize>) -> usize {
    if let Some(bytes) = configured {
        return bytes.clamp(COPY_FLUSH_1MB, COPY_FLUSH_MAX);
    }
    match avg_row_bytes {
        Some(bytes) if bytes >= 64 * 1024 => COPY_FLUSH_16MB,
        Some(bytes) if bytes >= 8 * 1024 => COPY_FLUSH_4MB,
        _ => COPY_FLUSH_1MB,
    }
}
```

Failure mode prevented: OOM from unbounded buffer growth; excessive syscalls from undersized flushes.

Verification: benchmark runs with varying row sizes + memory profiling.

## 12) Async and Concurrency Patterns

### 12.1 Bounded `mpsc::channel` Between Stages

All inter-stage communication uses bounded `tokio::sync::mpsc` channels. The capacity is derived from config or defaults to a small value:

```rust
let (tx, rx) = mpsc::channel::<Frame>(params.channel_capacity);
```

- `MUST` use bounded channels — never `mpsc::unbounded_channel`.
- `SHOULD` size channel capacity based on `StreamLimits::max_inflight_batches` or a tuned constant.

### 12.2 `spawn_blocking` for WASM and State Calls

All WASM component calls and synchronous state backend operations run inside `tokio::task::spawn_blocking`:

```rust
let src_handle = tokio::task::spawn_blocking(move || {
    run_source_stream(&runtime, &component, &config, &stream_ctx)
});
```

- `MUST` use `spawn_blocking` for any synchronous call that may block (WASM execution, DB queries, file I/O).
- `MUST NOT` call blocking WASM functions directly from an async context.

### 12.3 `JoinSet` + `Semaphore` for Parallelism

Parallel stream processing uses `JoinSet` for task collection and `Semaphore` for concurrency limiting:

```rust
let semaphore = Arc::new(tokio::sync::Semaphore::new(parallelism));
let mut join_set = JoinSet::new();

for stream in streams {
    let permit = semaphore.clone().acquire_owned().await?;
    join_set.spawn(async move {
        let result = process_stream(stream).await;
        drop(permit);
        result
    });
}

while let Some(result) = join_set.join_next().await { ... }
```

- `SHOULD` use `Semaphore` to control concurrency rather than pre-partitioning work.
- `MUST` drop the permit after the task completes, not before.

## 13) Testing Patterns

### 13.1 Colocated Tests

Tests live in `#[cfg(test)] mod tests` at the bottom of the same file. Every test module uses `use super::*`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // PipelineError tests
    // -----------------------------------------------------------------------

    #[test]
    fn config_error_defaults() {
        let err = ConnectorError::config("MISSING_HOST", "host is required");
        assert_eq!(err.category, ErrorCategory::Config);
        assert!(!err.retryable);
    }
}
```

- `MUST` colocate unit tests in the same file as the code under test.
- `SHOULD` use `// ---` section separators when a test module covers multiple concerns.

### 13.2 Required Test Categories by Change Type

| Change type | Required tests |
|------------|---------------|
| Rule/config behavior | Unit: happy path + invalid shape + edge values |
| Engine/runtime wiring | Integration + relevant e2e path |
| Policy behavior (`fail`/`skip`/`dlq`) | E2e assertions for all affected modes |
| Serde types (new or modified) | Roundtrip test (see 13.4) |
| Error types | Display format + category assertions |
| Perf-sensitive changes | Benchmark or targeted throughput evidence |

### 13.3 Test Data Helpers

Config-heavy test modules `SHOULD` define a `base_config()` helper and use struct update syntax:

```rust
fn base_config() -> Config {
    Config {
        host: "localhost".to_string(),
        port: 5432,
        user: "postgres".to_string(),
        password: String::new(),
        database: "test".to_string(),
        replication_slot: None,
        publication: None,
    }
}

#[test]
fn validate_rejects_empty_publication() {
    let cfg = Config {
        publication: Some(String::new()),
        ..base_config()
    };
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("must not be empty"));
}
```

### 13.4 Serde Roundtrip Test Pattern

Every `Serialize + Deserialize` type `SHOULD` have a roundtrip test:

```rust
#[test]
fn serde_roundtrip() {
    let err = ConnectorError::rate_limit("THROTTLED", "slow down", Some(5000))
        .with_details(serde_json::json!({"endpoint": "/api/data"}));
    let json = serde_json::to_string(&err).unwrap();
    let back: ConnectorError = serde_json::from_str(&json).unwrap();
    assert_eq!(err, back);
}
```

For enums, iterate all variants:

```rust
#[test]
fn sync_mode_roundtrip() {
    for mode in [SyncMode::FullRefresh, SyncMode::Incremental, SyncMode::Cdc] {
        let json = serde_json::to_string(&mode).unwrap();
        let back: SyncMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, back);
    }
}
```

### 13.5 Verification Commands

- `just fmt`
- `just lint`
- Relevant crate tests (`cargo test -p <crate>` or manifest-path)
- Relevant e2e suite (`cargo test --manifest-path tests/e2e/Cargo.toml --test <suite>`)

`MUST` include command evidence in PR description for non-trivial behavior/perf changes.

## 14) Documentation Standards

### Module Docstrings

Every file `SHOULD` start with a `//!` docstring describing the module's responsibility. Crate root `lib.rs` files `MUST` include one.

For crates with many modules, include a responsibility table:

```rust
//! | Module         | Responsibility |
//! |----------------|----------------|
//! | `arrow`        | Arrow IPC encode/decode utilities |
//! | `orchestrator` | Pipeline execution, retry, stream dispatch |
```

For crates with feature flags, include a feature table (see Section 4.3).

### Item Documentation

- `SHOULD` document public functions with `///` docstrings.
- `MUST` include `# Errors` sections on public functions that return `Result`:

```rust
/// # Errors
/// Returns `Err` if `replication_slot` or `publication` is empty or exceeds the
/// 63-byte PostgreSQL identifier limit.
pub fn validate(&self) -> Result<(), ConnectorError> { ... }
```

### Comments

- `SHOULD` comment invariants and non-obvious constraints, not line-by-line mechanics.
- `MUST` update protocol/config docs when behavior contracts change.
- `MUST NOT` leave stale docs after interface changes.

## 15) Good and Bad Patterns

### Good: hot-loop preallocation

```rust
let mut failures = Vec::with_capacity(config.rules.len());
```

### Bad: hidden per-iteration temporary allocation

```rust
for row in 0..batch.num_rows() {
    let failures = Vec::new(); // repeated allocation in hot path
}
```

### Good: explicit non-finite handling

```rust
if !value.is_finite() {
    return Err(DataError::non_finite(field));
}
```

### Bad: implicit comparison semantics with NaN

```rust
if value < min || value > max {
    // NaN can bypass both checks
}
```

### Good: typed error factory

```rust
ConnectorError::transient_db("DEADLOCK", "deadlock detected")
    .with_commit_state(CommitState::BeforeCommit)
```

### Bad: stringly-typed error at boundary

```rust
Err(anyhow!("deadlock detected")) // loses category, retry metadata, scope
```

### Good: `#[non_exhaustive]` on protocol enum

```rust
#[derive(Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum Feature {
    Cdc,
    Stateful,
}
```

### Bad: bare enum crossing host/connector boundary

```rust
#[derive(Serialize, Deserialize)]
pub enum Feature { // no #[non_exhaustive], no rename_all
    CDC,           // inconsistent casing
    Stateful,
}
```

### Good: blocking call on spawn_blocking

```rust
let result = tokio::task::spawn_blocking(move || {
    run_source_stream(&runtime, &component, &config, &ctx)
}).await?;
```

### Bad: blocking WASM call in async context

```rust
// Blocks the tokio runtime thread — causes scheduler starvation
let result = run_source_stream(&runtime, &component, &config, &ctx);
```

### Good: pre-computed write targets

```rust
// Pre-downcast active columns once per batch — eliminates per-cell downcast_ref() calls.
let typed_cols = downcast_columns(batch, target.active_cols)?;

for row_idx in 0..batch.num_rows() {
    for typed_col in &typed_cols {
        write_binary_field(&mut buf, typed_col, row_idx);
    }
}
```

### Bad: repeated schema lookups in hot loops

```rust
// BAD: querying metadata or downcasting inside a row-processing loop
for row in 0..batch.num_rows() {
    let col = batch.column(i).as_any().downcast_ref::<Int64Array>().unwrap();
    // per-row downcast — O(rows * columns) type checks instead of O(columns)
}
```

### Good: documenting safety and limits with `#[allow]`

```rust
// Safety: PG COPY binary tuple header uses i16 for field count;
// tables with >32,767 columns are not supported.
#[allow(clippy::cast_possible_truncation)]
let num_fields = target.active_cols.len() as i16;
```

### Bad: suppressing clippy without rationale

```rust
#[allow(clippy::cast_possible_truncation)]
let num_fields = target.active_cols.len() as i16; // why is this safe?
```

## 16) Enforcement Matrix

### Merge-Blocking

- Formatting/lint failures
- Test regressions in affected scope
- Missing coverage for changed correctness semantics
- Undocumented behavior changes to protocol/config contracts

### Review-Blocking Checklist

- Correctness semantics preserved or intentionally changed and documented
- Error categories preserved at boundaries (Section 6)
- No unbounded buffers introduced (Section 12.1)
- Hot-path allocation impact considered (Section 11.1)
- Metrics/logging implications considered (Section 11.4)

### Exceptions

- `SHOULD` deviations: allowed with rationale in PR
- `MUST`/`MUST NOT` deviations: require maintainer sign-off with risk + rollback note

## 17) Refactor Priority Rubric (Blueprint Driver)

Score each crate/module from `0..5` in each dimension:

- Correctness risk
- Performance impact
- Blast radius
- Maintainability debt
- Observability gap

Priority score:

```text
Priority = 2*Correctness + 2*Performance + BlastRadius + Maintainability + Observability
```

Classes:

- `P0`: high correctness + high performance pressure; refactor first
- `P1`: meaningful debt reduction with moderate operational risk
- `P2`: opportunistic cleanup and consistency work

### Suggested Initial Focus Areas

- P0 candidates: orchestrator stage coupling and checkpoint/commit coordination paths
- P1 candidates: connector validation helpers and shared metric-shape utilities
- P2 candidates: low-impact module decomposition and naming consistency

## 18) PR Template Snippet

Use this in maintainers' PR descriptions for non-trivial changes:

```markdown
## Coding Style Compliance
- [ ] Correctness semantics unchanged or documented
- [ ] Error categories preserved (§6)
- [ ] No unbounded buffering introduced (§12.1)
- [ ] Hot-path allocation impact reviewed (§11.1)
- [ ] Metrics/logging impact reviewed (§11.4)
- [ ] Serde roundtrip tests for new/changed types (§13.4)

## Verification
- [ ] just fmt
- [ ] just lint
- [ ] crate tests
- [ ] relevant e2e
- [ ] perf evidence (if hot path touched)
```
