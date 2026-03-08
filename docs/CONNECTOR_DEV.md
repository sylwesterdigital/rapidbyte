# Connector Developer Guide

This guide walks you through building a Rapidbyte connector from scratch. By the end, you will have a working Wasm component that plugs into the Rapidbyte pipeline engine.

## 1. Overview

A connector is a **WebAssembly component** compiled to `wasm32-wasip2` and executed inside a Wasmtime sandbox. The host manages networking, state, and data transport -- connectors focus purely on reading or writing data.

There are three connector roles:

| Role | Trait | Purpose |
|---|---|---|
| **Source** | `Source` | Reads data from an external system and emits Arrow IPC batches |
| **Destination** | `Destination` | Receives Arrow IPC batches and writes them to an external system |
| **Transform** | `Transform` | Receives batches, transforms them (e.g. SQL), and emits new batches |

All connectors depend on `rapidbyte-sdk`, which provides the trait definitions, Arrow re-exports, host FFI bindings, and the `#[connector(...)]` proc macro.

## 2. Scaffold a New Connector

The fastest way to start is with the scaffold command:

```bash
# Source connector (name must start with "source-")
just scaffold source-mysql

# Destination connector (name must start with "dest-")
just scaffold dest-snowflake

# Custom output path
just scaffold source-mysql connectors/custom-path
```

This generates a complete project skeleton:

```
connectors/source-mysql/
  .cargo/config.toml   # Sets wasm32-wasip2 as the default build target
  Cargo.toml           # Package manifest with rapidbyte-sdk dependency
  build.rs             # ManifestBuilder -- declares connector capabilities
  src/
    main.rs            # Entry point with #[connector(source)] and trait impl
    config.rs          # Config struct with serde + schema derives
    client.rs          # Connection setup and validation stub
    reader.rs          # Stream reading stub (sources) / writer.rs (destinations)
    schema.rs          # Schema discovery stub (sources only)
```

## 3. Project Structure

A mature connector typically has these modules:

| Module | Role |
|---|---|
| `config` | Config struct with `Deserialize` + `ConfigSchema` derives and a `validate()` method |
| `client` | Connection setup (using `HostTcpStream`), connection string builder, `validate()` |
| `encode` | Arrow RecordBatch encoding from source rows (sources) |
| `decode` | Arrow RecordBatch decoding into target rows (destinations) |
| `query` | SQL query builders, identifier quoting, cursor logic |
| `metrics` | Metric emission helpers (`ctx.metric(...)`) |
| `reader` / `writer` | Core read/write loop orchestration |

## 4. Manifest (build.rs)

The `build.rs` file declares your connector's capabilities to the host via `ManifestBuilder`. The host reads the generated manifest at load time to enforce permissions and advertise features.

### Source manifest

```rust
use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::SyncMode;

fn main() {
    ManifestBuilder::source("rapidbyte/source-mysql")
        .name("MySQL Source")
        .description("Reads data from MySQL tables")
        .sync_modes(&[SyncMode::FullRefresh, SyncMode::Incremental])
        .allow_runtime_network()   // Required for HostTcpStream
        .env_vars(&["MYSQL_SSL_CA"]) // Env vars visible inside sandbox
        .emit();
}
```

### Destination manifest

```rust
use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::{Feature, WriteMode};

fn main() {
    ManifestBuilder::destination("rapidbyte/dest-snowflake")
        .name("Snowflake Destination")
        .description("Writes data to Snowflake")
        .write_modes(&[
            WriteMode::Append,
            WriteMode::Replace,
            WriteMode::Upsert { primary_key: vec![] },
        ])
        .dest_features(vec![Feature::BulkLoad])
        .allow_runtime_network()
        .emit();
}
```

### Transform manifest

```rust
use rapidbyte_sdk::build::ManifestBuilder;

fn main() {
    ManifestBuilder::transform("rapidbyte/transform-sql")
        .name("SQL Transform")
        .description("Executes SQL queries on Arrow batches using Apache DataFusion")
        .emit();
}
```

### ManifestBuilder methods

| Method | Purpose |
|---|---|
| `allow_runtime_network()` | **Required** if the connector opens TCP connections via `HostTcpStream` |
| `env_vars(&[...])` | Allowlist of environment variables visible inside the Wasm sandbox |
| `sync_modes(&[...])` | Source only: supported sync modes (`FullRefresh`, `Incremental`, `Cdc`) |
| `write_modes(&[...])` | Destination only: supported write modes (`Append`, `Replace`, `Upsert`) |
| `dest_features(vec![...])` | Destination only: feature flags (e.g. `Feature::BulkLoad`, `Feature::ExactlyOnce`) |

## 5. Config Type

The config struct is deserialized from the `config:` section of a pipeline YAML. It must derive `Deserialize` and `ConfigSchema`.

```rust
use rapidbyte_sdk::error::ConnectorError;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// Database hostname
    pub host: String,

    /// Database port
    #[serde(default = "default_port")]
    #[schema(default = 5432)]
    pub port: u16,

    /// Database user
    pub user: String,

    /// Database password (never logged or exposed)
    #[serde(default)]
    #[schema(secret)]
    pub password: String,

    /// Database name
    pub database: String,

    /// Replication slot name (optional, CDC only)
    #[serde(default)]
    pub replication_slot: Option<String>,
}

fn default_port() -> u16 {
    5432
}

impl Config {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if let Some(slot) = self.replication_slot.as_ref() {
            if slot.is_empty() {
                return Err(ConnectorError::config(
                    "INVALID_CONFIG",
                    "replication_slot must not be empty".to_string(),
                ));
            }
            if slot.len() > 63 {
                return Err(ConnectorError::config(
                    "INVALID_CONFIG",
                    format!("replication_slot '{}' exceeds PostgreSQL 63-byte limit", slot),
                ));
            }
        }
        Ok(())
    }
}
```

### Key annotations

| Annotation | Purpose |
|---|---|
| `#[serde(default = "fn_name")]` | Provides a runtime default during deserialization |
| `#[schema(default = value)]` | Declares the default value in the schema metadata (for UI/docs) |
| `#[schema(secret)]` | Marks the field as sensitive -- the host will mask it in logs |
| `#[schema(advanced)]` | Marks the field as advanced (hidden by default in UIs) |
| `#[schema(values("a", "b"))]` | Declares allowed enum values for the field |

## 6. Implementing a Source

A source connector implements the `Source` trait. Here is a walkthrough of each method, based on the real `source-postgres` implementation.

### Entry point

```rust
mod client;
pub mod config;
mod encode;
mod reader;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(source)]
pub struct SourceMydb {
    config: config::Config,
}

impl Source for SourceMydb {
    type Config = config::Config;

    // Methods below...
}
```

### init

Called once at startup. Validate config and return a `ConnectorInfo` describing the connector's capabilities.

```rust
async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
    config.validate()?;
    Ok((
        Self { config },
        ConnectorInfo {
            protocol_version: ProtocolVersion::V4,
            features: vec![],
            default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
        },
    ))
}
```

### validate

Called during `rapidbyte check`. Test connectivity without reading data.

```rust
async fn validate(
    config: &Self::Config,
    _ctx: &Context,
) -> Result<ValidationResult, ConnectorError> {
    let client = client::connect(config)
        .await
        .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;

    client.query_one("SELECT 1", &[]).await.map_err(|e| {
        ConnectorError::transient_network("CONNECTION_TEST_FAILED",
            format!("Connection test failed: {e}"))
    })?;

    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: format!("Connected to {}:{}/{}", config.host, config.port, config.database),
    })
}
```

### discover

Return a `Catalog` describing available streams and their schemas.

```rust
async fn discover(&mut self, _ctx: &Context) -> Result<Catalog, ConnectorError> {
    let client = client::connect(&self.config)
        .await
        .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;

    let streams = discover_streams(&client).await
        .map_err(|e| ConnectorError::transient_db("DISCOVERY_FAILED", e))?;

    Ok(Catalog { streams })
}
```

### read

The main data path. Connect, query, iterate rows, build Arrow batches, and emit them to the host.

```rust
async fn read(
    &mut self,
    ctx: &Context,
    stream: StreamContext,
) -> Result<ReadSummary, ConnectorError> {
    let client = client::connect(&self.config)
        .await
        .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;

    // 1. Query the source
    let rows = client.query("SELECT * FROM users", &[]).await
        .map_err(|e| ConnectorError::transient_db("QUERY_FAILED", format!("{e}")))?;

    // 2. Encode rows into an Arrow RecordBatch
    let batch = encode::rows_to_record_batch(&rows, &columns, &schema)?;

    // 3. Emit the batch to the host
    ctx.emit_batch(&batch)
        .map_err(|e| ConnectorError::internal("EMIT_FAILED", e.message))?;

    // 4. Return summary
    Ok(ReadSummary {
        records_read: batch.num_rows() as u64,
        bytes_read: batch.get_array_memory_size() as u64,
        batches_emitted: 1,
        checkpoint_count: 0,
        records_skipped: 0,
        perf: None,
    })
}
```

In production connectors, the read loop typically uses server-side cursors and streams data in chunks, emitting multiple batches and respecting `stream.limits.max_batch_bytes`.

### close

Called at the end of the pipeline run. Clean up any resources.

```rust
async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> {
    ctx.log(LogLevel::Info, "source-mydb: close");
    Ok(())
}
```

## 7. Implementing a Destination

A destination connector implements the `Destination` trait.

### Entry point

```rust
mod client;
mod config;
mod writer;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(destination)]
pub struct DestMydb {
    config: config::Config,
}

impl Destination for DestMydb {
    type Config = config::Config;

    // Methods below...
}
```

### init

```rust
async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
    Ok((
        Self { config },
        ConnectorInfo {
            protocol_version: ProtocolVersion::V4,
            features: vec![Feature::ExactlyOnce],
            default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
        },
    ))
}
```

### validate

Same pattern as source -- test connectivity and return `ValidationResult`.

```rust
async fn validate(
    config: &Self::Config,
    _ctx: &Context,
) -> Result<ValidationResult, ConnectorError> {
    client::validate(config).await
}
```

### write

The main data path. Receive batches in a loop via `ctx.next_batch()`, write them to the target, and checkpoint periodically.

```rust
async fn write(
    &mut self,
    ctx: &Context,
    stream: StreamContext,
) -> Result<WriteSummary, ConnectorError> {
    let client = client::connect(&self.config)
        .await
        .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;

    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut batches_written: u64 = 0;

    // Receive batches from the host in a loop
    loop {
        match ctx.next_batch(stream.limits.max_batch_bytes) {
            Ok(None) => break, // No more batches
            Ok(Some((schema, batches))) => {
                for batch in &batches {
                    // Write batch rows to the target system
                    let rows = write_batch_to_target(&client, &schema, batch).await?;
                    total_rows += rows;
                    total_bytes += batch.get_array_memory_size() as u64;
                }
                batches_written += 1;
            }
            Err(e) => {
                return Err(ConnectorError::transient_db(
                    "WRITE_FAILED",
                    format!("next_batch failed: {e}"),
                ));
            }
        }
    }

    Ok(WriteSummary {
        records_written: total_rows,
        bytes_written: total_bytes,
        batches_written,
        checkpoint_count: 0,
        records_failed: 0,
        perf: None,
    })
}
```

### Checkpointing

For long-running writes, checkpoint periodically to allow resumption after failures:

```rust
let checkpoint = Checkpoint {
    id: checkpoint_count + 1,
    kind: CheckpointKind::Dest,
    stream: stream.stream_name.clone(),
    cursor_field: None,
    cursor_value: None,
    records_processed: total_rows,
    bytes_processed: total_bytes,
};
let _ = ctx.checkpoint(&checkpoint);
```

### close

```rust
async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> {
    ctx.log(LogLevel::Info, "dest-mydb: close");
    Ok(())
}
```

## 8. Implementing a Transform

A transform connector implements the `Transform` trait. Transforms receive batches from the upstream source (or previous transform), process them, and emit new batches downstream.

### Entry point

```rust
mod config;
mod transform;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(transform)]
pub struct TransformCustom {
    config: config::Config,
}

impl Transform for TransformCustom {
    type Config = config::Config;

    // Methods below...
}
```

### init

```rust
async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
    // Validate config at init time
    Ok((
        Self { config },
        ConnectorInfo {
            protocol_version: ProtocolVersion::V4,
            features: vec![],
            default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
        },
    ))
}
```

### validate

```rust
async fn validate(
    config: &Self::Config,
    _ctx: &Context,
) -> Result<ValidationResult, ConnectorError> {
    // Validate config without side effects
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: "Configuration is valid".to_string(),
    })
}
```

### transform

The core method. Receive batches, transform them, and emit results downstream.

```rust
async fn transform(
    &mut self,
    ctx: &Context,
    stream: StreamContext,
) -> Result<TransformSummary, ConnectorError> {
    let mut records_in: u64 = 0;
    let mut records_out: u64 = 0;
    let mut bytes_in: u64 = 0;
    let mut bytes_out: u64 = 0;
    let mut batches_processed: u64 = 0;

    // Receive batches from upstream
    while let Some((schema, batches)) = ctx.next_batch(stream.limits.max_batch_bytes)? {
        if batches.is_empty() {
            continue;
        }

        let batch_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        records_in += batch_rows;
        bytes_in += batches.iter().map(|b| b.get_array_memory_size() as u64).sum::<u64>();

        // Process each batch (your transformation logic here)
        for batch in &batches {
            let result = process_batch(batch)?;
            if result.num_rows() > 0 {
                records_out += result.num_rows() as u64;
                bytes_out += result.get_array_memory_size() as u64;
                ctx.emit_batch(&result)?;
            }
        }

        batches_processed += 1;
    }

    Ok(TransformSummary {
        records_in,
        records_out,
        bytes_in,
        bytes_out,
        batches_processed,
    })
}
```

The built-in `transform-sql` connector uses Apache DataFusion to execute SQL queries on each batch. It registers incoming batches as a MemTable named `input`, runs the user's SQL, and emits the results.

## 9. Networking

Connectors run inside a Wasm sandbox with **no direct socket access**. All TCP connections must go through the host-proxied `HostTcpStream`.

### Connecting with HostTcpStream

```rust
use rapidbyte_sdk::host_tcp::HostTcpStream;

let stream = HostTcpStream::connect(&config.host, config.port)
    .map_err(|e| format!("Connection failed: {e}"))?;
```

### Example: tokio-postgres connection

This is the pattern used by `source-postgres` and `dest-postgres`:

```rust
use rapidbyte_sdk::host_tcp::HostTcpStream;
use tokio_postgres::{Client, Config as PgConfig, NoTls};

pub async fn connect(config: &crate::config::Config) -> Result<Client, String> {
    let mut pg = PgConfig::new();
    pg.host(&config.host);
    pg.port(config.port);
    pg.user(&config.user);
    if !config.password.is_empty() {
        pg.password(&config.password);
    }
    pg.dbname(&config.database);

    let stream = HostTcpStream::connect(&config.host, config.port)
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

### Requirements

- Your `build.rs` **must** call `.allow_runtime_network()` on the `ManifestBuilder`, or the host will reject TCP connection attempts.
- If your connector needs to access environment variables (e.g. for SSL cert paths), declare them with `.env_vars(&["VAR_NAME"])`.

## 10. Data Flow: Arrow IPC Batches

All data moves between pipeline stages as Arrow IPC batches. The SDK handles the frame lifecycle transparently.

### Sources: Encode and emit

1. Convert your source rows into an Arrow `RecordBatch`
2. Call `ctx.emit_batch(&batch)` to send it to the host

```rust
use rapidbyte_sdk::arrow::array::{Int64Array, StringBuilder};
use rapidbyte_sdk::arrow::datatypes::{DataType, Field, Schema};
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use std::sync::Arc;

// Build schema
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, true),
]));

// Build arrays from your data
let ids = Int64Array::from(vec![1, 2, 3]);
let names = StringBuilder::from(vec![Some("alice"), Some("bob"), None]);

// Create RecordBatch
let batch = RecordBatch::try_new(schema, vec![
    Arc::new(ids),
    Arc::new(names.finish()),
]).expect("schema matches arrays");

// Emit to host
ctx.emit_batch(&batch)?;
```

Under the hood, `emit_batch` performs the frame lifecycle: `frame-new` -> `frame-write` (IPC-encoded data) -> `frame-seal` -> `emit-batch`. This is managed by the SDK; connector authors do not need to call these functions directly.

### Destinations: Receive and decode

1. Call `ctx.next_batch(max_bytes)` in a loop to receive batches from the host
2. The returned `(Arc<Schema>, Vec<RecordBatch>)` contains decoded Arrow data

```rust
loop {
    match ctx.next_batch(stream.limits.max_batch_bytes) {
        Ok(None) => break,       // End of stream
        Ok(Some((schema, batches))) => {
            for batch in &batches {
                // Access columns by index
                let id_col = batch.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("column 0 is Int64");

                for row in 0..batch.num_rows() {
                    let id = id_col.value(row);
                    // ... write to target
                }
            }
        }
        Err(e) => return Err(ConnectorError::internal("BATCH_ERROR", e.message)),
    }
}
```

### Batch compression

The host can optionally compress IPC frames with lz4 or zstd. This is configured at the pipeline level and is transparent to connectors -- the SDK handles compression and decompression in the frame lifecycle.

## 11. Error Handling

All errors returned from connector methods use `ConnectorError`. Use the factory methods to create errors with the right category and retry semantics.

### Factory methods

| Factory | Use when | Retryable |
|---|---|---|
| `ConnectorError::config()` | Invalid configuration | No |
| `ConnectorError::auth()` | Authentication failure | No |
| `ConnectorError::permission()` | Insufficient permissions | No |
| `ConnectorError::rate_limit()` | Rate limited by upstream | Yes (slow backoff) |
| `ConnectorError::transient_network()` | Network timeout, connection refused | Yes |
| `ConnectorError::transient_db()` | Deadlock, lock timeout | Yes |
| `ConnectorError::data()` | Bad data, constraint violation | No |
| `ConnectorError::schema()` | Schema mismatch | No |
| `ConnectorError::internal()` | Unexpected internal error, bugs | No |

### Usage examples

```rust
// Simple error
return Err(ConnectorError::config(
    "INVALID_PORT",
    "port must be between 1 and 65535",
));

// With structured details
return Err(ConnectorError::transient_network(
    "CONNECTION_TIMEOUT",
    format!("Timed out connecting to {}:{}", host, port),
).with_details(serde_json::json!({
    "host": host,
    "port": port,
    "timeout_ms": 5000,
})));

// With commit state (destinations)
return Err(ConnectorError::transient_db(
    "WRITE_FAILED",
    "INSERT failed mid-transaction",
).with_commit_state(CommitState::BeforeCommit));
```

### Builder chain

| Method | Purpose |
|---|---|
| `.with_details(json)` | Attach structured diagnostic info (JSON value) |
| `.with_commit_state(state)` | Record transaction state at time of error |
| `.with_scope(scope)` | Override default error scope |

### CommitState values

| Value | Meaning |
|---|---|
| `CommitState::BeforeCommit` | Error occurred before any data was committed |
| `CommitState::AfterCommitUnknown` | Commit was attempted but outcome is unknown |
| `CommitState::AfterCommitConfirmed` | Some data was committed before the error |

## 12. Testing Your Connector

### Build

```bash
cd connectors/my-source && cargo build
```

This compiles to `target/wasm32-wasip2/debug/my_source.wasm` and generates `my_source.manifest.json` alongside it (via `build.rs`).

### Copy to connector directory

```bash
cp target/wasm32-wasip2/debug/my_source.wasm ../../target/connectors/
cp target/wasm32-wasip2/debug/my_source.manifest.json ../../target/connectors/
```

### Create a test pipeline

Create a YAML file (e.g. `test-pipeline.yaml`):

```yaml
source:
  connector: my-source
  config:
    host: localhost
    port: 5432
    user: postgres
    password: secret
    database: testdb
  streams:
    - name: users
      sync_mode: full_refresh

destination:
  connector: dest-postgres
  config:
    host: localhost
    port: 5432
    user: postgres
    database: destdb
```

### Run

```bash
# Dry run (validate only, no data movement)
just run test-pipeline.yaml --dry-run

# Run with verbose logging
just run test-pipeline.yaml -vv

# Limit records for quick testing
just run test-pipeline.yaml --limit 10
```

### Unit tests

Standard `cargo test` works for pure logic (config validation, encoding, query building). Since connectors target `wasm32-wasip2`, host-integration tests require the full pipeline runner.

```bash
cd connectors/my-source && cargo test --target wasm32-wasip2
```

## 13. Publishing

### Release build

```bash
cd connectors/my-source && cargo build --release
```

The release binary is at `target/wasm32-wasip2/release/my_source.wasm`.

### Strip (optional, reduces size)

```bash
./scripts/strip-wasm.sh target/wasm32-wasip2/release/my_source.wasm
```

### Connector resolution

At runtime, the host resolves connectors by looking for two files in the connector directory:

| File | Purpose |
|---|---|
| `<name>.wasm` | The compiled Wasm component |
| `<name>.manifest.json` | Auto-generated manifest (capabilities, permissions, features) |

The connector directory is determined by `RAPIDBYTE_CONNECTOR_DIR` (defaults to `target/connectors/`).

For example, a connector named `source-mysql` would be resolved as:

```
$RAPIDBYTE_CONNECTOR_DIR/source_mysql.wasm
$RAPIDBYTE_CONNECTOR_DIR/source_mysql.manifest.json
```

Note that the file names use underscores (Rust crate naming convention), not hyphens.
