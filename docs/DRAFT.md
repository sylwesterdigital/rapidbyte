# Rapidbyte

Single-binary data ingestion engine with Wasm-sandboxed connectors.

Rapidbyte replaces heavyweight managed ELT platforms (Fivetran, Airbyte) with a
single native binary that orchestrates data pipelines through sandboxed WebAssembly
connectors. Connectors run as Wasm components with host-proxied networking and
Arrow IPC batch exchange — no JVM, no Docker, no sidecar processes.

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                  rapidbyte-cli                       │
│     run · check · discover · connectors · scaffold  │
├─────────────────────────────────────────────────────┤
│  rapidbyte-engine       │  rapidbyte-runtime        │
│  ┌─────────────┐        │  ┌───────────────────┐   │
│  │ Orchestrator │───────▶│  │ Wasmtime Component│   │
│  │   Runner     │        │  │ Model + WIT Host  │   │
│  └─────────────┘        │  └───────────────────┘   │
│       │                  │         │                 │
│  Config parsing          │  Sandbox, ACLs, Socket   │
│  Arrow IPC utils         │  AOT cache, Compression  │
├──────────────────────────┼──────────────────────────┤
│  rapidbyte-state         │  rapidbyte-types         │
│  SQLite · Postgres       │  Protocol · Manifest     │
│  Runs · Cursors · DLQ    │  Arrow · Checkpoint      │
├─────────────────────────────────────────────────────┤
│                  rapidbyte-sdk                       │
│  Source · Destination · Transform traits             │
│  #[connector] macro · FrameWriter · HostTcpStream    │
├─────────────────────────────────────────────────────┤
│                Wasm Connectors                       │
│    source-postgres  ·  dest-postgres                 │
└─────────────────────────────────────────────────────┘
```

- **Runtime:** Wasmtime component model, `wasm32-wasip2` target.
- **Protocol:** Version 4. Interface contract: `wit/rapidbyte-connector.wit`.
- **Data format:** Arrow IPC record batches flow between stages via bounded `mpsc` channels.
  V4 frame transport: batches are streamed into host-managed frames (`frame-new` → `frame-write`
  → `frame-seal` → `emit-batch`), eliminating guest-side buffer allocation.
- **State:** Pluggable backend (SQLite bundled, PostgreSQL implemented, S3 planned) for run
  metadata, cursor/checkpoint state, and DLQ records.
- **Networking:** Connectors have no direct WASI socket access. All outbound TCP is mediated
  through host imports (`connect-tcp`, `socket-read`, `socket-write`, `socket-close`) with
  manifest-declared network ACLs.

## Wasm Engine & Sandbox

Wasmtime component model with WIT-typed imports and exports.

- **Component model:** Each connector is a Wasm component exporting one of `source`,
  `destination`, or `transform` interfaces.
- **WIT interface** (`wit/rapidbyte-connector.wit`): defines types, host imports, and three
  connector worlds (`rapidbyte-source`, `rapidbyte-destination`, `rapidbyte-transform`).
- **Host imports:** Frame lifecycle (`frame-new`, `frame-write`, `frame-seal`, `frame-len`,
  `frame-read`, `frame-drop`), batch transport (`emit-batch`, `next-batch`), pipeline
  (`checkpoint`, `metric`, `emit-dlq-record`, `log`), state (`state-get`, `state-put`,
  `state-cas`), plus TCP socket operations.
- **Network ACL:** Derived from connector manifest permissions. Supports domain allowlists,
  runtime config domain derivation, and TLS requirements (required/optional/forbidden).
- **AOT compilation cache:** Controlled via `RAPIDBYTE_WASMTIME_AOT` env var. Pre-compiles
  components to native code and caches them on disk for faster subsequent loads.
- **WASI P2:** `wasmtime_wasi::p2::add_to_linker_sync` provides standard WASI imports.

## SDK & Protocol

The `rapidbyte-sdk` crate provides everything needed to build a connector:

**Traits:**
- `Source` — `init`, `validate`, `discover`, `read`, `close`
- `Destination` — `init`, `validate`, `write`, `close`
- `Transform` — `init`, `validate`, `transform`, `close`

**Export macro:** Attribute macro generates WIT bindings, component glue, manifest
embedding, and config schema in one shot:
```rust
#[connector(source)]
struct MySource;

#[connector(destination)]
struct MyDest;

#[connector(transform)]
struct MyTransform;
```

**Host FFI wrappers** (`host_ffi`): Typed functions for `emit_batch`, `next_batch`,
`checkpoint`, `metric`, `emit_dlq_record`, and key/value state operations. `emit_batch`
uses a `FrameWriter` adapter that streams Arrow IPC encoding directly into host-managed
frames via `frame-write` calls (zero guest-side allocation).

**HostTcpStream** (`host_tcp`): Adapter implementing `AsyncRead + AsyncWrite` over host TCP
imports, enabling `tokio-postgres` `connect_raw` from inside the Wasm sandbox.

**Protocol types:** `PayloadEnvelope`, `StreamContext`, `Checkpoint`, `ReadSummary`,
`WriteSummary`, `TransformSummary`, `ConnectorError` with structured error categories,
retry semantics, and commit state tracking. V4 uses frame-handle batch transport
(`FrameWriter`, `frame-new`/`frame-seal`/`emit-batch` lifecycle).

## Connector Manifest

JSON manifest alongside each `.wasm` binary declaring identity, capabilities, and security:

```json
{
  "manifest_version": "1.0",
  "id": "rapidbyte/source-postgres",
  "name": "PostgreSQL Source",
  "version": "0.1.0",
  "protocol_version": "4",
  "artifact": { "entry_point": "source_postgres.wasm" },
  "permissions": {
    "network": { "tls": "optional", "allow_runtime_config_domains": true },
    "env": { "allowed_vars": ["PGSSLROOTCERT"] }
  },
  "roles": {
    "source": { "supported_sync_modes": ["full_refresh", "incremental"], "features": [] }
  },
  "config_schema": { "$schema": "http://json-schema.org/draft-07/schema#", ... }
}
```

**Fields:** id, name, version, description, author, license, protocol_version, artifact
(entry_point, checksum, min_memory_mb), permissions (network, env, fs), roles
(source/destination/transform/utility capabilities), config_schema (JSON Schema Draft 7).

The host validates config against the schema before instantiating the Wasm guest.

## CLI

```
rapidbyte run <pipeline.yaml>       Execute a data pipeline
rapidbyte run <pipeline.yaml> --dry-run --limit 100
                                    Preview mode: pull N records, run transforms, print results
rapidbyte check <pipeline.yaml>     Validate config, manifests, and connectivity
rapidbyte discover <pipeline.yaml>  Discover available streams from a source
rapidbyte connectors                List available connector plugins
rapidbyte scaffold <name>           Scaffold a new connector project
```

Global flags: `--log-level` (error/warn/info/debug/trace), `--dry-run`, `--limit N`.
Also respects `RUST_LOG`.

### Dry Run & Local Testing

`--dry-run --limit N` connects to the source, pulls N records, pushes them through
transforms, and outputs the resulting Arrow batch as JSON or table format to stdout —
without writing to the destination. Gives data engineers instant feedback on schema
evolution and transform configs without touching production.

## Interactive Dev Shell (`rapidbyte dev`)

A terminal-based REPL for exploring, testing, and debugging data pipelines interactively.
Connects to real sources, streams data into an in-memory Arrow workspace, lets you query
with SQL, test transforms, profile data quality, diff against destinations, and export
working pipelines — all without writing YAML first.

### Command Language

Dot-commands for pipeline operations, raw SQL for data queries:

```
rapidbyte dev

rb> .source postgres --host localhost --port 5432 --user app --database mydb
Connected to PostgreSQL 16.2

rb> .tables
 public.users          | 12,847 rows
 public.orders         | 89,231 rows
 public.products       |  1,204 rows

rb> .stream public.users --limit 1000
Streamed 1,000 rows → workspace:users (12 columns, 847 KB)

rb> SELECT id, email, created_at FROM users WHERE created_at > '2026-01-01' LIMIT 10;
┌────┬──────────────────────┬─────────────────────┐
│ id │ email                │ created_at          │
├────┼──────────────────────┼─────────────────────┤
│ 42 │ alice@example.com    │ 2026-01-15 10:30:00 │
│ 87 │ bob@example.com      │ 2026-02-01 14:22:00 │
└────┴──────────────────────┴─────────────────────┘

rb> .check users --rules 'not_null: email, regex: {email: "^.+@.+$"}'
✓ not_null(email): 1,000/1,000 pass
✗ regex(email):    997/1,000 pass (3 failures)

rb> .profile users.email
type: Utf8, nulls: 0, unique: 994, min_len: 8, max_len: 47

rb> .dest postgres --host warehouse --database analytics --schema raw
Connected to destination

rb> .diff users
+ 142 new rows  ~ 38 changed rows  - 0 deleted rows

rb> .push users --write-mode upsert --primary-key id
Pushed 1,000 rows (180 upserted)

rb> .export my-pipeline.yaml
Exported pipeline: source(postgres) → dest(postgres), 1 stream, upsert mode
```

### Key Capabilities

- **Source exploration** — connect, list tables, describe schemas, stream samples
- **Arrow workspace** — in-memory scratch pad holding streamed data as Arrow `RecordBatch`es
- **SQL queries** — DataFusion SQL engine over workspace tables (SELECT, JOIN, aggregate)
- **Transform testing** — apply and chain transforms interactively, save SQL as named transforms
- **Data quality** — not-null/regex/range checks, column profiling, histograms
- **Destination testing** — connect, diff against target, push with write mode preview
- **Pipeline export** — serialize the current session (source + transforms + dest) as pipeline YAML
- **Step-through debugger** — load an existing pipeline YAML and step through stages one at a time
- **Incremental sync testing** — simulate cursor-based reads with `.stream --cursor-field`
- **Context-aware autocomplete** — table names after `FROM`, columns after alias, transforms after `.apply`

### Architecture

- **REPL:** `reedline` with custom completer and syntax highlighter
- **SQL engine:** DataFusion over Arrow workspace tables
- **Connectors:** Reuses existing Wasm connectors via `rapidbyte-runtime`
- **State:** Ephemeral in-memory workspace, optional persist to disk

## Pipeline YAML

```yaml
version: "1.0"
pipeline: pg_to_pg

source:
  use: source-postgres
  config:
    host: localhost
    port: 5432
    user: app
    password: ${DB_PASS}                           # env var substitution
    # password: aws-secrets://prod/db-pass         # cloud secret URI (planned)
    # password: 1password://vaults/infra/db-pass   # 1Password URI (planned)
    database: mydb
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
      columns: [id, name, email, updated_at]    # projection pushdown

transforms:                                       # optional, zero or more
  - use: transform-mask
    config: { fields: [email] }

  - use: rapidbyte/transform-validate             # data contract enforcement (planned)
    config:
      rules:
        - assert_not_null: user_id
        - assert_regex: { field: email, pattern: "^.+@.+\\..+$" }
      on_fail: dlq                                # send bad rows to DLQ

  - use: rapidbyte/transform-sql                  # in-flight SQL via DataFusion (planned)
    config:
      query: |
        SELECT user_id, count(order_id) as total_orders, sum(amount) as ltv
        FROM batch
        GROUP BY user_id

destination:
  use: dest-postgres
  config:
    host: warehouse.internal
    port: 5432
    user: loader
    database: analytics
    schema: raw
  write_mode: upsert
  primary_key: [id]
  on_data_error: dlq                              # skip | fail | dlq
  schema_evolution:
    new_column: add                               # add | ignore | fail
    removed_column: ignore                        # ignore | fail
    type_change: coerce                           # coerce | fail | null
    nullability_change: allow                     # allow | fail

state:
  backend: sqlite                                 # sqlite | postgres | s3 (planned)
  connection: /var/lib/rapidbyte/state.db
  # backend: s3                                   # ephemeral-friendly (planned)
  # connection: s3://my-bucket/rapidbyte/state

resources:
  parallelism: 4
  max_batch_bytes: 64mb
  max_inflight_batches: 16
  checkpoint_interval_bytes: 64mb
  checkpoint_interval_rows: 0                     # 0 = disabled
  checkpoint_interval_seconds: 0                  # 0 = disabled
  max_retries: 3
  compression: lz4                                # lz4 | zstd | null
```

## Sync Modes

| Mode | Description |
|------|-------------|
| `full_refresh` | Read entire table, no cursor tracking |
| `incremental` | Cursor-based delta reads; resumes from last checkpoint value |
| `cdc` | PostgreSQL logical replication via `pgoutput` protocol |

## Write Modes

| Mode | Description |
|------|-------------|
| `append` | Insert all records |
| `replace` | Truncate target table, then insert |
| `upsert` | Insert or update by `primary_key` using `ON CONFLICT ... DO UPDATE` |

## CDC (Change Data Capture)

PostgreSQL logical replication via the native `pgoutput` protocol.

### `pgoutput`

`pgoutput` is the native binary logical replication protocol used by standard PostgreSQL
logical replication:

- **Binary protocol** — no text parsing, preserves rich type information.
- **Native support** — no extension installation required, works with all managed PG
  services (RDS, Cloud SQL, Aurora, Supabase).
- **Relation messages** — the protocol sends column types and names inline, enabling
  automatic schema discovery from the WAL stream itself.
- **Streaming transactions** — supports large transactions streamed in chunks rather
  than buffered entirely in memory.
- Adds `_rb_op` metadata column with operation type (`insert`/`update`/`delete`).
- Tracks WAL LSN as cursor for checkpoint recovery.
- Batches changes into Arrow record batches (10,000 rows per batch).
- Destructive slot consumption requires checkpoint safety for exactly-once delivery.

### CDC Edge Cases

**Schema evolution mid-stream:** When `ALTER TABLE` occurs during an active replication
stream, `pgoutput` sends a new Relation message with the updated schema. Rapidbyte must
detect this and dynamically adapt the Arrow schema between batches without crashing the
pipeline. The destination applies schema evolution policies to handle the DDL delta.

**TOAST / out-of-line data:** PostgreSQL drops large column values (e.g., big JSON blobs)
from WAL unless `REPLICA IDENTITY FULL` is set on the table. When TOAST values are
unchanged in an UPDATE, the WAL contains a sentinel "unchanged toast" marker. Rapidbyte
must either fetch the missing value from the source table or emit a structured warning
so operators know data was dropped. Default behavior: log warning + emit DLQ record.

## Schema Evolution

Configurable per-pipeline policy with four dimensions:

| Dimension | Options | Default |
|-----------|---------|---------|
| `new_column` | `add`, `ignore`, `fail` | `add` |
| `removed_column` | `ignore`, `fail` | `ignore` |
| `type_change` | `coerce`, `fail`, `null` | `fail` |
| `nullability_change` | `allow`, `fail` | `allow` |

Policies are passed to connectors via `StreamPolicies` in `StreamContext`. The destination
connector applies them during DDL evolution and batch writes.

## Dead Letter Queue

Records that fail during writing are routed to a DLQ instead of failing the pipeline
(when `on_data_error: dlq` is configured):

- `DlqRecord`: stream_name, record_json, error_message, error_category, failed_at timestamp.
- Maximum 10,000 records held in memory per run (prevents unbounded growth).
- Persisted to the state backend at the end of each run.

## Secrets Management (GitOps Native)

Pipeline YAML supports environment variable substitution (`${DB_PASS}`). Planned:
direct secret resolution from cloud providers via URIs at runtime.

**Supported resolvers (planned):**

| URI scheme | Provider |
|------------|----------|
| `aws-secrets://secret-name` | AWS Secrets Manager |
| `aws-ssm://parameter-name` | AWS SSM Parameter Store |
| `gcp-secrets://project/secret/version` | GCP Secret Manager |
| `1password://vault/item/field` | 1Password |
| `vault://secret/path#key` | HashiCorp Vault |

This enables committing `pipeline.yaml` to Git with zero secrets in the file.
Rapidbyte resolves URIs at startup before passing config to connectors.

## In-Flight Data Validation (Data Contracts)

A built-in validation transform enforces data contracts on Arrow batches in-flight:

```yaml
transforms:
  - use: rapidbyte/transform-validate
    config:
      rules:
        - assert_not_null: user_id
        - assert_not_null: email
        - assert_regex: { field: email, pattern: "^.+@.+\\..+$" }
        - assert_range: { field: age, min: 0, max: 150 }
        - assert_unique: order_id
      on_fail: dlq    # dlq | fail | skip
```

Because Rapidbyte uses Arrow, rule evaluation is vectorized over column arrays —
computationally trivial even at high throughput. Failing rows route to the DLQ while
the pipeline continues processing valid records.

## In-Flight SQL Transforms (DataFusion)

DataFusion embedded as a transform connector enables SQL transforms on Arrow batches
as they flow through the pipeline:

```yaml
transforms:
  - use: rapidbyte/transform-sql
    config:
      query: |
        SELECT user_id, count(order_id) as total_orders, sum(amount) as ltv
        FROM batch
        GROUP BY user_id
```

Arrow IPC batches are registered as DataFusion table providers. The SQL executes
in-memory, producing new Arrow batches that continue downstream. This eliminates
the dual cost of ELT pipelines — move data with Fivetran, then transform in Snowflake
with dbt. Rapidbyte aggregates, filters, and reshapes data before it reaches the
warehouse, saving significant compute costs.

## Cloud-Native State Backends

SQLite is the default state backend — ideal for single-node and local development.
For ephemeral environments (GitHub Actions, AWS Lambda, K8s CronJobs), pluggable
backends allow state to survive node termination:

| Backend | Status | Use case |
|---------|--------|----------|
| `sqlite` | Implemented | Local, single-node, dev |
| `postgres` | Implemented | Teams with existing PG infrastructure |
| `s3` | Planned (P0) | Ephemeral CI/CD, serverless, K8s Jobs |

```yaml
state:
  backend: s3
  connection: s3://my-bucket/rapidbyte/state
```

State operations (`get`/`put`/`cas`) are abstracted behind a `StateBackend` trait.
S3 backend uses conditional writes (`If-None-Match` / ETags) for compare-and-set
to prevent concurrent pipeline runs from corrupting checkpoints.

## Projection Pushdown

The `columns` field on each stream in the pipeline YAML specifies which columns to read:

```yaml
streams:
  - name: users
    sync_mode: full_refresh
    columns: [id, name, email]
```

The source connector receives `selected_columns` in `StreamContext` and constructs queries
selecting only the specified columns. Column names are validated against PostgreSQL identifier
rules to prevent SQL injection.

## Pipeline Parallelism

Multiple streams execute concurrently within a single pipeline run:

- `resources.parallelism` controls the maximum number of concurrent streams (default: 1).
- Implemented via `tokio::sync::Semaphore` for concurrency control.
- Each stream gets its own source → [transform...] → destination channel pipeline.
- Channels are bounded by `max_inflight_batches` for backpressure.

## Compression

Arrow IPC batches transferred between pipeline stages can be compressed:

- **LZ4** (`lz4_flex`): Fast compression, lower ratio. Good default for most workloads.
- **Zstd** (level 1): Better ratio, slightly higher CPU. Good for large batches.
- Configured via `resources.compression` in pipeline YAML.
- Applied transparently by host imports on emit/receive.

## Error Handling & Retries

Connectors return structured `ConnectorError` with:
- **Category:** config, auth, permission, rate_limit, transient_network, transient_db, data, schema, frame, internal.
- **Scope:** per-stream, per-batch, per-record.
- **Retry semantics:** retryable flag, retry_after_ms hint, backoff_class (fast/normal/slow),
  safe_to_retry flag, commit_state (before_commit/after_commit_unknown/after_commit_confirmed).

The orchestrator retries transient errors up to `max_retries` times with exponential backoff.

## Observability

- **Structured logging:** `tracing` + `tracing-subscriber` with `--log-level` CLI flag.
- **Connector metrics:** records_read/written, bytes_read/written (emitted via `metric` host import).
- **Host timing breakdown:** connect, query, fetch, encode, decode, flush, commit, vm_setup,
  emit_batch, next_batch, compress, decompress — all tracked per-run.
- **Run tracking:** Each pipeline run recorded in the state backend with start/end time,
  status, records read/written, and error messages.

## Connectors

### Implemented

| Connector | Roles | Notes |
|-----------|-------|-------|
| `source-postgres` | Source | Snapshot, incremental cursor, CDC. `tokio-postgres` over `HostTcpStream`. |
| `dest-postgres` | Destination | INSERT and COPY modes. Batch commits. DDL auto-creation. Schema evolution. |

### Built-in Transforms

| Transform | Status | Notes |
|-----------|--------|-------|
| `transform-sql` | Implemented | DataFusion SQL on Arrow batches in-flight |
| `transform-validate` | Implemented | Data contracts: not-null, regex, range, unique |
| `transform-mask` | Planned (P1) | Field masking / PII redaction |

### Connector Roadmap

| Connector | Priority | Notes |
|-----------|----------|-------|
| `source-mysql` | P1 | MySQL binlog CDC |
| `dest-s3-parquet` | P1 | Parquet files on S3 |
| `dest-bigquery` | P1 | BigQuery Storage Write API |
| `dest-snowflake` | P2 | Snowflake PUT + COPY |
| `dest-duckdb` | P2 | Embedded analytics |
| `source-http` | P2 | REST/webhook source |
| `source-s3` | P2 | S3 file source (CSV, Parquet, JSON) |

## Roadmap

### Implemented (current)

- Wasmtime component model runtime with WIT interface (V4)
- V4 frame transport (host-managed frames, streaming FrameWriter, zero guest allocation)
- Source, Destination, Transform connector lifecycle
- Connector manifests with config schema validation
- Pipeline YAML configuration
- Three sync modes: full_refresh, incremental, CDC (`pgoutput` logical replication)
- Three write modes: append, replace, upsert
- Schema evolution policies (4 dimensions)
- Dead letter queue with SQLite persistence
- Projection pushdown (column selection)
- Pipeline parallelism (semaphore-based)
- LZ4 and Zstd channel compression
- AOT compilation cache
- Structured error handling with retry semantics
- CLI: run, check, discover, connectors, scaffold
- Host-proxied TCP networking with ACLs
- Connector metrics and host timing breakdown
- SQLite and PostgreSQL state backends for checkpoints and run history
- Dry run mode (`--dry-run --limit N`) for local pipeline preview
- In-flight SQL transform connector (`transform-sql`) on Arrow batches
- In-flight validation transform connector (`transform-validate`) with fail/skip/dlq policy handling
- Full PG type correctness (timestamp, date, bytea, json, uuid, numeric, etc.)
- Modular crate architecture (types, state, runtime, engine, sdk, cli)
- E2E test suite and benchmarking scripts

### Critical path (P0)

- **S3 state backend** — enables ephemeral deployments (CI/CD, Lambda, K8s Jobs)
- **Interactive dev shell** (`rapidbyte dev`) — exploratory workflow for fast connector/pipeline iteration
- **Secrets management** — resolve secret URIs at startup (AWS/GCP/Vault/1Password)

### Near-term (P1)

- TUI progress display during pipeline runs
- OCI registry for connector distribution (`rapidbyte pull`)
- Additional connectors: MySQL source, S3/Parquet dest, BigQuery dest
- OpenTelemetry metrics and trace export
- DLQ replay and routing to destination tables

### Future (P2)

- Pipeline hooks / middleware (pre-batch, post-commit callbacks)
- Managed cloud service with scheduling and monitoring
- Connector marketplace
- Distributed tracing across pipeline stages
- Additional connectors: Snowflake, DuckDB, HTTP source, S3 source
- Prometheus metrics endpoint
