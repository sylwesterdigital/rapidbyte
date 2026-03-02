# Rapidbyte

Single-binary data pipeline engine with Wasm-sandboxed connectors.

Rapidbyte replaces managed ETL platforms (Fivetran, Airbyte) with a single native
binary. Connectors run as WASI components inside a Wasmtime sandbox with
host-proxied networking. Data flows between stages as Arrow IPC batches — no JVM,
no Docker, no sidecar processes.

## Features

- Full refresh, incremental, and CDC sync modes
- Append, replace, and upsert write modes
- In-flight SQL transforms via DataFusion
- Data validation with dead letter queue for bad rows
- Schema evolution policies
- LZ4/Zstd batch compression
- Projection pushdown (column selection)
- Host-proxied networking with ACLs (no raw sockets in guest)
- SQLite or Postgres state backend
- Dry-run mode with `--limit` for instant feedback

## Quick Start

**Prerequisites:** Rust 1.75+, [`just`](https://github.com/casey/just)

Build the host binary and connectors:

```bash
just build && just build-connectors
```

Create a pipeline YAML file:

```yaml
version: "1.0"
pipeline: my_pipeline

source:
  use: source-postgres
  config:
    host: localhost
    port: 5432
    user: app
    password: ${PG_PASSWORD}
    database: mydb
  streams:
    - name: users
      sync_mode: full_refresh
    - name: orders
      sync_mode: incremental
      cursor_field: updated_at

destination:
  use: dest-postgres
  config:
    host: warehouse
    port: 5432
    user: loader
    password: ${DEST_PG_PASSWORD}
    database: analytics
    schema: raw
  write_mode: append

state:
  backend: sqlite
```

Run it:

```bash
rapidbyte run pipeline.yaml
```

Validate config and connectivity without moving data:

```bash
rapidbyte check pipeline.yaml
```

Preview the first 100 rows without writing to the destination:

```bash
rapidbyte run pipeline.yaml --dry-run --limit 100
```

## CLI

| Command | Description |
|---------|-------------|
| `rapidbyte run <pipeline.yaml>` | Execute a data pipeline |
| `rapidbyte check <pipeline.yaml>` | Validate config, manifests, and connectivity |
| `rapidbyte discover <pipeline.yaml>` | Discover available streams from a source |
| `rapidbyte connectors` | List available connector plugins |
| `rapidbyte scaffold <name>` | Scaffold a new connector project |

**Global flags:** `--log-level` (error/warn/info/debug/trace), `--dry-run`, `--limit N`

## Pipeline Configuration

```yaml
version: "1.0"
pipeline: example

source:
  use: source-postgres          # Connector name (resolved from ./connectors/)
  config: { ... }               # Connector-specific config (validated against manifest schema)
  streams:
    - name: users
      sync_mode: incremental    # full_refresh | incremental
      cursor_field: updated_at  # Required for incremental
      columns: [id, email]      # Optional projection pushdown

transforms:
  - use: transform-sql
    config:
      query: "SELECT id, lower(email) AS email FROM input"
  - use: transform-validate
    config:
      rules:
        not_null: [id, email]

destination:
  use: dest-postgres
  config: { ... }
  write_mode: upsert            # append | replace | upsert
  primary_key: [id]             # Required for upsert
  on_data_error: dlq            # fail | skip | dlq
  schema_evolution: additive    # strict | additive | permissive

state:
  backend: sqlite               # sqlite | postgres
  connection: state.db          # Optional (sqlite default: ./rapidbyte_state.db)

resources:
  max_memory: 256mb
  max_batch_bytes: 64mb
  compression: lz4              # none | lz4 | zstd
```

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the full connector protocol specification.

## Connectors

| Connector | Description |
|-----------|-------------|
| `source-postgres` | Read from PostgreSQL (full refresh, incremental) |
| `dest-postgres` | Write to PostgreSQL (append, replace, upsert) |
| `transform-sql` | SQL transforms via DataFusion |
| `transform-validate` | Row-level data validation with DLQ support |

Connectors target `wasm32-wasip2` and are built separately from the host binary:

```bash
cd connectors/source-postgres && cargo build
```

Or build all connectors at once:

```bash
just build-connectors
```

## Architecture

```
Source ──► Transform(s) ──► Destination
       mpsc::channel      mpsc::channel
```

- **Orchestrator** runs stages sequentially, connected with bounded `mpsc::channel`
- **Runtime** embeds Wasmtime component model with typed WIT imports/exports (`rapidbyte:connector@4.0.0`)
- **Arrow IPC** batches flow via host-managed frames (`frame-new` / `frame-write` / `frame-seal` / `emit-batch`) — zero guest-side buffer allocation
- **Network ACL** enforces connector-declared permissions; no direct WASI socket access

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) and [`docs/CODING_STYLE.md`](docs/CODING_STYLE.md) for details.

## Development

```bash
just build              # Build host binary
just build-connectors   # Build all connectors (wasm32-wasip2)
just test               # Run workspace tests
just lint               # Clippy
just fmt                # Format
just e2e                # End-to-end tests (requires Docker)
just bench              # Benchmarks
just scaffold my-src    # Scaffold a new connector
```

## License

Apache-2.0
