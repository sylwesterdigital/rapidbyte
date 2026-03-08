# Rapidbyte Plugin Protocol (v5)

This document defines the plugin protocol used by Rapidbyte v5 (Wasmtime component runtime).

- Runtime: **Wasmtime component model**
- Plugin target: **`wasm32-wasip2`**
- WIT package: **`rapidbyte:plugin@5.0.0`** (`wit/rapidbyte-plugin.wit`)
- Protocol version string in manifests/PluginInfo: **`5`**

## 1. Architecture

Rapidbyte runs plugins as WebAssembly components and connects stages with Arrow IPC batches:

- Source component exports `source`
- Destination component exports `destination`
- Transform component exports `transform`
- Host imports are provided via WIT `interface host`

There is no pointer/length host ABI (`rb_*`) in v4. All calls are typed through canonical ABI generated from WIT.

## 2. WIT Worlds

Defined in `wit/rapidbyte-plugin.wit`:

- `world rapidbyte-source`
  - imports: `host`
  - exports: `source`
- `world rapidbyte-destination`
  - imports: `host`
  - exports: `destination`
- `world rapidbyte-transform`
  - imports: `host`
  - exports: `transform`
- `world rapidbyte-host`
  - imports: `host` (guest-side SDK bindings)

## 3. Plugin Lifecycle

### 3.1 Source

1. `open(config-json)`
2. optional `discover()`
3. optional `validate()`
4. `run(session, request)` (one stream at a time)
5. `close()` (always called best-effort)

### 3.2 Destination

1. `open(config-json)`
2. optional `validate()`
3. `run(session, request)` (one stream at a time)
4. `close()`

### 3.3 Transform

1. `open(config-json)`
2. optional `validate()`
3. `run(session, request)` (one stream at a time)
4. `close()`

`config-json` is plugin config serialized as JSON.
`request.stream-context-json` is `StreamContext` JSON serialized by host.

## 4. Host Import API (`interface host`)

### 4.1 Batch transport

- `emit-batch(handle: u64) -> result<_, plugin-error>`
  - Source/transform publishes a sealed host frame to the next stage.
- `next-batch() -> result<option<u64>, plugin-error>`
  - Destination/transform pulls the next sealed host frame handle.
  - `none` signals end-of-stream.
- Frame lifecycle:
  - `frame-new(capacity)` allocates a writable frame handle.
  - `frame-write(handle, chunk)` appends bytes into the frame.
  - `frame-seal(handle)` marks the frame immutable and publishable.
  - `frame-len`, `frame-read`, and `frame-drop` support receive/decode and cleanup.

### 4.2 Logging and telemetry

- `log(level: u32, msg: string)`
- `checkpoint(kind: u32, payload-json: string) -> result<_, plugin-error>`
- `metric(payload-json: string) -> result<_, plugin-error>`

`kind` values:
- `0`: source checkpoint
- `1`: destination checkpoint
- `2`: transform checkpoint

### 4.3 State

- `state-get(scope, key) -> result<option<string>, plugin-error>`
- `state-put(scope, key, val) -> result<_, plugin-error>`
- `state-cas(scope, key, expected, new-val) -> result<bool, plugin-error>`

Scopes:
- `0` pipeline
- `1` stream
- `2` plugin-instance

### 4.4 Host-proxied TCP

- `connect-tcp(host, port) -> result<u64, plugin-error>`
- `socket-read(handle, len) -> result<socket-read-result, plugin-error>`
- `socket-write(handle, data) -> result<socket-write-result, plugin-error>`
- `socket-close(handle)`

`socket-read-result`:
- `data(list<u8>)`
- `eof`
- `would-block`

`socket-write-result`:
- `written(u64)`
- `would-block`

## 5. Error Model

All fallible plugin lifecycle and host functions use `plugin-error` from WIT.

Fields:
- category (`config|auth|permission|rate-limit|transient-*|data|schema|internal`)
- scope (`per-stream|per-batch|per-record`)
- code/message
- retry metadata (`retryable`, `retry-after-ms`, `backoff-class`, `safe-to-retry`)
- optional commit state (`before-commit|after-commit-unknown|after-commit-confirmed`)
- optional JSON details

Host preserves plugin retry metadata and maps plugin failures to `PipelineError::Plugin`.

## 6. Data Exchange

- Batch payloads are Arrow IPC stream fragments written into host-managed frames
- Stage handoff uses frame handles (`u64`) via `emit-batch`/`next-batch`
- Optional host-side channel compression (`lz4`/`zstd`) is transparent to plugins
- Stream execution is sequential per plugin instance (`run-*` called once per stream)

### 6.1 Stream Runtime Overrides

`ctx-json` (`StreamContext`) may include host-resolved runtime override hints that
plugins can consume without changing data semantics:

- `effective_parallelism` (`u32?`): effective stream worker fan-out selected by host.
- `partition_strategy` (`mod|range?`): source full-refresh sharding strategy override.
- `copy_flush_bytes_override` (`u64?`): destination COPY flush threshold override.

Override precedence for each knob is:

1. Explicit user pin
2. Host autotune decision
3. Plugin/default fallback

Plugins must treat these overrides as performance hints only and must not change
write semantics, checkpoint semantics, or cursor semantics.

## 7. Checkpoint Coordination

Host stores source and destination checkpoint envelopes and advances persisted
cursors only after correlating source+destination confirmation for the same
stream frontier.

Current behavior:

- Host assigns checkpoint `id` from the batch frontier observed on the host
  data path. Plugin-supplied checkpoint IDs are treated as placeholders.
- Source checkpoints are correlated only with destination checkpoints that
  confirm the same frontier for the same stream.
- Malformed checkpoint payloads fail the host call; they are not silently
  dropped.

This preserves exactly-once semantics for incremental workflows where
destination commit acknowledgment is required before cursor advancement.

## 8. Security and Permissions

Permissions are read from plugin manifests (`permissions`):

- `env.allowed_vars`: only listed env vars are passed into WASI context
- `fs.preopens`: only declared host directories are preopened
- `network.allowed_domains` and `allow_runtime_config_domains` drive host ACL for `connect-tcp`

Current enforcement behavior:
- Host **enforces ACL** on `connect-tcp`
- Host uses non-blocking sockets and returns `would-block` variants
- Direct WASI network usage is disabled in host WASI context; plugins are expected to use host-proxied TCP

## 9. Manifest Compatibility

Host validates plugin manifest role compatibility before run/check/discover:

- Source pipelines require `roles.source`
- Destination pipelines require `roles.destination`
- Transform pipelines require `roles.transform`

Host expects `manifest.protocol_version == "5"`; mismatches fail plugin
resolution before run/check/discover.

## 10. Building Plugins

### 10.1 Language-Agnostic Contract

Any language that compiles to `wasm32-wasip2` and implements the WIT interface can be a Rapidbyte plugin. The WIT file (`wit/rapidbyte-plugin.wit`) is the source of truth — not any particular SDK.

A valid plugin is a WASI component that:
1. Exports one of `source`, `destination`, or `transform`
2. Accepts JSON config via `open(config-json: string)`
3. Exchanges Arrow IPC batches via host imports (`emit-batch`/`next-batch`)
4. Returns structured `plugin-error` records on failure

### 10.2 Rust SDK (recommended for Rust plugins)

The `rapidbyte-sdk` crate provides ergonomic Rust traits and macros:

- `#[plugin(source)]` — exports a source component
- `#[plugin(destination)]` — exports a destination component
- `#[plugin(transform)]` — exports a transform component

The SDK handles WIT binding generation, config JSON deserialization, Tokio runtime management, and error type conversion.

For TCP clients (e.g. `tokio-postgres`), use `rapidbyte_sdk::host_tcp::HostTcpStream` with `connect_raw` to route through host-proxied networking.

### 10.3 Other Languages

Plugins can be written in any language with WASI component support:
- **Go:** Use `wit-bindgen-go` to generate bindings from the WIT file
- **Python:** Use `componentize-py` to compile Python to a WASI component
- **C/C++:** Use `wit-bindgen-c` for C bindings

The plugin must implement the same WIT exports and call the same WIT imports regardless of language.

## SQL Transform (`transform-sql`)

Executes a SQL query against each incoming Arrow batch using Apache DataFusion.
The incoming data is registered as a table named `input`.

**Config:**

```yaml
transforms:
  - use: transform-sql
    config:
      query: "SELECT id, UPPER(name) AS name, age + 1 AS next_age FROM input WHERE active = true"
```

**Supported operations:** column selection, filtering (WHERE), computed columns,
type casting (CAST), string functions (UPPER, LOWER, TRIM, CONCAT), math
expressions, CASE/WHEN, and all standard SQL scalar functions supported by
DataFusion.

Execution notes:

- The transform keeps a long-lived DataFusion `SessionContext` per stream.
- SQL is parsed once during init, then re-planned against the current `input`
  table each batch.
- Result batches are emitted through DataFusion's streaming execution path;
  they are not fully materialized with `collect()`.

**Limitations:** Batch-by-batch execution — cross-batch aggregations (GROUP BY,
DISTINCT, window functions) still operate per-batch, not across the full
stream.

**Table name:** Always `input`. The query must reference `FROM input`.

## 11. Migration Notes

Removed in prior protocols:
- `rb_open`, `rb_run_read`, `rb_run_write`, `rb_close` C-ABI exports
- `rb_allocate` / `rb_deallocate` memory protocol
- `rb_host_*` pointer-based host imports
- Legacy runtime integration

Replaced by:
- Wasmtime component worlds generated from WIT
- typed canonical ABI for all host/guest calls

### Pipeline-Level Permissions & Limits

Pipeline operators can restrict plugin sandbox capabilities and resource usage
beyond what the plugin manifest declares. Add `permissions` and/or `limits`
blocks to any plugin in the pipeline:

    source:
      use: source-postgres
      config: { ... }
      permissions:
        network:
          allowed_hosts: [db.production.internal, "*.analytics.corp"]
        env:
          allowed_vars: [DATABASE_URL]
        fs:
          allowed_preopens: [/data/exports]
      limits:
        max_memory: 128mb
        timeout_seconds: 60

**Permissions** are capability-based (can/cannot access X). Effective permissions
use set intersection — a capability is granted only if both manifest and pipeline
allow it.

**Limits** are quantitative bounds (how much). Effective limit = min(manifest, pipeline).

| Category | Manifest field | Pipeline field | Merge rule |
|----------|---------------|----------------|------------|
| Network | `allowed_domains` | `allowed_hosts` | Set intersection |
| Env vars | `allowed_vars` | `allowed_vars` | Set intersection |
| Filesystem | `preopens` | `allowed_preopens` | Set intersection |
| Memory | `limits.max_memory` | `limits.max_memory` | min() |
| Timeout | `limits.timeout_seconds` | `limits.timeout_seconds` | min() |

- Pipeline can only narrow, never widen.
- Omitted fields leave the manifest values unchanged.
- Empty lists (`[]`) block all access for that category.
- Default limits when neither specifies: memory = unlimited, timeout = 300s.
