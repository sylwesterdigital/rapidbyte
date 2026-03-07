# Rapidbyte

Data pipeline engine using WASI component connectors (Wasmtime runtime).

## Project Structure

```
crates/
  rapidbyte-cli/     # CLI binary (`rapidbyte run`, `rapidbyte check`, `rapidbyte dev`)
  rapidbyte-dev/     # Interactive dev shell (REPL with reedline, DataFusion, Arrow workspace)
  rapidbyte-engine/  # Pipeline orchestrator, config parsing, Arrow utils
  rapidbyte-runtime/ # Wasmtime component runtime, host imports, sandbox
  rapidbyte-sdk/     # Connector SDK (protocol types, component host bindings)
  rapidbyte-state/   # State backend (SQLite, Postgres)
  rapidbyte-types/   # Shared protocol types (leaf crate, no internal deps)
connectors/
  source-postgres/   # Source connector (wasm32-wasip2 target)
  dest-postgres/     # Destination connector (wasm32-wasip2 target)
  transform-sql/   # SQL transform connector (wasm32-wasip2, DataFusion)
tests/
  e2e.sh             # End-to-end test orchestrator (Docker PG)
  e2e/               # Individual E2E test scenarios
  lib/               # Shared test helpers
  fixtures/          # SQL seeds, pipeline YAMLs
  bench/             # E2E benchmark harness (Rust binary)
    fixtures/        # Bench SQL seeds, pipeline YAMLs
    lib/             # Criterion-style report generator (Python)
    analyze.py       # Historical results viewer
```

- Workspace has 8 crates (cli, dev, engine, runtime, sdk, sdk/macros, state, types). Connectors are excluded from workspace and build separately.
- Connectors target `wasm32-wasip2` via their `.cargo/config.toml`.

## Crate Dependency Graph

```
types (leaf — no internal deps)
  ├── state    → types
  ├── runtime  → types, state
  ├── sdk      → types
  └── engine   → types, runtime, state
      ├── dev  → engine, runtime, types, state
      └── cli  → engine, runtime, types, dev
```

Pure data types and enums belong in `types`. Host-only config types (YAML parsing) stay in `engine/config/`.

## Commands

```bash
just dev                # Launch interactive dev shell (builds first)
just build              # Build host binary (debug)
just build-connectors   # Build both connectors (wasm32-wasip2, debug)
just release            # Build host binary (release, optimized)
just release-connectors # Build + strip both connectors (wasm32-wasip2, release, LTO+O3)
just test               # cargo test --workspace (host tests)
just e2e                # Full E2E: build, Docker PG, pipeline, verify
just bench --profile medium                        # All connectors, medium profile (50K rows)
just bench postgres --profile medium               # Postgres, medium profile, 50K rows
just bench postgres 100000 --profile small         # Postgres, small profile (~500 B/row), 100K rows
just bench postgres --profile large                # Postgres, large profile (~50 KB/row), 10K rows
just bench postgres --profile small --iters 5      # Postgres, small profile, 5 iterations
just bench-compare main feature                    # Compare benchmarks between refs
cargo bench             # Criterion micro-benchmarks (Arrow codec, state backend)
just fmt                # cargo fmt
just lint               # cargo clippy
```

## CLI Commands

```bash
rapidbyte dev                         # Interactive dev shell (REPL)
```

## CLI Output Flags

```bash
rapidbyte run pipeline.yaml           # Default: spinner + compact summary
rapidbyte run pipeline.yaml -v        # Verbose: per-stream table + stage timings
rapidbyte run pipeline.yaml -vv       # Diagnostic: full timing, WASM overhead, shard skew, CPU/RSS
rapidbyte run pipeline.yaml -q        # Quiet: exit code only, errors on stderr
```

- `-v`/`--verbose` can be stacked: `-v` (detailed), `-vv` (diagnostic)
- `-q`/`--quiet` suppresses all output except errors on stderr
- `--log-level` controls tracing output (orthogonal to verbosity)
- Human-readable output goes to stderr; machine-readable (`@@BENCH_JSON@@`) goes to stdout
- Non-TTY environments (piped output) automatically disable spinner and colors

## Building

```bash
# Host (native)
cargo build

# Connectors (wasm32-wasip2)
cd connectors/source-postgres && cargo build
cd connectors/dest-postgres && cargo build
```

## Key Architecture

- Orchestrator (`crates/rapidbyte-engine/src/orchestrator.rs`) runs source/transform/destination in blocking stages connected with `mpsc::channel`. Connector resolution and state backend creation live in `resolve.rs`.
- Runtime (`crates/rapidbyte-runtime/src/engine.rs`) embeds Wasmtime component model and typed WIT imports/exports.
- Host imports enforce connector-side ACLs for `connect-tcp` and disable direct WASI socket networking.
- State backend is SQLite or Postgres (`crates/rapidbyte-state/`), used for run metadata and cursor/checkpoint state.
- Arrow IPC batches flow between stages via V3 frame transport (host-managed frames with `frame-new`/`frame-write`/`frame-seal`/`emit-batch` lifecycle); optional lz4/zstd compression is handled in host imports.
- SQL transforms (`transform-sql`) run DataFusion inside WASM sandbox, using the same connector infrastructure as source/destination connectors.

## Crate Conventions

- All host crates use `#![warn(clippy::pedantic)]`.
- Each crate has a module table in its `lib.rs` doc comment.
- Top-level re-exports in `lib.rs` for the most common public types.
- Crate-internal structs use `pub(crate)` visibility.
- Error types use `thiserror`, public APIs return `anyhow::Result` or typed errors.

## Notes

- Connectors must use `rapidbyte-sdk` attribute macro (`#[connector(source)]`, `#[connector(destination)]`, `#[connector(transform)]`).
- Network I/O for connectors should go through `HostTcpStream` (`rapidbyte_sdk::host_tcp`) to preserve host permission checks.
- Protocol/documentation source of truth is `docs/PROTOCOL.md`.
