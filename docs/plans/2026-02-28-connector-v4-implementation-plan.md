# Connector Protocol v4 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the connector protocol with a single standardized v4 API across WIT, runtime, SDK, and first-party connectors.

**Architecture:** Define one canonical WIT contract (`rapidbyte:connector@4.0.0`) with typed platform contracts and unified lifecycle/run methods, then align runtime bindings, SDK traits/macros/FFI, and connector implementations to the new contract. Remove role-specific naming drift by collapsing execution calls into a single `run` shape and shared request/summary types.

**Tech Stack:** Rust, WIT component model (`wit-bindgen`, `wasmtime::component::bindgen`), Tokio, Arrow IPC

---

### Task 1: Replace WIT with canonical v4 contract

**Files:**
- Modify: `wit/rapidbyte-connector.wit`
- Test: `crates/rapidbyte-runtime/src/bindings.rs` (compile-time bindgen consumer)

**Step 1: Write the failing test**

Add a compile-only assertion test in runtime that references new world exports:

```rust
#[test]
fn v4_world_bindings_exist() {
    let _ = std::any::TypeId::of::<source_bindings::RapidbyteSource>();
    let _ = std::any::TypeId::of::<dest_bindings::RapidbyteDestination>();
    let _ = std::any::TypeId::of::<transform_bindings::RapidbyteTransform>();
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-runtime v4_world_bindings_exist -- --nocapture`
Expected: FAIL due to missing updated WIT symbols/world surface.

**Step 3: Write minimal implementation**

Replace WIT package/version and interfaces to include:

```wit
package rapidbyte:connector@4.0.0;

interface core-types { ... }
interface stream-types { ... }
interface telemetry { ... }
interface host-io { ... }
interface source { open; validate; run; close; }
interface destination { open; validate; run; close; }
interface transform { open; validate; run; close; }
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-runtime v4_world_bindings_exist -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add wit/rapidbyte-connector.wit crates/rapidbyte-runtime/src/bindings.rs
git commit -m "feat(protocol): define canonical connector v4 WIT contract"
```

### Task 2: Align runtime bindings and host trait impls to v4 names/types

**Files:**
- Modify: `crates/rapidbyte-runtime/src/bindings.rs`
- Modify: `crates/rapidbyte-runtime/src/lib.rs`
- Modify: `crates/rapidbyte-engine/src/runner.rs`
- Test: `crates/rapidbyte-engine/src/runner.rs` tests

**Step 1: Write the failing test**

Add/update a runner test that calls connector lifecycle via `run` instead of role-specific `run-*`.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-engine validate_connector -- --nocapture`
Expected: FAIL because runtime still expects old exported method names.

**Step 3: Write minimal implementation**

Update runtime glue and runner call sites:

```rust
let iface = bindings.rapidbyte_connector_source();
iface.call_run(&mut store, session, run_request)
```

Update host import method naming in host trait implementations:

- `emit_batch` -> `batch_emit`
- `next_batch` -> `batch_next`
- `checkpoint` -> `telemetry_checkpoint`
- `metric` -> `telemetry_metric`
- `emit_dlq_record` -> `telemetry_dlq`
- `connect_tcp`/`socket_*` -> `tcp_*`

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-engine validate_connector -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-runtime/src/bindings.rs crates/rapidbyte-runtime/src/lib.rs crates/rapidbyte-engine/src/runner.rs
git commit -m "refactor(runtime): consume v4 run lifecycle and host io names"
```

### Task 3: Standardize SDK host FFI around v4 host-io contract

**Files:**
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs`
- Modify: `crates/rapidbyte-sdk/src/context.rs`
- Test: `crates/rapidbyte-sdk/src/context.rs` tests

**Step 1: Write the failing test**

Add/update `context` tests to exercise renamed host methods through stubs (batch/state/telemetry/tcp).

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-sdk context:: -- --nocapture`
Expected: FAIL from missing/renamed host binding methods.

**Step 3: Write minimal implementation**

Update generated host world and call sites:

```rust
bindings::rapidbyte::connector::host_io::batch_emit(handle)
bindings::rapidbyte::connector::host_io::telemetry_metric(&event)
bindings::rapidbyte::connector::host_io::tcp_connect(host, port)
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-sdk context:: -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/host_ffi.rs crates/rapidbyte-sdk/src/context.rs
git commit -m "refactor(sdk): align host ffi with connector v4 host-io contract"
```

### Task 4: Unify SDK connector traits to canonical `run` API

**Files:**
- Modify: `crates/rapidbyte-sdk/src/connector.rs`
- Modify: `crates/rapidbyte-sdk/src/lib.rs`
- Test: `crates/rapidbyte-sdk/src/connector.rs` tests

**Step 1: Write the failing test**

Update trait-shape compile tests so each role implements `run` and no longer requires `read/write/transform` methods.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-sdk test_trait_shapes_compile -- --nocapture`
Expected: FAIL because traits still require old role-specific method names.

**Step 3: Write minimal implementation**

Refactor role traits to standard lifecycle method names plus single `run` signature with typed request:

```rust
async fn run(
    &mut self,
    ctx: &Context,
    request: RunRequest,
) -> Result<RunSummary, ConnectorError>;
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-sdk test_trait_shapes_compile -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/connector.rs crates/rapidbyte-sdk/src/lib.rs
git commit -m "refactor(sdk): standardize connector traits on lifecycle + run"
```

### Task 5: Update `#[connector(...)]` macro generation for v4 lifecycle

**Files:**
- Modify: `crates/rapidbyte-sdk/macros/src/connector.rs`
- Test: `crates/rapidbyte-sdk/macros/tests/`

**Step 1: Write the failing test**

Add/update macro expansion tests to assert generated guest impl exports `open/validate/run/close` and no `run-read|run-write|run-transform`.

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-sdk-macros -- --nocapture`
Expected: FAIL because macro still emits role-specific run function names.

**Step 3: Write minimal implementation**

Refactor generated guest code to dispatch role-specific connector trait implementation through a shared `run` entry point and typed request/summary conversion.

**Step 4: Run test to verify it passes**

Run: `cargo test -p rapidbyte-sdk-macros -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/macros/src/connector.rs crates/rapidbyte-sdk/macros/tests
git commit -m "refactor(macros): generate canonical v4 lifecycle + run exports"
```

### Task 6: Migrate `source-postgres` to v4 SDK API

**Files:**
- Modify: `connectors/source-postgres/src/main.rs`
- Modify: `connectors/source-postgres/src/reader.rs`
- Modify: `connectors/source-postgres/src/discovery.rs`
- Test: `connectors/source-postgres/src/main.rs` tests

**Step 1: Write the failing test**

Add/update source connector tests to validate `run` dispatch supports `discover` and `read` phases from typed `run-request`.

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path connectors/source-postgres/Cargo.toml -- --nocapture`
Expected: FAIL due to trait method signature mismatch.

**Step 3: Write minimal implementation**

Implement `Source::run` with phase dispatch and produce canonical `RunSummary`.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path connectors/source-postgres/Cargo.toml -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/source-postgres/src/main.rs connectors/source-postgres/src/reader.rs connectors/source-postgres/src/discovery.rs
git commit -m "refactor(source-postgres): adopt connector v4 run contract"
```

### Task 7: Migrate `dest-postgres` and `transform-sql` to v4 SDK API

**Files:**
- Modify: `connectors/dest-postgres/src/main.rs`
- Modify: `connectors/transform-sql/src/main.rs`
- Modify: `connectors/transform-sql/src/transform.rs`
- Test: connector-local tests in both crates

**Step 1: Write the failing test**

Update tests to assert both connectors implement `run` and return canonical run summary.

**Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path connectors/dest-postgres/Cargo.toml -- --nocapture`
Run: `cargo test --manifest-path connectors/transform-sql/Cargo.toml -- --nocapture`
Expected: FAIL from old trait method names.

**Step 3: Write minimal implementation**

Implement `Destination::run` and `Transform::run` and map role-specific counters into canonical summary.

**Step 4: Run test to verify it passes**

Run: `cargo test --manifest-path connectors/dest-postgres/Cargo.toml -- --nocapture`
Run: `cargo test --manifest-path connectors/transform-sql/Cargo.toml -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add connectors/dest-postgres/src/main.rs connectors/transform-sql/src/main.rs connectors/transform-sql/src/transform.rs
git commit -m "refactor(connectors): migrate destination and transform to v4 run lifecycle"
```

### Task 8: Update protocol documentation and run full verification

**Files:**
- Modify: `docs/PROTOCOL.md`
- Modify: `docs/BUILD.md` (if build notes mention old lifecycle names)

**Step 1: Write the failing test**

Add/adjust doc assertions in code comments/examples to reference `open/validate/run/close` and typed platform contracts.

**Step 2: Run test to verify it fails**

Run: `rg -n "run-read|run-write|run-transform|@2.0.0|@3.0.0" docs crates connectors`
Expected: Hits found in docs and/or code before final cleanup.

**Step 3: Write minimal implementation**

Update protocol docs to v4, new names, and typed contract examples.

**Step 4: Run test to verify it passes**

Run:

```bash
just fmt
just lint
just test
just build-connectors
rg -n "run-read|run-write|run-transform|@2.0.0|@3.0.0" docs crates connectors
```

Expected:

- Format/lint/tests/build complete successfully.
- Search returns no legacy protocol identifiers (except intentional historical references in migration notes).

**Step 5: Commit**

```bash
git add docs/PROTOCOL.md docs/BUILD.md
git commit -m "docs(protocol): document canonical connector v4 api"
```
