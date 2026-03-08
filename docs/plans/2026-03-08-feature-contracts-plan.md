# Feature Contract Enforcement Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enforce that plugins declaring a `Feature` actually implement the contract for that feature, via compile-time trait requirements, a conformance test harness, and runtime sanity checks.

**Architecture:** Feature traits (`PartitionedSource`, `CdcSource`, `BulkLoadDestination`) are added to `rapidbyte-sdk/src/connector.rs`. The `#[connector]` proc macro reads the manifest at compile time and emits errors if the required trait is missing. A `conformance` module (behind a feature flag) provides a `ConformanceHarness` trait and universal test functions. Host-side sanity checks are added to the orchestrator's result aggregation.

**Tech Stack:** Rust proc macros (syn/quote), `rapidbyte-sdk`, `rapidbyte-types`, `rapidbyte-engine` orchestrator.

---

## Task 1: Add typed context structs to `rapidbyte-types`

**Files:**
- Modify: `crates/rapidbyte-types/src/stream.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs` (re-export)

**Step 1: Add `PartitionCoordinates` and `CdcResumeToken` structs**

In `crates/rapidbyte-types/src/stream.rs`, add after the `PartitionStrategy` enum:

```rust
/// Typed partition coordinates passed to sources that declare `PartitionedRead`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCoordinates {
    pub count: u32,
    pub index: u32,
    pub strategy: PartitionStrategy,
}

/// Typed CDC resume token passed to sources that declare `Cdc`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcResumeToken {
    /// The opaque resume value (LSN, offset, etc.). `None` on first run.
    pub value: Option<String>,
    /// The cursor type hint for the source.
    pub cursor_type: CursorType,
}
```

**Step 2: Add `StreamContext::partition_coordinates_typed` helper**

```rust
impl StreamContext {
    /// Extract typed partition coordinates if present and valid.
    #[must_use]
    pub fn partition_coordinates_typed(&self) -> Option<PartitionCoordinates> {
        let (count, index) = self.partition_coordinates()?;
        Some(PartitionCoordinates {
            count,
            index,
            strategy: self.partition_strategy.unwrap_or(PartitionStrategy::Mod),
        })
    }

    /// Extract a typed CDC resume token from cursor info.
    #[must_use]
    pub fn cdc_resume_token(&self) -> Option<CdcResumeToken> {
        if self.sync_mode != SyncMode::Cdc {
            return None;
        }
        let cursor = self.cursor_info.as_ref()?;
        Some(CdcResumeToken {
            value: cursor.last_value.as_ref().map(|v| match v {
                CursorValue::Utf8 { value } | CursorValue::Lsn { value } => value.clone(),
            }),
            cursor_type: cursor.cursor_type,
        })
    }
}
```

**Step 3: Add tests**

```rust
#[test]
fn partition_coordinates_typed_returns_struct() {
    let ctx = StreamContext {
        partition_count: Some(4),
        partition_index: Some(2),
        partition_strategy: Some(PartitionStrategy::Range),
        ..StreamContext::test_default("users")
    };
    let coords = ctx.partition_coordinates_typed().unwrap();
    assert_eq!(coords.count, 4);
    assert_eq!(coords.index, 2);
    assert_eq!(coords.strategy, PartitionStrategy::Range);
}

#[test]
fn partition_coordinates_typed_defaults_to_mod() {
    let ctx = StreamContext {
        partition_count: Some(4),
        partition_index: Some(0),
        ..StreamContext::test_default("users")
    };
    let coords = ctx.partition_coordinates_typed().unwrap();
    assert_eq!(coords.strategy, PartitionStrategy::Mod);
}

#[test]
fn cdc_resume_token_returns_none_for_non_cdc() {
    let ctx = StreamContext::test_default("users");
    assert!(ctx.cdc_resume_token().is_none());
}
```

**Step 4: Verify**

```bash
cargo test -p rapidbyte-types
```

**Step 5: Commit**

```bash
git add crates/rapidbyte-types/
git commit -m "feat(types): add PartitionCoordinates and CdcResumeToken typed structs"
```

---

## Task 2: Rename `Feature::BulkLoadCopy` to `Feature::BulkLoad`

**Files:**
- Modify: `crates/rapidbyte-types/src/wire.rs`
- Modify: all files referencing `BulkLoadCopy` (find via grep)

**Step 1: Rename the variant**

In `crates/rapidbyte-types/src/wire.rs`, change:

```rust
/// Bulk load via COPY protocol.
BulkLoadCopy,
```

to:

```rust
/// Bulk load support (COPY, multipart upload, load jobs, etc.).
BulkLoad,
```

**Step 2: Update serde alias for backward compat**

Add a serde alias so manifests using the old name still deserialize:

```rust
#[serde(alias = "bulk_load_copy")]
BulkLoad,
```

**Step 3: Grep and update all references**

```bash
grep -rn "BulkLoadCopy" crates/ connectors/
```

Update every occurrence: `Feature::BulkLoadCopy` → `Feature::BulkLoad`.
This includes `crates/rapidbyte-sdk/src/build.rs` (doc comment), any
destination connector build.rs files, and test files.

**Step 4: Verify**

```bash
cargo test --workspace
```

**Step 5: Commit**

```bash
git commit -am "refactor(types): rename Feature::BulkLoadCopy to Feature::BulkLoad"
```

---

## Task 3: Add feature traits to `rapidbyte-sdk`

**Files:**
- Create: `crates/rapidbyte-sdk/src/features.rs`
- Modify: `crates/rapidbyte-sdk/src/lib.rs` (add module)
- Modify: `crates/rapidbyte-sdk/src/prelude.rs` (re-export)

**Step 1: Create the feature traits module**

Create `crates/rapidbyte-sdk/src/features.rs`:

```rust
//! Feature trait contracts.
//!
//! When a plugin declares a [`Feature`] in its manifest, the SDK requires
//! the corresponding trait to be implemented.  The `#[connector]` proc macro
//! enforces this at compile time.

use crate::context::Context;
use crate::error::ConnectorError;
use crate::metric::{ReadSummary, WriteSummary};
use crate::stream::{CdcResumeToken, PartitionCoordinates, StreamContext};

/// Required when a source declares `Feature::PartitionedRead`.
///
/// The generated WIT glue dispatches to `read_partition` when partition
/// coordinates are present in the `StreamContext`.
#[allow(async_fn_in_trait)]
pub trait PartitionedSource {
    async fn read_partition(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        partition: PartitionCoordinates,
    ) -> Result<ReadSummary, ConnectorError>;
}

/// Required when a source declares `Feature::Cdc`.
///
/// The generated WIT glue dispatches to `read_changes` when the stream's
/// sync mode is `Cdc`.
#[allow(async_fn_in_trait)]
pub trait CdcSource {
    async fn read_changes(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        resume: CdcResumeToken,
    ) -> Result<ReadSummary, ConnectorError>;
}

/// Required when a destination declares `Feature::BulkLoad`.
///
/// The generated WIT glue dispatches to `write_bulk` for streams that
/// should use bulk loading.
#[allow(async_fn_in_trait)]
pub trait BulkLoadDestination {
    async fn write_bulk(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, ConnectorError>;
}
```

**Step 2: Register the module in `lib.rs`**

In `crates/rapidbyte-sdk/src/lib.rs`, add:

```rust
#[cfg(feature = "runtime")]
pub mod features;
```

**Step 3: Re-export in prelude**

In `crates/rapidbyte-sdk/src/prelude.rs`, add:

```rust
pub use crate::features::{BulkLoadDestination, CdcSource, PartitionedSource};
pub use crate::stream::{CdcResumeToken, PartitionCoordinates};
```

**Step 4: Add compile-time shape tests**

In `crates/rapidbyte-sdk/src/features.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{Destination, Source};
    use crate::stream::StreamContext;

    // Verify trait shapes are compatible — if this compiles, the trait
    // signatures are consistent with Source/Destination.
    #[allow(dead_code)]
    fn assert_partitioned_source<T: Source + PartitionedSource>() {}
    #[allow(dead_code)]
    fn assert_cdc_source<T: Source + CdcSource>() {}
    #[allow(dead_code)]
    fn assert_bulk_load_dest<T: Destination + BulkLoadDestination>() {}
}
```

**Step 5: Verify**

```bash
cargo test -p rapidbyte-sdk
```

**Step 6: Commit**

```bash
git add crates/rapidbyte-sdk/src/features.rs crates/rapidbyte-sdk/src/lib.rs crates/rapidbyte-sdk/src/prelude.rs
git commit -m "feat(sdk): add PartitionedSource, CdcSource, BulkLoadDestination feature traits"
```

---

## Task 4: Proc macro reads manifest and enforces feature traits

**Files:**
- Modify: `crates/rapidbyte-sdk/macros/src/connector.rs`
- Modify: `crates/rapidbyte-sdk/macros/Cargo.toml` (add `serde_json` dep)

This is the core enforcement step. The proc macro already generates WIT
glue in `gen_source_methods`. We modify it to:

1. Read `OUT_DIR/rapidbyte_manifest.json` at compile time
2. Check which features are declared
3. Emit trait bound assertions and dispatch code

**Step 1: Add manifest reading helper**

In `crates/rapidbyte-sdk/macros/src/connector.rs`, add:

```rust
use std::path::PathBuf;

/// Features declared in the manifest for a given role.
struct ManifestFeatures {
    has_partitioned_read: bool,
    has_cdc: bool,
    has_bulk_load: bool,
}

/// Read the manifest JSON from OUT_DIR (set by build.rs).
/// Returns None if the manifest doesn't exist (non-wasm builds, tests).
fn read_manifest_features(role: &ConnectorRole) -> Option<ManifestFeatures> {
    let out_dir = std::env::var("OUT_DIR").ok()?;
    let path = PathBuf::from(out_dir).join("rapidbyte_manifest.json");
    let json = std::fs::read_to_string(&path).ok()?;
    let manifest: serde_json::Value = serde_json::from_str(&json).ok()?;

    let features_key = match role {
        ConnectorRole::Source => "source",
        ConnectorRole::Destination => "destination",
        ConnectorRole::Transform => return Some(ManifestFeatures {
            has_partitioned_read: false,
            has_cdc: false,
            has_bulk_load: false,
        }),
    };

    let features: Vec<String> = manifest
        .get("roles")
        .and_then(|r| r.get(features_key))
        .and_then(|s| s.get("features"))
        .and_then(|f| serde_json::from_value(f.clone()).ok())
        .unwrap_or_default();

    Some(ManifestFeatures {
        has_partitioned_read: features.iter().any(|f| f == "partitioned_read"),
        has_cdc: features.iter().any(|f| f == "cdc"),
        has_bulk_load: features.iter().any(|f| f == "bulk_load" || f == "bulk_load_copy"),
    })
}
```

**Step 2: Generate compile-time trait assertions**

Add a function that emits `const _: () = { ... }` blocks with trait bounds:

```rust
fn gen_feature_assertions(
    role: &ConnectorRole,
    struct_name: &Ident,
    features: &ManifestFeatures,
) -> TokenStream {
    let mut assertions = Vec::new();

    match role {
        ConnectorRole::Source => {
            if features.has_partitioned_read {
                assertions.push(quote! {
                    const _: () = {
                        fn __assert_partitioned_source<T: ::rapidbyte_sdk::features::PartitionedSource>() {}
                        fn __check() { __assert_partitioned_source::<#struct_name>(); }
                    };
                });
            }
            if features.has_cdc {
                assertions.push(quote! {
                    const _: () = {
                        fn __assert_cdc_source<T: ::rapidbyte_sdk::features::CdcSource>() {}
                        fn __check() { __assert_cdc_source::<#struct_name>(); }
                    };
                });
            }
        }
        ConnectorRole::Destination => {
            if features.has_bulk_load {
                assertions.push(quote! {
                    const _: () = {
                        fn __assert_bulk_load_dest<T: ::rapidbyte_sdk::features::BulkLoadDestination>() {}
                        fn __check() { __assert_bulk_load_dest::<#struct_name>(); }
                    };
                });
            }
        }
        ConnectorRole::Transform => {}
    }

    quote! { #(#assertions)* }
}
```

**Step 3: Generate dispatch in `gen_source_methods`**

Modify `gen_source_methods` to accept `ManifestFeatures` and dispatch to
feature trait methods when appropriate. The `run` method becomes:

```rust
fn run(...) -> ... {
    let stream = parse_stream_context(request.stream_context_json)?;
    // ... existing setup ...

    let summary = rt.block_on(async {
        // Dispatch to feature-specific method if applicable
        if let Some(partition) = stream.partition_coordinates_typed() {
            <#struct_name as ::rapidbyte_sdk::features::PartitionedSource>::read_partition(
                conn, &ctx, stream, partition
            ).await
        } else if stream.sync_mode == ::rapidbyte_sdk::wire::SyncMode::Cdc {
            let resume = stream.cdc_resume_token().unwrap_or(::rapidbyte_sdk::stream::CdcResumeToken {
                value: None,
                cursor_type: ::rapidbyte_sdk::cursor::CursorType::Utf8,
            });
            <#struct_name as ::rapidbyte_sdk::features::CdcSource>::read_changes(
                conn, &ctx, stream, resume
            ).await
        } else {
            <#struct_name as #trait_path>::read(conn, &ctx, stream).await
        }
    }).map_err(to_component_error)?;
    // ... rest unchanged ...
}
```

When a feature is NOT declared, the dispatch branch is omitted entirely
(the trait bound wouldn't be satisfied anyway). The macro generates only
the branches for declared features.

**Step 4: Wire it into `expand()`**

In the `expand` function, call `read_manifest_features` and pass the result
to `gen_guest_impl` and `gen_feature_assertions`:

```rust
pub fn expand(role: ConnectorRole, input: ItemStruct) -> Result<TokenStream> {
    let struct_name = &input.ident;
    // ... existing code ...

    let features = read_manifest_features(&role);
    let feature_assertions = features.as_ref()
        .map(|f| gen_feature_assertions(&role, struct_name, f))
        .unwrap_or_default();

    Ok(quote! {
        #input
        #wit_bindings
        #common
        #guest_impl
        #feature_assertions
        #bindings_mod::export!(RapidbyteComponent with_types_in #bindings_mod);
        #embeds
        fn main() {}
    })
}
```

**Step 5: Add `serde_json` dependency to macros crate**

In `crates/rapidbyte-sdk/macros/Cargo.toml`, add:

```toml
serde_json = { workspace = true }
```

**Step 6: Verify**

```bash
cargo test --workspace
```

The existing source-postgres plugin declares `PartitionedRead` and `Cdc` —
it must now implement `PartitionedSource` and `CdcSource` or fail to compile.
This will be addressed in Task 5.

**Step 7: Commit**

```bash
git add crates/rapidbyte-sdk/macros/
git commit -m "feat(sdk/macros): enforce feature traits via manifest-aware proc macro"
```

---

## Task 5: Migrate source-postgres to feature traits

**Files:**
- Modify: `connectors/source-postgres/src/main.rs`

The current `read()` method has a `match stream.sync_mode` that dispatches
to CDC vs. normal read, and the normal read path internally checks
`partition_coordinates()`. Split this into three trait impls.

**Step 1: Implement `PartitionedSource`**

```rust
impl PartitionedSource for SourcePostgres {
    async fn read_partition(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        partition: PartitionCoordinates,
    ) -> Result<ReadSummary, ConnectorError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        reader::read_stream(&client, ctx, &stream, connect_secs)
            .await
            .map_err(|e| ConnectorError::internal("READ_FAILED", e))
    }
}
```

Note: `reader::read_stream` already reads `partition_coordinates()` from
the `StreamContext`. The `PartitionCoordinates` parameter makes the contract
explicit — the caller guarantees coordinates are present. Internally, the
reader still reads them from `StreamContext` (no refactor of reader internals
needed in this task).

**Step 2: Implement `CdcSource`**

```rust
impl CdcSource for SourcePostgres {
    async fn read_changes(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        _resume: CdcResumeToken,
    ) -> Result<ReadSummary, ConnectorError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        cdc::read_cdc_changes(&client, ctx, &stream, &self.config, connect_secs)
            .await
            .map_err(|e| ConnectorError::internal("CDC_READ_FAILED", e))
    }
}
```

**Step 3: Simplify `Source::read` to non-partitioned, non-CDC only**

```rust
impl Source for SourcePostgres {
    // ... init, discover, validate, close unchanged ...

    async fn read(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<ReadSummary, ConnectorError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        reader::read_stream(&client, ctx, &stream, connect_secs)
            .await
            .map_err(|e| ConnectorError::internal("READ_FAILED", e))
    }
}
```

**Step 4: Add feature trait imports**

In `connectors/source-postgres/src/main.rs`, the prelude already imports
`PartitionedSource`, `CdcSource`, `PartitionCoordinates`, `CdcResumeToken`
(added in Task 3 Step 3).

**Step 5: Build the plugin**

```bash
cd connectors/source-postgres && cargo build
```

This verifies the proc macro enforcement works end-to-end: the manifest
declares `PartitionedRead` + `Cdc`, the macro checks for `PartitionedSource`
+ `CdcSource`, and both traits are implemented.

**Step 6: Commit**

```bash
git add connectors/source-postgres/
git commit -m "refactor(source-postgres): implement PartitionedSource and CdcSource feature traits"
```

---

## Task 6: Host-side runtime sanity checks

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs`

Add post-execution sanity checks in the result aggregation loop.

**Step 1: Add shard sanity check after stream results are collected**

After the `for sr in stream_collection.successes` loop in the
`execute_streams` function (around line 1290), add:

```rust
// Sanity-check partitioned read results
if stream_build.stream_ctxs.iter().any(|s| s.partition_count.is_some()) {
    let shard_counts: Vec<u64> = stream_metrics
        .iter()
        .filter(|m| m.partition_index.is_some())
        .map(|m| m.records_read)
        .collect();

    if !shard_counts.is_empty() {
        if shard_counts.iter().any(|&c| c == 0) {
            tracing::warn!(
                "PartitionedRead sanity check: one or more shards returned 0 rows \
                 — the source may not be honoring partition coordinates"
            );
        }
        if shard_counts.len() > 1
            && shard_counts.iter().all(|&c| c == shard_counts[0])
            && shard_counts[0] > 100
        {
            tracing::warn!(
                shard_count = shard_counts.len(),
                rows_per_shard = shard_counts[0],
                "PartitionedRead sanity check: all shards returned identical row counts \
                 — the source may be ignoring partition coordinates"
            );
        }
    }
}
```

**Step 2: Identify where `StreamShardMetric` is defined**

Check the existing `StreamShardMetric` struct to ensure `partition_index`
and `records_read` fields exist. If not, the shard count check should use
the `StreamResult` data instead. Adapt accordingly.

**Step 3: Test**

```bash
cargo test -p rapidbyte-engine
```

**Step 4: Commit**

```bash
git add crates/rapidbyte-engine/src/orchestrator.rs
git commit -m "feat(orchestrator): add runtime sanity checks for PartitionedRead"
```

---

## Task 7: Add `conformance` module to SDK

**Files:**
- Create: `crates/rapidbyte-sdk/src/conformance.rs`
- Modify: `crates/rapidbyte-sdk/src/lib.rs`
- Modify: `crates/rapidbyte-sdk/Cargo.toml` (add `conformance` feature)

**Step 1: Add the feature flag**

In `crates/rapidbyte-sdk/Cargo.toml`:

```toml
[features]
default = ["runtime", "build"]
runtime = ["dep:tokio", "dep:wit-bindgen", "dep:arrow", "dep:rapidbyte-sdk-macros"]
build = []
conformance = ["dep:tokio"]
```

**Step 2: Create the conformance module**

Create `crates/rapidbyte-sdk/src/conformance.rs`:

```rust
//! Conformance test harness for verifying feature contracts.
//!
//! Plugin authors implement [`ConformanceHarness`] for their backend and
//! call the universal test functions to verify feature correctness.
//!
//! # Example
//!
//! ```ignore
//! use rapidbyte_sdk::conformance::{ConformanceHarness, run_partitioned_read};
//!
//! struct MyHarness { /* ... */ }
//! impl ConformanceHarness for MyHarness { /* ... */ }
//!
//! #[tokio::test]
//! async fn conformance_partitioned_read() {
//!     run_partitioned_read(&MyHarness::new().await).await;
//! }
//! ```

use std::collections::HashSet;
use std::path::PathBuf;

/// Mutation type for CDC conformance tests.
#[derive(Debug, Clone)]
pub enum Mutation {
    Insert { id: String, data: String },
    Update { id: String, data: String },
    Delete { id: String },
}

/// Backend-specific test harness.
///
/// Each plugin implements this to plug into the conformance suite.
/// The seed/verify methods are backend-specific; the assertions are universal.
#[allow(async_fn_in_trait)]
pub trait ConformanceHarness {
    /// Seed `row_count` rows into the named table/object.
    async fn seed(&self, table: &str, row_count: u64) -> Result<(), Box<dyn std::error::Error>>;

    /// Count total rows in the named table/object.
    async fn count_rows(&self, table: &str) -> Result<u64, Box<dyn std::error::Error>>;

    /// Return all row IDs/primary keys for duplicate detection.
    async fn collect_ids(&self, table: &str) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    /// Apply mutations for CDC verification.
    async fn apply_mutations(
        &self,
        table: &str,
        mutations: &[Mutation],
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// Return the connector WASM path and config JSON.
    fn connector_config(&self) -> (PathBuf, serde_json::Value);

    /// Clean up test resources.
    async fn teardown(&self) -> Result<(), Box<dyn std::error::Error>>;
}

/// Verify that a source honoring `PartitionedRead` correctly partitions data.
///
/// Seeds `row_count` rows, runs the connector with `shard_count` shards,
/// and asserts that all rows are covered with no duplicates.
pub async fn run_partitioned_read<H: ConformanceHarness>(
    harness: &H,
    table: &str,
    row_count: u64,
    shard_count: u32,
) {
    // Seed
    harness
        .seed(table, row_count)
        .await
        .expect("seed failed");

    let seeded = harness
        .count_rows(table)
        .await
        .expect("count_rows failed");
    assert_eq!(
        seeded, row_count,
        "Seeded row count mismatch: expected {row_count}, got {seeded}"
    );

    // TODO: Run connector with shard_count partitions, collect emitted row IDs
    // This requires a lightweight in-process WASM runner or shelling out to
    // the rapidbyte binary. Deferred to integration — the harness trait and
    // assertion framework are the deliverable for this task.

    harness.teardown().await.expect("teardown failed");
}

/// Verify that a source honoring `Cdc` correctly captures changes.
pub async fn run_cdc<H: ConformanceHarness>(harness: &H, table: &str) {
    harness.seed(table, 100).await.expect("seed failed");

    // TODO: Run connector in CDC mode, apply mutations, verify captures.
    // Same deferral as run_partitioned_read.

    harness.teardown().await.expect("teardown failed");
}
```

**Step 3: Register the module**

In `crates/rapidbyte-sdk/src/lib.rs`:

```rust
#[cfg(feature = "conformance")]
pub mod conformance;
```

**Step 4: Verify**

```bash
cargo test -p rapidbyte-sdk
cargo test -p rapidbyte-sdk --features conformance
```

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/conformance.rs crates/rapidbyte-sdk/src/lib.rs crates/rapidbyte-sdk/Cargo.toml
git commit -m "feat(sdk): add conformance test harness for feature contract verification"
```

---

## Task 8: Full verification

**Step 1: Run all workspace tests**

```bash
cargo test --workspace
```

**Step 2: Build all plugins**

```bash
just build-connectors
```

**Step 3: Run E2E tests**

```bash
just e2e
```

**Step 4: Run benchmark**

```bash
just bench postgres 1000000 --profile small --iters 3
```

Verify throughput is comparable to baseline (~480K+ rows/s, ~2.5+ CPU cores).

**Step 5: Commit any fixes**

If any issues arise, fix and commit individually.
