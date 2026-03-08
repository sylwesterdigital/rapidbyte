# Feature Contract Enforcement

## Problem

When a plugin declares a `Feature` (e.g., `PartitionedRead`, `Cdc`), nothing
enforces that it actually implements the contract. A source could declare
`PartitionedRead` and read the full table on every shard, duplicating data
N times. The same gap exists for every feature.

## Design

Three tiers of enforcement, each catching a different class of bug:

1. **SDK trait contracts (compile-time)** — prevents "declared but forgot to handle"
2. **Conformance test harness (test-time)** — proves correctness
3. **Host-side runtime sanity checks (production)** — catches regressions

## Tier 1: SDK Feature Traits

### Principle

Features describe abstract capabilities, not implementation details. They are
role-agnostic in the enum but enforce role-specific trait contracts.

### Feature Taxonomy

| Feature            | Meaning                                              |
|--------------------|------------------------------------------------------|
| `PartitionedRead`  | Source can read a subset based on shard coordinates   |
| `Cdc`              | Source can track and emit changes since a checkpoint  |
| `BulkLoad`         | Destination can ingest data in bulk                   |
| `SchemaAutoMigrate`| Destination handles schema drift                     |
| `ExactlyOnce`      | Guarantees no duplicates or gaps                      |
| `Stateful`         | Uses host state backend for cursor persistence        |

Note: `BulkLoadCopy` is renamed to `BulkLoad`. The COPY protocol is a
Postgres implementation detail; S3 would use multipart upload, BigQuery
would use load jobs. The feature describes *what*, not *how*.

### Trait Hierarchy

Features that change the API shape get their own trait:

```rust
/// Required when manifest declares Feature::PartitionedRead on a source.
pub trait PartitionedSource: Source {
    async fn read_partition(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        partition: PartitionCoordinates,
    ) -> Result<ReadSummary, ConnectorError>;
}

/// Required when manifest declares Feature::Cdc on a source.
pub trait CdcSource: Source {
    async fn read_changes(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        resume: CdcResumeToken,
    ) -> Result<ReadSummary, ConnectorError>;
}

/// Required when manifest declares Feature::BulkLoad on a destination.
pub trait BulkLoadDestination: Destination {
    async fn write_bulk(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, ConnectorError>;
}
```

Typed context structs replace optional fields:

```rust
pub struct PartitionCoordinates {
    pub count: u32,
    pub index: u32,
    pub strategy: PartitionStrategy,
}

pub struct CdcResumeToken {
    pub value: Option<String>,
    pub cursor_type: CursorType,
}
```

### Compile-Time Enforcement

The `#[connector(source)]` proc macro already has access to the manifest
(via `OUT_DIR/rapidbyte_manifest.json`). It reads the declared features and:

- If `PartitionedRead` is declared, generates WIT glue that dispatches to
  `read_partition()` when partition coordinates are present in the
  `StreamContext`, and emits a compile error if the type does not implement
  `PartitionedSource`.
- If `Cdc` is declared, generates dispatch to `read_changes()` when
  `sync_mode == Cdc`, compile error without `CdcSource`.
- Same pattern for destination features.

The WIT interface stays unchanged (single `run` export, JSON-serialized
`StreamContext`). The dispatch happens inside the SDK-generated glue code.

### Role-Agnostic Features

Features are not hardcoded to source or destination. The same `Feature` enum
is used in both `SourceCapabilities.features` and
`DestinationCapabilities.features`. The trait enforcement is role-specific:
declaring `PartitionedRead` on a source requires `PartitionedSource`;
declaring it on a destination (future: parallel writes) would require a
`PartitionedDestination` trait.

## Tier 2: Conformance Test Harness

### Location

Lives in `rapidbyte-sdk` behind a `conformance` Cargo feature flag. Plugins
pull it in as a dev-dependency:

```toml
[dev-dependencies]
rapidbyte-sdk = { path = "...", features = ["conformance"] }
```

### Harness Trait

Each plugin provides a backend-specific test harness:

```rust
pub trait ConformanceHarness {
    /// Seed N rows into the test table/object.
    async fn seed(&self, table: &str, row_count: u64) -> Result<()>;

    /// Count total rows.
    async fn count_rows(&self, table: &str) -> Result<u64>;

    /// Return all row IDs for duplicate detection.
    async fn collect_ids(&self, table: &str) -> Result<Vec<String>>;

    /// Apply mutations for CDC verification.
    async fn apply_mutations(&self, table: &str, mutations: &[Mutation]) -> Result<()>;

    /// Connector WASM path and config JSON.
    fn connector_config(&self) -> (PathBuf, serde_json::Value);

    /// Clean up test resources.
    async fn teardown(&self) -> Result<()>;
}
```

### Universal Assertions

The conformance suite provides test functions parameterized by harness:

**`conformance::partitioned_read(harness)`**
1. Seed 10,000 rows
2. Run connector with 4 shards (partition 0..3)
3. Collect all emitted row IDs across shards
4. Assert: total IDs == 10,000
5. Assert: no duplicates (HashSet size == Vec size)
6. Assert: each shard produced > 0 rows

**`conformance::cdc(harness)`**
1. Seed 100 rows, run CDC, capture checkpoint
2. Apply mutations (inserts, updates, deletes)
3. Run CDC from checkpoint
4. Assert: all mutations captured with correct operation types
5. Assert: checkpoint advanced

**`conformance::bulk_load(harness)`**
1. Seed 10,000 rows in source
2. Run full pipeline (source -> destination)
3. Assert: destination row count == 10,000, no duplicates

**`conformance::exactly_once(harness)`**
1. Seed and run with simulated mid-pipeline restart
2. Resume from checkpoint
3. Assert: exactly N rows in destination, no gaps, no duplicates

### Plugin Usage

```rust
use rapidbyte_sdk::conformance::{ConformanceHarness, run_partitioned_read, run_cdc};

struct PostgresHarness { /* connection pool */ }
impl ConformanceHarness for PostgresHarness { /* ... */ }

#[tokio::test]
async fn conformance_partitioned_read() {
    run_partitioned_read(&PostgresHarness::new().await).await;
}
```

A Postgres plugin, S3 plugin, and MySQL plugin all implement their own
harness — different seed/verify logic, same assertions.

## Tier 3: Host-Side Runtime Sanity Checks

Lightweight `tracing::warn!` checks in the orchestrator's post-execution
path. Zero performance cost.

| Feature           | Check                                                    |
|-------------------|----------------------------------------------------------|
| `PartitionedRead` | Warn if any shard returned 0 rows                        |
| `PartitionedRead` | Warn if all shards returned identical row counts         |
| `Cdc`             | Warn if checkpoint didn't advance                        |
| `ExactlyOnce`     | Warn if checkpoint gaps detected after resumed run       |
| `BulkLoad`        | No runtime check needed (conformance tests cover it)     |

## Migration

- Rename `Feature::BulkLoadCopy` to `Feature::BulkLoad` (breaking change,
  coordinate with manifest version bump).
- Existing plugins continue to work without feature traits initially; the
  proc macro enforcement can be gated behind a manifest protocol version
  (e.g., V5+) so older plugins aren't broken.
- Add feature traits incrementally: `PartitionedRead` first (already have
  a working plugin), then `Cdc`, then destination features.
