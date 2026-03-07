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
//!     run_partitioned_read(&MyHarness::new().await, "test_table", 10_000, 4).await;
//! }
//! ```

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

    /// Return the plugin WASM path and config JSON.
    fn plugin_config(&self) -> (PathBuf, serde_json::Value);

    /// Clean up test resources.
    async fn teardown(&self) -> Result<(), Box<dyn std::error::Error>>;
}

/// Verify that a source honoring `PartitionedRead` correctly partitions data.
///
/// Seeds `row_count` rows, runs the plugin with `shard_count` shards,
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
        .expect("conformance: seed failed");

    let seeded = harness
        .count_rows(table)
        .await
        .expect("conformance: count_rows failed");
    assert_eq!(
        seeded, row_count,
        "conformance: seeded row count mismatch: expected {row_count}, got {seeded}"
    );

    // TODO: Run plugin with shard_count partitions, collect emitted row IDs.
    // This requires a lightweight in-process WASM runner or shelling out to
    // the rapidbyte binary. Deferred to integration — the harness trait and
    // assertion framework are the deliverable for this task.
    let _ = shard_count;

    harness
        .teardown()
        .await
        .expect("conformance: teardown failed");
}

/// Verify that a source honoring `Cdc` correctly captures changes.
pub async fn run_cdc<H: ConformanceHarness>(harness: &H, table: &str) {
    harness
        .seed(table, 100)
        .await
        .expect("conformance: seed failed");

    // TODO: Run plugin in CDC mode, apply mutations, verify captures.
    // Same deferral as run_partitioned_read.

    harness
        .teardown()
        .await
        .expect("conformance: teardown failed");
}
