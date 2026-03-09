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
use std::{collections::BTreeSet, error::Error};

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
    async fn seed(&self, table: &str, row_count: u64) -> Result<(), Box<dyn Error>>;

    /// Count total rows in the named table/object.
    async fn count_rows(&self, table: &str) -> Result<u64, Box<dyn Error>>;

    /// Return all row IDs/primary keys for duplicate detection.
    async fn collect_ids(&self, table: &str) -> Result<Vec<String>, Box<dyn Error>>;

    /// Run the plugin in partitioned-read mode and return observed row IDs.
    async fn run_partitioned_read_ids(
        &self,
        table: &str,
        shard_count: u32,
    ) -> Result<Vec<String>, Box<dyn Error>>;

    /// Apply mutations for CDC verification.
    async fn apply_mutations(
        &self,
        table: &str,
        mutations: &[Mutation],
    ) -> Result<(), Box<dyn Error>>;

    /// Run the plugin in CDC mode and return observed mutation IDs.
    async fn run_cdc_capture_ids(&self, table: &str) -> Result<Vec<String>, Box<dyn Error>>;

    /// Return the plugin WASM path and config JSON.
    fn plugin_config(&self) -> (PathBuf, serde_json::Value);

    /// Clean up test resources.
    async fn teardown(&self) -> Result<(), Box<dyn Error>>;
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

    let expected_ids = harness
        .collect_ids(table)
        .await
        .expect("conformance: collect_ids failed");
    let expected_id_count = u64::try_from(expected_ids.len()).expect("usize should fit into u64");
    assert_eq!(
        expected_id_count, row_count,
        "conformance: expected ID count mismatch: expected {row_count}, got {expected_id_count}"
    );
    let expected_set: BTreeSet<_> = expected_ids.into_iter().collect();
    assert_eq!(
        u64::try_from(expected_set.len()).expect("usize should fit into u64"),
        row_count,
        "conformance: expected IDs must be unique"
    );

    let observed_ids = harness
        .run_partitioned_read_ids(table, shard_count)
        .await
        .expect("conformance: run_partitioned_read_ids failed");
    let observed_id_count = u64::try_from(observed_ids.len()).expect("usize should fit into u64");
    assert_eq!(
        observed_id_count, row_count,
        "conformance: observed row count mismatch: expected {row_count}, got {observed_id_count}"
    );
    let observed_set: BTreeSet<_> = observed_ids.into_iter().collect();
    assert_eq!(
        u64::try_from(observed_set.len()).expect("usize should fit into u64"),
        row_count,
        "conformance: observed IDs must be unique"
    );
    assert_eq!(
        observed_set, expected_set,
        "conformance: observed partitioned-read IDs did not match seeded IDs"
    );

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

    let mutations = vec![
        Mutation::Insert {
            id: "insert-101".to_string(),
            data: "inserted".to_string(),
        },
        Mutation::Update {
            id: "42".to_string(),
            data: "updated".to_string(),
        },
        Mutation::Delete {
            id: "7".to_string(),
        },
    ];

    harness
        .apply_mutations(table, &mutations)
        .await
        .expect("conformance: apply_mutations failed");

    let expected_ids: BTreeSet<_> = mutations.iter().map(mutation_id).collect();
    let observed_ids = harness
        .run_cdc_capture_ids(table)
        .await
        .expect("conformance: run_cdc_capture_ids failed");
    let observed_set: BTreeSet<_> = observed_ids.into_iter().collect();
    assert_eq!(
        observed_set.len(),
        expected_ids.len(),
        "conformance: observed CDC IDs must be unique and complete"
    );
    assert_eq!(
        observed_set, expected_ids,
        "conformance: observed CDC IDs did not match applied mutations"
    );

    harness
        .teardown()
        .await
        .expect("conformance: teardown failed");
}

fn mutation_id(mutation: &Mutation) -> String {
    match mutation {
        Mutation::Insert { id, .. } | Mutation::Update { id, .. } | Mutation::Delete { id } => {
            id.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic;

    use super::*;

    struct FakeHarness {
        source_ids: Vec<String>,
        partitioned_ids: Vec<String>,
        cdc_ids: Vec<String>,
        seeded_rows: u64,
    }

    impl FakeHarness {
        fn new(
            source_ids: Vec<&str>,
            partitioned_ids: Vec<&str>,
            cdc_ids: Vec<&str>,
            seeded_rows: u64,
        ) -> Self {
            Self {
                source_ids: source_ids.into_iter().map(str::to_string).collect(),
                partitioned_ids: partitioned_ids.into_iter().map(str::to_string).collect(),
                cdc_ids: cdc_ids.into_iter().map(str::to_string).collect(),
                seeded_rows,
            }
        }
    }

    #[allow(async_fn_in_trait)]
    impl ConformanceHarness for FakeHarness {
        async fn seed(&self, _table: &str, _row_count: u64) -> Result<(), Box<dyn Error>> {
            Ok(())
        }

        async fn count_rows(&self, _table: &str) -> Result<u64, Box<dyn Error>> {
            Ok(self.seeded_rows)
        }

        async fn collect_ids(&self, _table: &str) -> Result<Vec<String>, Box<dyn Error>> {
            Ok(self.source_ids.clone())
        }

        async fn run_partitioned_read_ids(
            &self,
            _table: &str,
            _shard_count: u32,
        ) -> Result<Vec<String>, Box<dyn Error>> {
            Ok(self.partitioned_ids.clone())
        }

        async fn apply_mutations(
            &self,
            _table: &str,
            _mutations: &[Mutation],
        ) -> Result<(), Box<dyn Error>> {
            Ok(())
        }

        async fn run_cdc_capture_ids(&self, _table: &str) -> Result<Vec<String>, Box<dyn Error>> {
            Ok(self.cdc_ids.clone())
        }

        fn plugin_config(&self) -> (PathBuf, serde_json::Value) {
            (PathBuf::new(), serde_json::Value::Null)
        }

        async fn teardown(&self) -> Result<(), Box<dyn Error>> {
            Ok(())
        }
    }

    #[test]
    fn partitioned_read_accepts_exact_coverage() {
        let harness = FakeHarness::new(vec!["1", "2", "3"], vec!["1", "2", "3"], vec![], 3);
        futures::executor::block_on(run_partitioned_read(&harness, "users", 3, 2));
    }

    #[test]
    fn partitioned_read_rejects_duplicate_ids() {
        let harness = FakeHarness::new(vec!["1", "2", "3"], vec!["1", "2", "2"], vec![], 3);
        let result = panic::catch_unwind(|| {
            futures::executor::block_on(run_partitioned_read(&harness, "users", 3, 2));
        });
        assert!(result.is_err());
    }

    #[test]
    fn partitioned_read_requires_full_coverage() {
        let harness = FakeHarness::new(vec!["1", "2", "3"], vec!["1", "2"], vec![], 3);
        let result = panic::catch_unwind(|| {
            futures::executor::block_on(run_partitioned_read(&harness, "users", 3, 2));
        });
        assert!(result.is_err());
    }

    #[test]
    fn cdc_accepts_expected_mutation_ids() {
        let harness = FakeHarness::new(vec![], vec![], vec!["insert-101", "42", "7"], 100);
        futures::executor::block_on(run_cdc(&harness, "users"));
    }

    #[test]
    fn cdc_requires_all_mutations_to_be_observed() {
        let harness = FakeHarness::new(vec![], vec![], vec!["insert-101", "42"], 100);
        let result = panic::catch_unwind(|| {
            futures::executor::block_on(run_cdc(&harness, "users"));
        });
        assert!(result.is_err());
    }
}
