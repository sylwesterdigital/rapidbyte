//! In-memory Arrow workspace backed by `DataFusion` for SQL queries.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;

/// Summary information for a single table in the workspace.
pub struct TableSummary {
    pub name: String,
    pub rows: u64,
    pub columns: usize,
    pub memory_bytes: u64,
}

/// In-memory workspace that holds streamed `RecordBatch`es and registers them
/// as `DataFusion` `MemTable`s for interactive SQL queries.
pub struct ArrowWorkspace {
    tables: HashMap<String, (SchemaRef, Vec<RecordBatch>)>,
    session: SessionContext,
}

impl ArrowWorkspace {
    /// Create an empty workspace.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            session: SessionContext::new(),
        }
    }

    /// Insert or replace a table in the workspace.
    ///
    /// Deregisters any previous table with the same name, creates a new
    /// `MemTable`, registers it with the `DataFusion` session, and stores
    /// the schema and batches in the internal map.
    ///
    /// # Errors
    ///
    /// Returns an error if the `MemTable` cannot be created or registered.
    pub fn insert(
        &mut self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        // Deregister previous table if it exists.
        if self.tables.contains_key(name) {
            self.session
                .deregister_table(name)
                .context("failed to deregister previous table")?;
        }

        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![batches.clone()])
                .context("failed to create MemTable")?;

        self.session
            .register_table(name, Arc::new(mem_table))
            .context("failed to register table")?;

        self.tables.insert(name.to_string(), (schema, batches));
        Ok(())
    }

    /// Execute a SQL query against the workspace and collect all result batches.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails to parse, plan, or execute.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self
            .session
            .sql(sql)
            .await
            .context("failed to execute SQL query")?;
        let batches = df
            .collect()
            .await
            .context("failed to collect query results")?;
        Ok(batches)
    }

    /// Return a summary of every table in the workspace, sorted by name.
    #[must_use]
    pub fn summary(&self) -> Vec<TableSummary> {
        let mut summaries: Vec<TableSummary> = self
            .tables
            .iter()
            .map(|(name, (schema, batches))| {
                let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                let columns = schema.fields().len();
                let memory_bytes: u64 = batches
                    .iter()
                    .map(|b| {
                        b.columns()
                            .iter()
                            .map(|col| col.get_array_memory_size() as u64)
                            .sum::<u64>()
                    })
                    .sum();
                TableSummary {
                    name: name.clone(),
                    rows,
                    columns,
                    memory_bytes,
                }
            })
            .collect();
        summaries.sort_by(|a, b| a.name.cmp(&b.name));
        summaries
    }

    /// Drop one table (if `table` is `Some`) or all tables from the workspace.
    pub fn clear(&mut self, table: Option<&str>) {
        if let Some(name) = table {
            if self.tables.remove(name).is_some() {
                let _ = self.session.deregister_table(name);
            }
        } else {
            for name in self.tables.keys().cloned().collect::<Vec<_>>() {
                let _ = self.session.deregister_table(&name);
            }
            self.tables.clear();
        }
    }

    /// Check whether a table with the given name exists in the workspace.
    #[must_use]
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Check whether the workspace contains no tables.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

impl Default for ArrowWorkspace {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn test_batches(schema: &SchemaRef) -> Vec<RecordBatch> {
        vec![RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap()]
    }

    #[tokio::test]
    async fn test_insert_and_query() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();
        let batches = test_batches(&schema);

        ws.insert("users", schema, batches).unwrap();

        let results = ws.query("SELECT * FROM users").await.unwrap();
        let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_insert_replaces_existing() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();
        let batches = test_batches(&schema);

        ws.insert("users", Arc::clone(&schema), batches).unwrap();

        // Replace with different data (only 2 rows).
        let new_batches = vec![RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["xander", "yara"])),
            ],
        )
        .unwrap()];

        ws.insert("users", schema, new_batches).unwrap();

        let results = ws.query("SELECT * FROM users").await.unwrap();
        let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_summary() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();
        let batches = test_batches(&schema);

        ws.insert("users", schema, batches).unwrap();

        let summaries = ws.summary();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].name, "users");
        assert_eq!(summaries[0].rows, 3);
        assert_eq!(summaries[0].columns, 2);
        assert!(summaries[0].memory_bytes > 0);
    }

    #[test]
    fn test_clear_single() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();

        ws.insert("users", Arc::clone(&schema), test_batches(&schema))
            .unwrap();
        ws.insert("orders", Arc::clone(&schema), test_batches(&schema))
            .unwrap();

        ws.clear(Some("users"));

        assert!(!ws.has_table("users"));
        assert!(ws.has_table("orders"));
    }

    #[test]
    fn test_clear_all() {
        let mut ws = ArrowWorkspace::new();
        let schema = test_schema();

        ws.insert("users", Arc::clone(&schema), test_batches(&schema))
            .unwrap();
        ws.insert("orders", Arc::clone(&schema), test_batches(&schema))
            .unwrap();

        ws.clear(None);

        assert!(ws.is_empty());
    }

    #[tokio::test]
    async fn test_sql_error_returns_err() {
        let ws = ArrowWorkspace::new();
        let result = ws.query("SELECT * FROM nonexistent_table").await;
        assert!(result.is_err());
    }
}
