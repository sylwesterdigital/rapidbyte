//! `PostgreSQL`-backed implementation of [`StateBackend`].
//!
//! Uses the sync `postgres` crate with a single `Mutex<Client>` for
//! thread safety. The `postgres` crate manages its own internal tokio
//! runtime, so this works from any thread.

use std::sync::{Mutex, MutexGuard};

use chrono::Utc;
use postgres::{Client, NoTls};
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};

use crate::backend::StateBackend;
use crate::error::{self, StateError};

/// Idempotent DDL for state tables (`PostgreSQL` dialect).
const CREATE_TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS sync_cursors (
    pipeline TEXT NOT NULL,
    stream TEXT NOT NULL,
    cursor_field TEXT,
    cursor_value TEXT,
    updated_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')),
    PRIMARY KEY (pipeline, stream)
);

CREATE TABLE IF NOT EXISTS sync_runs (
    id BIGSERIAL PRIMARY KEY,
    pipeline TEXT NOT NULL,
    stream TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')),
    finished_at TEXT,
    records_read BIGINT DEFAULT 0,
    records_written BIGINT DEFAULT 0,
    bytes_read BIGINT DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS dlq_records (
    id BIGSERIAL PRIMARY KEY,
    pipeline TEXT NOT NULL,
    run_id BIGINT NOT NULL REFERENCES sync_runs(id),
    stream_name TEXT NOT NULL,
    record_json TEXT NOT NULL,
    error_message TEXT NOT NULL,
    error_category TEXT NOT NULL,
    failed_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))
);

CREATE INDEX IF NOT EXISTS idx_dlq_pipeline_run ON dlq_records (pipeline, run_id);
"#;

/// `PostgreSQL`-backed state storage.
///
/// Create with [`PostgresStateBackend::open`] providing a libpq-style
/// connection string (e.g. `"host=localhost dbname=rapidbyte user=postgres"`).
pub struct PostgresStateBackend {
    client: Mutex<Client>,
}

impl PostgresStateBackend {
    /// Connect to a `PostgreSQL` database and initialize state tables.
    ///
    /// `connstr` is a libpq-style connection string or `PostgreSQL` URI:
    /// - `"host=localhost port=5432 dbname=rapidbyte user=postgres"`
    /// - `"postgresql://postgres@localhost/rapidbyte"`
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Backend`] if connection or DDL execution fails.
    pub fn open(connstr: &str) -> error::Result<Self> {
        let mut client = Client::connect(connstr, NoTls).map_err(StateError::backend)?;
        client
            .batch_execute(CREATE_TABLES)
            .map_err(StateError::backend)?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    /// Acquire the client lock.
    fn lock_client(&self) -> error::Result<MutexGuard<'_, Client>> {
        self.client.lock().map_err(|_| StateError::LockPoisoned)
    }

    /// Current UTC time as ISO-8601 string.
    fn now_iso() -> String {
        Utc::now().to_rfc3339()
    }
}

impl StateBackend for PostgresStateBackend {
    fn get_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> error::Result<Option<CursorState>> {
        let mut client = self.lock_client()?;
        let rows = client
            .query(
                "SELECT cursor_field, cursor_value, updated_at \
                 FROM sync_cursors WHERE pipeline = $1 AND stream = $2",
                &[&pipeline.as_str(), &stream.as_str()],
            )
            .map_err(StateError::backend)?;

        match rows.first() {
            Some(row) => {
                let cursor_field: Option<String> = row.get(0);
                let cursor_value: Option<String> = row.get(1);
                let updated_at: String = row.get(2);
                Ok(Some(CursorState {
                    cursor_field,
                    cursor_value,
                    updated_at,
                }))
            }
            None => Ok(None),
        }
    }

    fn set_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> error::Result<()> {
        let mut client = self.lock_client()?;
        client
            .execute(
                "INSERT INTO sync_cursors (pipeline, stream, cursor_field, cursor_value, updated_at) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (pipeline, stream) \
                 DO UPDATE SET cursor_field = $3, cursor_value = $4, updated_at = $5",
                &[
                    &pipeline.as_str(),
                    &stream.as_str(),
                    &cursor.cursor_field,
                    &cursor.cursor_value,
                    &cursor.updated_at,
                ],
            )
            .map_err(StateError::backend)?;
        Ok(())
    }

    fn start_run(&self, pipeline: &PipelineId, stream: &StreamName) -> error::Result<i64> {
        let mut client = self.lock_client()?;
        let row = client
            .query_one(
                "INSERT INTO sync_runs (pipeline, stream, status) \
                 VALUES ($1, $2, $3) RETURNING id",
                &[
                    &pipeline.as_str(),
                    &stream.as_str(),
                    &RunStatus::Running.as_str(),
                ],
            )
            .map_err(StateError::backend)?;
        Ok(row.get(0))
    }

    #[allow(clippy::cast_possible_wrap, clippy::similar_names)]
    fn complete_run(&self, run_id: i64, status: RunStatus, stats: &RunStats) -> error::Result<()> {
        let mut client = self.lock_client()?;
        client
            .execute(
                "UPDATE sync_runs SET status = $1, finished_at = to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), \
                 records_read = $2, records_written = $3, bytes_read = $4, error_message = $5 \
                 WHERE id = $6",
                &[
                    &status.as_str(),
                    &(stats.records_read as i64),
                    &(stats.records_written as i64),
                    &(stats.bytes_read as i64),
                    &stats.error_message,
                    &run_id,
                ],
            )
            .map_err(StateError::backend)?;
        Ok(())
    }

    fn compare_and_set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        expected: Option<&str>,
        new_value: &str,
    ) -> error::Result<bool> {
        let mut client = self.lock_client()?;
        let now = Self::now_iso();

        let rows_affected = match expected {
            Some(expected_val) => client
                .execute(
                    "UPDATE sync_cursors SET cursor_value = $1, updated_at = $2 \
                     WHERE pipeline = $3 AND stream = $4 AND cursor_value = $5",
                    &[
                        &new_value,
                        &now,
                        &pipeline.as_str(),
                        &stream.as_str(),
                        &expected_val,
                    ],
                )
                .map_err(StateError::backend)?,
            None => client
                .execute(
                    "INSERT INTO sync_cursors (pipeline, stream, cursor_value, updated_at) \
                     VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                    &[&pipeline.as_str(), &stream.as_str(), &new_value, &now],
                )
                .map_err(StateError::backend)?,
        };

        Ok(rows_affected > 0)
    }

    fn insert_dlq_records(
        &self,
        pipeline: &PipelineId,
        run_id: i64,
        records: &[DlqRecord],
    ) -> error::Result<u64> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut client = self.lock_client()?;
        let mut tx = client
            .transaction()
            .map_err(|e| StateError::backend_context("insert_dlq_records: begin tx", e))?;
        let stmt = tx
            .prepare(
                "INSERT INTO dlq_records \
                 (pipeline, run_id, stream_name, record_json, error_message, error_category, failed_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .map_err(|e| StateError::backend_context("insert_dlq_records: prepare", e))?;

        for record in records {
            tx.execute(
                &stmt,
                &[
                    &pipeline.as_str(),
                    &run_id,
                    &record.stream_name.as_str(),
                    &record.record_json.as_str(),
                    &record.error_message.as_str(),
                    &record.error_category.as_str(),
                    &record.failed_at.as_str(),
                ],
            )
            .map_err(|e| StateError::backend_context("insert_dlq_records: execute", e))?;
        }
        tx.commit()
            .map_err(|e| StateError::backend_context("insert_dlq_records: commit", e))?;

        Ok(records.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::envelope::Timestamp;
    use rapidbyte_types::error::ErrorCategory;

    /// Helper: get Postgres connection string from env or skip test.
    fn test_connstr() -> String {
        std::env::var("TEST_POSTGRES_URL")
            .expect("TEST_POSTGRES_URL not set — skipping Postgres integration test")
    }

    /// Helper: clean up test tables before each test.
    fn clean_tables(client: &mut Client) {
        client
            .batch_execute(
                "DELETE FROM dlq_records; DELETE FROM sync_runs; DELETE FROM sync_cursors;",
            )
            .unwrap();
    }

    #[test]
    #[ignore = "requires TEST_POSTGRES_URL"]
    fn cursor_roundtrip() {
        let backend = PostgresStateBackend::open(&test_connstr()).unwrap();
        clean_tables(&mut backend.lock_client().unwrap());

        let pid = PipelineId::new("pg_test");
        let sn = StreamName::new("pg_stream");

        assert!(backend.get_cursor(&pid, &sn).unwrap().is_none());

        backend
            .set_cursor(
                &pid,
                &sn,
                &CursorState {
                    cursor_field: Some("id".into()),
                    cursor_value: Some("42".into()),
                    updated_at: PostgresStateBackend::now_iso(),
                },
            )
            .unwrap();

        let cursor = backend.get_cursor(&pid, &sn).unwrap().unwrap();
        assert_eq!(cursor.cursor_field, Some("id".into()));
        assert_eq!(cursor.cursor_value, Some("42".into()));
    }

    #[test]
    #[ignore = "requires TEST_POSTGRES_URL"]
    fn run_lifecycle() {
        let backend = PostgresStateBackend::open(&test_connstr()).unwrap();
        clean_tables(&mut backend.lock_client().unwrap());

        let pid = PipelineId::new("pg_test");
        let sn = StreamName::new("pg_stream");
        let run_id = backend.start_run(&pid, &sn).unwrap();
        assert!(run_id > 0);

        backend
            .complete_run(
                run_id,
                RunStatus::Completed,
                &RunStats {
                    records_read: 500,
                    records_written: 500,
                    bytes_read: 25000,
                    error_message: None,
                },
            )
            .unwrap();
    }

    #[test]
    #[ignore = "requires TEST_POSTGRES_URL"]
    fn compare_and_set_postgres() {
        let backend = PostgresStateBackend::open(&test_connstr()).unwrap();
        clean_tables(&mut backend.lock_client().unwrap());

        let pid = PipelineId::new("pg_cas");
        let sn = StreamName::new("pg_stream");

        // Insert via CAS from None
        assert!(backend.compare_and_set(&pid, &sn, None, "10").unwrap());

        // CAS with correct expected
        assert!(backend
            .compare_and_set(&pid, &sn, Some("10"), "20")
            .unwrap());

        // CAS with wrong expected
        assert!(!backend
            .compare_and_set(&pid, &sn, Some("10"), "30")
            .unwrap());

        let cursor = backend.get_cursor(&pid, &sn).unwrap().unwrap();
        assert_eq!(cursor.cursor_value, Some("20".into()));
    }

    #[test]
    #[ignore = "requires TEST_POSTGRES_URL"]
    fn dlq_records_insert() {
        let backend = PostgresStateBackend::open(&test_connstr()).unwrap();
        clean_tables(&mut backend.lock_client().unwrap());

        let pid = PipelineId::new("pg_dlq");
        let sn = StreamName::new("pg_stream");
        let run_id = backend.start_run(&pid, &sn).unwrap();

        let records = vec![DlqRecord {
            stream_name: "users".into(),
            record_json: r#"{"id":1}"#.into(),
            error_message: "constraint violation".into(),
            error_category: ErrorCategory::Data,
            failed_at: Timestamp::new("2026-02-21T12:00:00+00:00"),
        }];

        let count = backend.insert_dlq_records(&pid, run_id, &records).unwrap();
        assert_eq!(count, 1);
    }
}
