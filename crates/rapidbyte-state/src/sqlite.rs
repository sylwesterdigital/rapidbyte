//! `SQLite`-backed implementation of [`StateBackend`].
//!
//! Uses a single `Mutex<Connection>` for thread safety.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use chrono::{NaiveDateTime, Utc};
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
use rusqlite::Connection;

use crate::backend::StateBackend;
use crate::error::{self, StateError};

/// `SQLite` datetime format (UTC, no timezone suffix).
const SQLITE_DATETIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

/// Idempotent DDL for state tables.
const CREATE_TABLES: &str = r"
CREATE TABLE IF NOT EXISTS sync_cursors (
    pipeline TEXT NOT NULL,
    stream TEXT NOT NULL,
    cursor_field TEXT,
    cursor_value TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (pipeline, stream)
);

CREATE TABLE IF NOT EXISTS sync_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    stream TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    records_read INTEGER DEFAULT 0,
    records_written INTEGER DEFAULT 0,
    bytes_read INTEGER DEFAULT 0,
    bytes_written INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS dlq_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    run_id INTEGER NOT NULL REFERENCES sync_runs(id),
    stream_name TEXT NOT NULL,
    record_json TEXT NOT NULL,
    error_message TEXT NOT NULL,
    error_category TEXT NOT NULL,
    failed_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dlq_pipeline_run ON dlq_records (pipeline, run_id);
";

/// `SQLite`-backed state storage.
///
/// Create with [`SqliteStateBackend::open`] for file-backed persistence
/// or [`SqliteStateBackend::in_memory`] for tests.
pub struct SqliteStateBackend {
    conn: Mutex<Connection>,
}

#[cfg(test)]
type RunRow = (String, i64, i64, Option<String>, Option<String>);

impl SqliteStateBackend {
    /// Open or create a `SQLite` state database at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Io`] if the directory can't be created,
    /// or [`StateError::Backend`] if the database can't be opened.
    pub fn open(path: &Path) -> error::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path).map_err(StateError::backend)?;
        conn.execute_batch(CREATE_TABLES)
            .map_err(StateError::backend)?;
        Self::migrate_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create an in-memory `SQLite` backend (for testing).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Backend`] if the in-memory database can't
    /// be initialized.
    pub fn in_memory() -> error::Result<Self> {
        let conn = Connection::open_in_memory().map_err(StateError::backend)?;
        conn.execute_batch(CREATE_TABLES)
            .map_err(StateError::backend)?;
        Self::migrate_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn migrate_schema(conn: &Connection) -> error::Result<()> {
        match conn.execute(
            "ALTER TABLE sync_runs ADD COLUMN bytes_written INTEGER DEFAULT 0",
            [],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(_, Some(message)))
                if message.contains("duplicate column name") =>
            {
                Ok(())
            }
            Err(err) => Err(StateError::backend(err)),
        }
    }

    /// Acquire the connection lock.
    fn lock_conn(&self) -> error::Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|_| StateError::LockPoisoned)
    }

    /// Format current UTC time for `SQLite` storage.
    fn now_sqlite() -> String {
        Utc::now().format(SQLITE_DATETIME_FMT).to_string()
    }

    /// Convert a `SQLite` datetime string to ISO-8601.
    fn sqlite_to_iso8601(raw: &str) -> String {
        NaiveDateTime::parse_from_str(raw, SQLITE_DATETIME_FMT).map_or_else(
            |e| {
                tracing::warn!(raw, %e, "failed to parse SQLite datetime, returning raw value");
                raw.to_string()
            },
            |ndt| format!("{}Z", ndt.format("%Y-%m-%dT%H:%M:%S")),
        )
    }

    /// Convert an ISO-8601 string to `SQLite` datetime format.
    fn iso8601_to_sqlite(iso: &str) -> String {
        chrono::DateTime::parse_from_rfc3339(iso).map_or_else(
            |e| {
                tracing::warn!(iso, %e, "failed to parse ISO-8601 datetime, storing raw value");
                iso.to_string()
            },
            |dt| dt.format(SQLITE_DATETIME_FMT).to_string(),
        )
    }

    #[cfg(test)]
    fn get_run_row(&self, run_id: i64) -> error::Result<RunRow> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT status, records_read, bytes_written, finished_at, error_message \
             FROM sync_runs WHERE id = ?1",
            [run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .map_err(StateError::backend)
    }

    #[cfg(test)]
    fn count_dlq_records_for_run(&self, pipeline: &PipelineId, run_id: i64) -> error::Result<i64> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT COUNT(*) FROM dlq_records WHERE pipeline = ?1 AND run_id = ?2",
            rusqlite::params![pipeline.as_str(), run_id],
            |row| row.get(0),
        )
        .map_err(StateError::backend)
    }

    #[cfg(test)]
    fn first_dlq_stream_error(&self, pipeline: &PipelineId) -> error::Result<(String, String)> {
        let conn = self.lock_conn()?;
        conn.query_row(
            "SELECT stream_name, error_message FROM dlq_records \
             WHERE pipeline = ?1 ORDER BY id LIMIT 1",
            rusqlite::params![pipeline.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(StateError::backend)
    }
}

impl StateBackend for SqliteStateBackend {
    fn get_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> error::Result<Option<CursorState>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT cursor_field, cursor_value, updated_at \
                 FROM sync_cursors WHERE pipeline = ?1 AND stream = ?2",
            )
            .map_err(StateError::backend)?;

        let result = stmt.query_row(
            rusqlite::params![pipeline.as_str(), stream.as_str()],
            |row| {
                let cursor_field: Option<String> = row.get(0)?;
                let cursor_value: Option<String> = row.get(1)?;
                let updated_at_str: String = row.get(2)?;
                Ok((cursor_field, cursor_value, updated_at_str))
            },
        );

        match result {
            Ok((cursor_field, cursor_value, updated_at_str)) => Ok(Some(CursorState {
                cursor_field,
                cursor_value,
                updated_at: Self::sqlite_to_iso8601(&updated_at_str),
            })),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StateError::backend(e)),
        }
    }

    fn set_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> error::Result<()> {
        let conn = self.lock_conn()?;
        let updated_at = Self::iso8601_to_sqlite(&cursor.updated_at);
        conn.execute(
            "INSERT INTO sync_cursors (pipeline, stream, cursor_field, cursor_value, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(pipeline, stream) \
             DO UPDATE SET cursor_field = ?3, cursor_value = ?4, updated_at = ?5",
            rusqlite::params![
                pipeline.as_str(),
                stream.as_str(),
                cursor.cursor_field,
                cursor.cursor_value,
                updated_at,
            ],
        )
        .map_err(StateError::backend)?;
        Ok(())
    }

    fn start_run(&self, pipeline: &PipelineId, stream: &StreamName) -> error::Result<i64> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT INTO sync_runs (pipeline, stream, status) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                pipeline.as_str(),
                stream.as_str(),
                RunStatus::Running.as_str()
            ],
        )
        .map_err(StateError::backend)?;
        Ok(conn.last_insert_rowid())
    }

    #[allow(clippy::cast_possible_wrap, clippy::similar_names)]
    fn complete_run(&self, run_id: i64, status: RunStatus, stats: &RunStats) -> error::Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "UPDATE sync_runs SET status = ?1, finished_at = datetime('now'), \
             records_read = ?2, records_written = ?3, bytes_read = ?4, bytes_written = ?5, error_message = ?6 \
             WHERE id = ?7",
            rusqlite::params![
                status.as_str(),
                stats.records_read as i64,
                stats.records_written as i64,
                stats.bytes_read as i64,
                stats.bytes_written as i64,
                stats.error_message,
                run_id,
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
        let conn = self.lock_conn()?;
        let now = Self::now_sqlite();

        let rows_affected = match expected {
            Some(expected_val) => conn
                .execute(
                    "UPDATE sync_cursors SET cursor_value = ?1, updated_at = ?2 \
                     WHERE pipeline = ?3 AND stream = ?4 AND cursor_value = ?5",
                    rusqlite::params![
                        new_value,
                        now,
                        pipeline.as_str(),
                        stream.as_str(),
                        expected_val
                    ],
                )
                .map_err(StateError::backend)?,
            None => conn
                .execute(
                    "INSERT OR IGNORE INTO sync_cursors (pipeline, stream, cursor_value, updated_at) \
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![pipeline.as_str(), stream.as_str(), new_value, now],
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

        let conn = self.lock_conn()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| StateError::backend_context("insert_dlq_records: begin tx", e))?;
        let mut stmt = tx
            .prepare(
                "INSERT INTO dlq_records \
                 (pipeline, run_id, stream_name, record_json, error_message, error_category, failed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )
            .map_err(|e| StateError::backend_context("insert_dlq_records: prepare", e))?;

        for record in records {
            stmt.execute(rusqlite::params![
                pipeline.as_str(),
                run_id,
                record.stream_name,
                record.record_json,
                record.error_message,
                record.error_category.as_str(),
                record.failed_at.as_str(),
            ])
            .map_err(|e| StateError::backend_context("insert_dlq_records: execute", e))?;
        }
        drop(stmt);
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

    fn pid(name: &str) -> PipelineId {
        PipelineId::new(name)
    }

    fn stream(name: &str) -> StreamName {
        StreamName::new(name)
    }

    fn now_iso() -> String {
        Utc::now().to_rfc3339()
    }

    #[test]
    fn cursor_roundtrip() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        assert!(backend
            .get_cursor(&pid("p"), &stream("s"))
            .unwrap()
            .is_none());

        backend
            .set_cursor(
                &pid("p"),
                &stream("s"),
                &CursorState {
                    cursor_field: Some("updated_at".into()),
                    cursor_value: Some("2024-01-15T10:00:00Z".into()),
                    updated_at: now_iso(),
                },
            )
            .unwrap();

        let cursor = backend
            .get_cursor(&pid("p"), &stream("s"))
            .unwrap()
            .unwrap();
        assert_eq!(cursor.cursor_field, Some("updated_at".into()));
        assert_eq!(cursor.cursor_value, Some("2024-01-15T10:00:00Z".into()));
        assert!(!cursor.updated_at.is_empty());
    }

    #[test]
    fn cursor_upsert() {
        let backend = SqliteStateBackend::in_memory().unwrap();

        backend
            .set_cursor(
                &pid("p"),
                &stream("s"),
                &CursorState {
                    cursor_field: Some("id".into()),
                    cursor_value: Some("100".into()),
                    updated_at: now_iso(),
                },
            )
            .unwrap();

        backend
            .set_cursor(
                &pid("p"),
                &stream("s"),
                &CursorState {
                    cursor_field: Some("id".into()),
                    cursor_value: Some("200".into()),
                    updated_at: now_iso(),
                },
            )
            .unwrap();

        let cursor = backend
            .get_cursor(&pid("p"), &stream("s"))
            .unwrap()
            .unwrap();
        assert_eq!(cursor.cursor_value, Some("200".into()));
    }

    #[test]
    fn different_pipelines_independent() {
        let backend = SqliteStateBackend::in_memory().unwrap();

        backend
            .set_cursor(
                &pid("a"),
                &stream("s"),
                &CursorState {
                    cursor_field: None,
                    cursor_value: Some("aaa".into()),
                    updated_at: now_iso(),
                },
            )
            .unwrap();

        backend
            .set_cursor(
                &pid("b"),
                &stream("s"),
                &CursorState {
                    cursor_field: None,
                    cursor_value: Some("bbb".into()),
                    updated_at: now_iso(),
                },
            )
            .unwrap();

        let a = backend
            .get_cursor(&pid("a"), &stream("s"))
            .unwrap()
            .unwrap();
        let b = backend
            .get_cursor(&pid("b"), &stream("s"))
            .unwrap()
            .unwrap();
        assert_eq!(a.cursor_value, Some("aaa".into()));
        assert_eq!(b.cursor_value, Some("bbb".into()));
    }

    #[test]
    fn run_lifecycle() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run_id = backend.start_run(&pid("p"), &stream("s")).unwrap();
        assert!(run_id > 0);

        backend
            .complete_run(
                run_id,
                RunStatus::Completed,
                &RunStats {
                    records_read: 1000,
                    records_written: 1000,
                    bytes_read: 50000,
                    bytes_written: 45000,
                    error_message: None,
                },
            )
            .unwrap();

        let (status, records_read, bytes_written, finished, _error) =
            backend.get_run_row(run_id).unwrap();
        assert_eq!(status, "completed");
        assert_eq!(records_read, 1000);
        assert_eq!(bytes_written, 45000);
        assert!(finished.is_some());
    }

    #[test]
    fn run_failure() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run_id = backend.start_run(&pid("p"), &stream("orders")).unwrap();

        backend
            .complete_run(
                run_id,
                RunStatus::Failed,
                &RunStats {
                    records_read: 50,
                    records_written: 0,
                    bytes_read: 2000,
                    bytes_written: 0,
                    error_message: Some("Connection reset".into()),
                },
            )
            .unwrap();

        let (status, _records, _bytes_written, _finished, error_msg) =
            backend.get_run_row(run_id).unwrap();
        assert_eq!(status, "failed");
        assert_eq!(error_msg, Some("Connection reset".into()));
    }

    #[test]
    fn multiple_runs() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run1 = backend.start_run(&pid("p"), &stream("s")).unwrap();
        let run2 = backend.start_run(&pid("p"), &stream("s")).unwrap();
        assert_ne!(run1, run2);
        assert!(run2 > run1);
    }

    #[test]
    fn dlq_records_insert_and_count() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run_id = backend.start_run(&pid("p"), &stream("s")).unwrap();

        let records = vec![
            DlqRecord {
                stream_name: "users".into(),
                record_json: r#"{"id":1}"#.into(),
                error_message: "not-null violation".into(),
                error_category: ErrorCategory::Data,
                failed_at: Timestamp::new("2026-02-21T12:00:00+00:00"),
            },
            DlqRecord {
                stream_name: "users".into(),
                record_json: r#"{"id":2}"#.into(),
                error_message: "type mismatch".into(),
                error_category: ErrorCategory::Data,
                failed_at: Timestamp::new("2026-02-21T12:00:01+00:00"),
            },
        ];

        let count = backend
            .insert_dlq_records(&pid("p"), run_id, &records)
            .unwrap();
        assert_eq!(count, 2);

        let stored = backend
            .count_dlq_records_for_run(&pid("p"), run_id)
            .unwrap();
        assert_eq!(stored, 2);

        let (stream_name, error_msg) = backend.first_dlq_stream_error(&pid("p")).unwrap();
        assert_eq!(stream_name, "users");
        assert_eq!(error_msg, "not-null violation");
    }

    #[test]
    fn dlq_records_empty_insert() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let count = backend.insert_dlq_records(&pid("p"), 1, &[]).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn dlq_records_invalid_run_id_includes_operation_context() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let records = vec![DlqRecord {
            stream_name: "users".into(),
            record_json: r#"{"id":1}"#.into(),
            error_message: "bad row".into(),
            error_category: ErrorCategory::Data,
            failed_at: Timestamp::new("2026-02-21T12:00:00+00:00"),
        }];

        let err = backend
            .insert_dlq_records(&pid("p"), 999, &records)
            .expect_err("invalid run id should fail");
        assert!(err.to_string().contains("insert_dlq_records"));
    }

    #[test]
    fn compare_and_set_success() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        backend
            .set_cursor(
                &pid("p"),
                &stream("s"),
                &CursorState {
                    cursor_field: Some("id".into()),
                    cursor_value: Some("100".into()),
                    updated_at: now_iso(),
                },
            )
            .unwrap();

        let result = backend
            .compare_and_set(&pid("p"), &stream("s"), Some("100"), "200")
            .unwrap();
        assert!(result);

        let got = backend
            .get_cursor(&pid("p"), &stream("s"))
            .unwrap()
            .unwrap();
        assert_eq!(got.cursor_value, Some("200".into()));
    }

    #[test]
    fn compare_and_set_failure_mismatch() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        backend
            .set_cursor(
                &pid("p"),
                &stream("s"),
                &CursorState {
                    cursor_field: Some("id".into()),
                    cursor_value: Some("100".into()),
                    updated_at: now_iso(),
                },
            )
            .unwrap();

        let result = backend
            .compare_and_set(&pid("p"), &stream("s"), Some("999"), "200")
            .unwrap();
        assert!(!result);

        let got = backend
            .get_cursor(&pid("p"), &stream("s"))
            .unwrap()
            .unwrap();
        assert_eq!(got.cursor_value, Some("100".into()));
    }

    #[test]
    fn compare_and_set_from_none() {
        let backend = SqliteStateBackend::in_memory().unwrap();

        let result = backend
            .compare_and_set(&pid("p"), &stream("s"), None, "50")
            .unwrap();
        assert!(result);

        let got = backend
            .get_cursor(&pid("p"), &stream("s"))
            .unwrap()
            .unwrap();
        assert_eq!(got.cursor_value, Some("50".into()));
    }

    #[test]
    fn compare_and_set_from_none_but_exists() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        backend
            .set_cursor(
                &pid("p"),
                &stream("s"),
                &CursorState {
                    cursor_field: Some("id".into()),
                    cursor_value: Some("100".into()),
                    updated_at: now_iso(),
                },
            )
            .unwrap();

        let result = backend
            .compare_and_set(&pid("p"), &stream("s"), None, "200")
            .unwrap();
        assert!(!result);

        let got = backend
            .get_cursor(&pid("p"), &stream("s"))
            .unwrap()
            .unwrap();
        assert_eq!(got.cursor_value, Some("100".into()));
    }

    #[test]
    fn sqlite_to_iso8601_conversion() {
        let iso = SqliteStateBackend::sqlite_to_iso8601("2024-01-15 10:00:00");
        assert_eq!(iso, "2024-01-15T10:00:00Z");
    }

    #[test]
    fn iso8601_to_sqlite_conversion() {
        let sqlite = SqliteStateBackend::iso8601_to_sqlite("2024-01-15T10:00:00Z");
        assert_eq!(sqlite, "2024-01-15 10:00:00");
    }
}
