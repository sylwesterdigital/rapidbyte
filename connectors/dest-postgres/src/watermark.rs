//! Watermark CRUD for dest-postgres resume tracking.
//!
//! Maintains the `__rb_watermarks` metadata table that records how many rows
//! and bytes have been committed for each stream, enabling safe resume after
//! partial runs.

use pg_escape::quote_identifier;
use tokio_postgres::Client;

use crate::pg_error::format_pg_error;

/// Build the fully-qualified watermarks table name for the given schema.
fn watermarks_table(target_schema: &str) -> String {
    format!("{}.__rb_watermarks", quote_identifier(target_schema))
}

/// Ensure the `__rb_watermarks` metadata table exists, creating the schema
/// first if necessary.
pub(crate) async fn ensure_table(client: &Client, target_schema: &str) -> Result<(), String> {
    let lock_name = format!("rb:ddl:schema:{target_schema}");
    crate::pg_error::with_ddl_lock(client, &lock_name, || async {
        let create_schema = format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_identifier(target_schema)
        );
        client.execute(&create_schema, &[]).await.map_err(|e| {
            format_pg_error(&format!("Failed to create schema '{target_schema}'"), &e)
        })?;

        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                stream_name TEXT PRIMARY KEY,
                records_committed BIGINT NOT NULL DEFAULT 0,
                bytes_committed BIGINT NOT NULL DEFAULT 0,
                committed_at TIMESTAMP NOT NULL DEFAULT NOW()
            )",
            watermarks_table(target_schema)
        );
        client
            .execute(&ddl, &[])
            .await
            .map_err(|e| format_pg_error("Failed to create watermarks table", &e))?;
        Ok(())
    })
    .await
}

/// Get watermark (records committed) for a stream. Returns 0 if none.
pub(crate) async fn get(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<u64, String> {
    let sql = format!(
        "SELECT records_committed FROM {} WHERE stream_name = $1",
        watermarks_table(target_schema)
    );
    match client.query_opt(&sql, &[&stream_name]).await {
        Ok(Some(row)) => {
            let val: i64 = row.get(0);
            // Safety: records_committed stored as BIGINT (i64) is always non-negative.
            #[allow(clippy::cast_sign_loss)]
            let count = val as u64;
            Ok(count)
        }
        Ok(None) => Ok(0),
        Err(e) => Err(format_pg_error("Failed to get watermark", &e)),
    }
}

/// Upsert watermark row inside the same transaction as data writes.
pub(crate) async fn set(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    records_committed: u64,
    bytes_committed: u64,
) -> Result<(), String> {
    let sql = format!(
        "INSERT INTO {} (stream_name, records_committed, bytes_committed, committed_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (stream_name)
         DO UPDATE SET records_committed = $2, bytes_committed = $3, committed_at = NOW()",
        watermarks_table(target_schema)
    );
    client
        .execute(
            &sql,
            &[
                &stream_name,
                // Safety: row/byte counts are always non-negative and won't exceed i64::MAX
                // in practice. PostgreSQL BIGINT is i64.
                #[allow(clippy::cast_possible_wrap)]
                &(records_committed as i64),
                #[allow(clippy::cast_possible_wrap)]
                &(bytes_committed as i64),
            ],
        )
        .await
        .map_err(|e| format_pg_error("Failed to set watermark", &e))?;
    Ok(())
}

/// Clear watermark for a stream after successful completion.
pub(crate) async fn clear(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<(), String> {
    let sql = format!(
        "DELETE FROM {} WHERE stream_name = $1",
        watermarks_table(target_schema)
    );
    client
        .execute(&sql, &[&stream_name])
        .await
        .map_err(|e| format_pg_error("Failed to clear watermark", &e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermarks_table_qualifies_correctly() {
        assert_eq!(watermarks_table("public"), "public.__rb_watermarks");
        assert_eq!(watermarks_table("raw"), "raw.__rb_watermarks");
        assert_eq!(
            watermarks_table("my schema"),
            r#""my schema".__rb_watermarks"#
        );
    }
}
