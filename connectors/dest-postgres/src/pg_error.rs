//! Shared `PostgreSQL` error formatting for the destination connector.

pub(crate) fn format_pg_error(prefix: &str, error: &tokio_postgres::Error) -> String {
    if let Some(db_error) = error.as_db_error() {
        let detail = db_error.detail().unwrap_or("n/a");
        let hint = db_error.hint().unwrap_or("n/a");
        format!(
            "{prefix}: {} (sqlstate={} severity={} detail={} hint={})",
            db_error.message(),
            db_error.code().code(),
            db_error.severity(),
            detail,
            hint
        )
    } else {
        format!("{prefix}: {error}")
    }
}

pub(crate) async fn with_ddl_lock<F, Fut, T>(
    client: &tokio_postgres::Client,
    lock_name: &str,
    f: F,
) -> Result<T, String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    client
        .query_one(
            "SELECT pg_advisory_lock(hashtext($1)::bigint)",
            &[&lock_name],
        )
        .await
        .map_err(|e| {
            format_pg_error(
                &format!("Failed to acquire DDL advisory lock '{lock_name}'"),
                &e,
            )
        })?;

    let result = f().await;

    let unlock_result = client
        .query_one(
            "SELECT pg_advisory_unlock(hashtext($1)::bigint)",
            &[&lock_name],
        )
        .await
        .map_err(|e| {
            format_pg_error(
                &format!("Failed to release DDL advisory lock '{lock_name}'"),
                &e,
            )
        });

    match (result, unlock_result) {
        (Ok(value), Ok(_)) => Ok(value),
        (Err(err), Ok(_)) => Err(err),
        (Ok(_), Err(unlock_err)) => Err(unlock_err),
        (Err(err), Err(_unlock_err)) => Err(err),
    }
}
