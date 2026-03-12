//! DDL orchestration: table creation, schema drift handling, and staging swaps.

mod drift;
mod staging;

use std::collections::HashSet;

use pg_escape::quote_identifier;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::SchemaEvolutionPolicy;

use self::drift::detect_schema_drift;
use crate::pg_error::format_pg_error;
use crate::types::arrow_to_pg_type;

pub(crate) use self::staging::{prepare_staging, swap_staging_table};

fn is_pg_type_typname_race(code: &str, message: &str, detail: &str) -> bool {
    code == "23505"
        && (message.contains("pg_type_typname_nsp_index")
            || detail.contains("pg_type_typname_nsp_index"))
}

fn is_concurrent_create_table_race(error: &tokio_postgres::Error) -> bool {
    let Some(db_error) = error.as_db_error() else {
        return false;
    };

    is_pg_type_typname_race(
        db_error.code().code(),
        db_error.message(),
        db_error.detail().unwrap_or_default(),
    )
}


/// Bundles the mutable schema-tracking sets used during DDL orchestration.
pub(crate) struct SchemaState {
    pub(crate) created_tables: HashSet<String>,
    pub(crate) ignored_columns: HashSet<String>,
    pub(crate) type_null_columns: HashSet<String>,
}

impl SchemaState {
    pub(crate) fn new() -> Self {
        Self {
            created_tables: HashSet::new(),
            ignored_columns: HashSet::new(),
            type_null_columns: HashSet::new(),
        }
    }
}

/// Create the target table if it doesn't exist, based on the Arrow schema.
async fn create_table(
    ctx: &Context,
    client: &Client,
    qualified_table: &str,
    arrow_schema: &rapidbyte_sdk::arrow::datatypes::Schema,
    primary_key: Option<&[String]>,
) -> Result<(), String> {
    let columns_ddl: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let pg_type = arrow_to_pg_type(field.data_type());
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            format!("{} {}{}", quote_identifier(field.name()), pg_type, nullable)
        })
        .collect();

    let mut ddl_parts: Vec<String> = columns_ddl;

    if let Some(pk_cols) = primary_key {
        if !pk_cols.is_empty() {
            let pk = pk_cols
                .iter()
                .map(|k| quote_identifier(k))
                .collect::<Vec<_>>()
                .join(", ");
            ddl_parts.push(format!("PRIMARY KEY ({pk})"));
        }
    }

    let ddl = format!(
        "CREATE UNLOGGED TABLE IF NOT EXISTS {} ({})",
        qualified_table,
        ddl_parts.join(", ")
    );

    ctx.log(
        LogLevel::Debug,
        &format!("dest-postgres: ensuring table: {ddl}"),
    );

    client
        .execute("SAVEPOINT rb_create_table", &[])
        .await
        .map_err(|e| format_pg_error("Failed to create DDL savepoint", &e))?;

    match client.execute(&ddl, &[]).await {
        Ok(_) => {
            client
                .execute("RELEASE SAVEPOINT rb_create_table", &[])
                .await
                .map_err(|e| format_pg_error("Failed to release DDL savepoint", &e))?;
        }
        Err(error) if is_concurrent_create_table_race(&error) => {
            client
                .execute("ROLLBACK TO SAVEPOINT rb_create_table", &[])
                .await
                .map_err(|e| format_pg_error("Failed to rollback DDL savepoint", &e))?;
            client
                .execute("RELEASE SAVEPOINT rb_create_table", &[])
                .await
                .map_err(|e| format_pg_error("Failed to release DDL savepoint", &e))?;
            ctx.log(
                LogLevel::Warn,
                &format!(
                    "dest-postgres: concurrent CREATE TABLE race detected for {} (pg_type_typname_nsp_index); continuing",
                    qualified_table
                ),
            );
        }
        Err(error) => {
            let _ = client.execute("ROLLBACK TO SAVEPOINT rb_create_table", &[]).await;
            let _ = client.execute("RELEASE SAVEPOINT rb_create_table", &[]).await;
            return Err(format_pg_error(
                &format!("Failed to create table {qualified_table}"),
                &error,
            ));
        }
    }

    Ok(())
}

impl SchemaState {
    /// Ensure target schema, table, and schema drift handling are complete.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn ensure_table(
        &mut self,
        ctx: &Context,
        client: &Client,
        target_schema: &str,
        stream_name: &str,
        write_mode: Option<&WriteMode>,
        schema_policy: Option<&SchemaEvolutionPolicy>,
        arrow_schema: &rapidbyte_sdk::arrow::datatypes::Schema,
    ) -> Result<(), String> {
        let qualified_table = crate::decode::qualified_name(target_schema, stream_name);

        if self.created_tables.contains(&qualified_table) {
            return Ok(());
        }

        let lock_name = format!("rb:ddl:schema:{target_schema}");
        crate::pg_error::with_ddl_lock(client, &lock_name, || async {
            let create_schema = format!(
                "CREATE SCHEMA IF NOT EXISTS {}",
                quote_identifier(target_schema)
            );
            client
                .execute(&create_schema, &[])
                .await
                .map_err(|e| {
                    format_pg_error(&format!("Failed to create schema '{target_schema}'"), &e)
                })?;

            let pk = match write_mode {
                Some(WriteMode::Upsert { primary_key }) => Some(primary_key.as_slice()),
                _ => None,
            };
            create_table(ctx, client, &qualified_table, arrow_schema, pk).await?;
            self.created_tables.insert(qualified_table.clone());

            if let Some(policy) = schema_policy {
                if let Some(drift) =
                    detect_schema_drift(client, target_schema, stream_name, arrow_schema).await?
                {
                    ctx.log(
                        LogLevel::Info,
                        &format!(
                            "dest-postgres: schema drift detected for {}: {} new, {} removed, {} type changes, {} nullability changes",
                            qualified_table,
                            drift.new_columns.len(),
                            drift.removed_columns.len(),
                            drift.type_changes.len(),
                            drift.nullability_changes.len()
                        ),
                    );
                    self.apply_policy(ctx, client, &qualified_table, &drift, policy)
                        .await?;
                }
            }

            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::is_pg_type_typname_race;

    #[test]
    fn detects_pg_type_typname_unique_violation_race() {
        assert!(is_pg_type_typname_race(
            "23505",
            "duplicate key value violates unique constraint \"pg_type_typname_nsp_index\"",
            "Key (typname, typnamespace)=(users, 16425) already exists."
        ));
    }

    #[test]
    fn ignores_other_unique_violations() {
        assert!(!is_pg_type_typname_race(
            "23505",
            "duplicate key value violates unique constraint \"users_pkey\"",
            "Key (id)=(42) already exists."
        ));
    }

    #[test]
    fn ignores_non_unique_violation_codes() {
        assert!(!is_pg_type_typname_race(
            "42P01",
            "relation \"raw_parallel.users\" does not exist",
            ""
        ));
    }
}
