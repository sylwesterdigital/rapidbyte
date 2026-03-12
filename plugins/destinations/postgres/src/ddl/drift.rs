//! Schema drift detection and policy application.

use std::collections::{HashMap, HashSet};

use pg_escape::quote_identifier;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::{
    ColumnPolicy, NullabilityPolicy, SchemaEvolutionPolicy, TypeChangePolicy,
};

use crate::types::{arrow_to_pg_type, pg_types_compatible};

/// Detected differences between an incoming Arrow schema and an existing PG table.
#[derive(Debug, Default)]
pub(crate) struct SchemaDrift {
    /// Columns present in the Arrow schema but not in the existing table (name, `pg_type`).
    pub(crate) new_columns: Vec<(String, String)>,
    /// Columns present in the existing table but not in the Arrow schema.
    pub(crate) removed_columns: Vec<String>,
    /// Columns whose PG type differs (name, `old_pg_type`, `new_pg_type`).
    pub(crate) type_changes: Vec<(String, String, String)>,
    /// Columns whose nullability differs (name, `was_nullable`, `now_nullable`).
    pub(crate) nullability_changes: Vec<(String, bool, bool)>,
}

impl SchemaDrift {
    fn is_empty(&self) -> bool {
        self.new_columns.is_empty()
            && self.removed_columns.is_empty()
            && self.type_changes.is_empty()
            && self.nullability_changes.is_empty()
    }
}

/// Fetch existing column names, types, and nullability from `information_schema`.
async fn get_existing_columns(
    client: &Client,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<(String, String, bool)>, String> {
    let rows = client
        .query(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
            &[&schema_name, &table_name],
        )
        .await
        .map_err(|e| format!("Failed to query existing columns: {e}"))?;

    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.get(0);
            let dtype: String = r.get(1);
            let nullable: String = r.get(2);
            (name, dtype, nullable == "YES")
        })
        .collect())
}

/// Detect schema differences between an Arrow schema and an existing PG table.
pub(crate) async fn detect_schema_drift(
    client: &Client,
    schema_name: &str,
    table_name: &str,
    arrow_schema: &rapidbyte_sdk::arrow::datatypes::Schema,
) -> Result<Option<SchemaDrift>, String> {
    let existing = get_existing_columns(client, schema_name, table_name).await?;
    if existing.is_empty() {
        return Ok(None);
    }

    let existing_names: HashSet<&str> = existing.iter().map(|(n, _, _)| n.as_str()).collect();
    let arrow_names: HashSet<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // New columns: present in Arrow schema but absent from the existing table.
    let new_columns: Vec<(String, String)> = arrow_schema
        .fields()
        .iter()
        .filter(|f| !existing_names.contains(f.name().as_str()))
        .map(|f| {
            (
                f.name().clone(),
                arrow_to_pg_type(f.data_type()).to_string(),
            )
        })
        .collect();

    // Removed columns: present in existing table but absent from Arrow schema.
    let removed_columns: Vec<String> = existing
        .iter()
        .filter(|(n, _, _)| !arrow_names.contains(n.as_str()))
        .map(|(n, _, _)| n.clone())
        .collect();

    // Type and nullability changes for columns present in both schemas.
    let mut type_changes = Vec::new();
    let mut nullability_changes = Vec::new();

    let existing_map: HashMap<&str, (&str, bool)> = existing
        .iter()
        .map(|(n, t, nullable)| (n.as_str(), (t.as_str(), *nullable)))
        .collect();

    for field in arrow_schema.fields() {
        if let Some(&(old_type, old_nullable)) = existing_map.get(field.name().as_str()) {
            let new_pg_type = arrow_to_pg_type(field.data_type());
            if !pg_types_compatible(old_type, new_pg_type) {
                type_changes.push((
                    field.name().clone(),
                    old_type.to_owned(),
                    new_pg_type.to_string(),
                ));
            }
            if old_nullable != field.is_nullable() {
                nullability_changes.push((field.name().clone(), old_nullable, field.is_nullable()));
            }
        }
    }

    let drift = SchemaDrift {
        new_columns,
        removed_columns,
        type_changes,
        nullability_changes,
    };

    if drift.is_empty() {
        Ok(None)
    } else {
        Ok(Some(drift))
    }
}

use super::SchemaState;

impl SchemaState {
    /// Apply schema evolution policy to detected drift, executing DDL as needed.
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn apply_policy(
        &mut self,
        ctx: &Context,
        client: &Client,
        qualified_table: &str,
        drift: &SchemaDrift,
        policy: &SchemaEvolutionPolicy,
    ) -> Result<(), String> {
        // Handle new columns.
        for (col_name, pg_type) in &drift.new_columns {
            match policy.new_column {
                ColumnPolicy::Fail => {
                    return Err(format!(
                        "Schema evolution: new column '{col_name}' detected but policy is 'fail'"
                    ));
                }
                ColumnPolicy::Add => {
                    let sql = format!(
                        "ALTER TABLE {} ADD COLUMN {} {}",
                        qualified_table,
                        quote_identifier(col_name),
                        pg_type
                    );
                    client
                        .execute(&sql, &[])
                        .await
                        .map_err(|e| format!("ALTER TABLE ADD COLUMN '{col_name}' failed: {e}"))?;
                    ctx.log(
                        LogLevel::Info,
                        &format!("dest-postgres: added column '{col_name}' {pg_type}"),
                    );
                }
                ColumnPolicy::Ignore => {
                    self.ignored_columns.insert(col_name.clone());
                    ctx.log(
                        LogLevel::Info,
                        &format!(
                            "dest-postgres: ignoring new column '{col_name}' per schema policy (excluded from writes)"
                        ),
                    );
                }
            }
        }

        // Handle removed columns.
        for col_name in &drift.removed_columns {
            match policy.removed_column {
                ColumnPolicy::Fail => {
                    return Err(format!(
                        "Schema evolution: column '{col_name}' removed but policy is 'fail'"
                    ));
                }
                ColumnPolicy::Ignore | ColumnPolicy::Add => {
                    ctx.log(
                        LogLevel::Info,
                        &format!(
                            "dest-postgres: column '{col_name}' removed from source, keeping in table per policy"
                        ),
                    );
                }
            }
        }

        // Handle type changes.
        for (col_name, old_type, new_type) in &drift.type_changes {
            match policy.type_change {
                TypeChangePolicy::Fail => {
                    return Err(format!(
                        "Schema evolution: type change for '{col_name}' ({old_type} -> {new_type}) but policy is 'fail'"
                    ));
                }
                TypeChangePolicy::Coerce => {
                    let col_ident = quote_identifier(col_name);
                    let sql = format!(
                        "ALTER TABLE {qualified_table} ALTER COLUMN {col_ident} TYPE {new_type} USING {col_ident}::{new_type}"
                    );
                    client.execute(&sql, &[]).await.map_err(|e| {
                        format!(
                            "Schema evolution: ALTER COLUMN '{col_name}' TYPE {new_type} failed: {e}"
                        )
                    })?;
                    ctx.log(
                        LogLevel::Info,
                        &format!(
                            "dest-postgres: coerced '{col_name}' from {old_type} to {new_type}"
                        ),
                    );
                }
                TypeChangePolicy::Null => {
                    self.type_null_columns.insert(col_name.clone());
                    ctx.log(
                        LogLevel::Info,
                        &format!(
                            "dest-postgres: type change for '{col_name}' ({old_type} -> {new_type}), policy=Null — values will be NULL"
                        ),
                    );
                }
            }
        }

        // Handle nullability changes.
        for (col_name, was_nullable, now_nullable) in &drift.nullability_changes {
            match policy.nullability_change {
                NullabilityPolicy::Fail => {
                    return Err(format!(
                        "Schema evolution: nullability change for '{col_name}' ({was_nullable} -> {now_nullable}) but policy is 'fail'"
                    ));
                }
                NullabilityPolicy::Allow => {
                    let col_ident = quote_identifier(col_name);
                    if *was_nullable && !now_nullable {
                        let sql = format!(
                            "ALTER TABLE {qualified_table} ALTER COLUMN {col_ident} SET NOT NULL"
                        );
                        match client.execute(&sql, &[]).await {
                            Ok(_) => {
                                ctx.log(
                                    LogLevel::Info,
                                    &format!("dest-postgres: SET NOT NULL on '{col_name}'"),
                                );
                            }
                            Err(e) => {
                                ctx.log(
                                    LogLevel::Warn,
                                    &format!(
                                        "dest-postgres: SET NOT NULL on '{col_name}' failed (existing NULLs?): {e}"
                                    ),
                                );
                            }
                        }
                    } else if !was_nullable && *now_nullable {
                        let sql = format!(
                            "ALTER TABLE {qualified_table} ALTER COLUMN {col_ident} DROP NOT NULL"
                        );
                        client.execute(&sql, &[]).await.map_err(|e| {
                            format!("ALTER TABLE DROP NOT NULL on '{col_name}' failed: {e}")
                        })?;
                        ctx.log(
                            LogLevel::Info,
                            &format!("dest-postgres: DROP NOT NULL on '{col_name}'"),
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_drift_default_is_empty() {
        let drift = SchemaDrift::default();
        assert!(drift.is_empty());
    }

    #[test]
    fn schema_drift_detects_all_categories() {
        let drift = SchemaDrift {
            new_columns: vec![("age".into(), "INTEGER".into())],
            removed_columns: vec!["old_col".into()],
            type_changes: vec![("name".into(), "integer".into(), "TEXT".into())],
            nullability_changes: vec![("id".into(), true, false)],
        };
        assert!(!drift.is_empty());
        assert_eq!(drift.new_columns.len(), 1);
        assert_eq!(drift.removed_columns.len(), 1);
        assert_eq!(drift.type_changes.len(), 1);
        assert_eq!(drift.nullability_changes.len(), 1);
    }
}
