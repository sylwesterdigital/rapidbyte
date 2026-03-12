use std::collections::HashSet;
use std::sync::Arc;

use tokio_postgres::Client;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::catalog::SchemaHint;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::stream::SchemaEvolutionPolicy;

use crate::config::LoadMethod;
use crate::ddl::prepare_staging;
use crate::decode;

/// Immutable setup output for destination worker execution.
#[derive(Debug, Clone)]
pub struct WriteContract {
    pub target_schema: String,
    pub stream_name: String,
    pub effective_stream: String,
    pub qualified_table: String,
    pub effective_write_mode: Option<WriteMode>,
    pub schema_policy: SchemaEvolutionPolicy,
    pub needs_schema_ensure: bool,
    pub use_watermarks: bool,
    pub checkpoint: CheckpointConfig,
    pub copy_flush_bytes: Option<usize>,
    pub load_method: LoadMethod,
    pub is_replace: bool,
    pub watermark_records: u64,
    pub ignored_columns: HashSet<String>,
    pub type_null_columns: HashSet<String>,
}

/// Checkpoint threshold configuration extracted from `StreamLimits`.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    pub bytes: u64,
    pub rows: u64,
    pub seconds: u64,
}

fn preflight_schema_from_hint(schema_hint: &SchemaHint) -> Option<Arc<Schema>> {
    match schema_hint {
        SchemaHint::Columns(columns) => {
            if columns.is_empty() {
                None
            } else {
                Some(build_arrow_schema(columns))
            }
        }
        SchemaHint::ArrowIpc(ipc_bytes) => decode_ipc(ipc_bytes).ok().map(|(schema, _)| schema),
        _ => None,
    }
}

pub(crate) fn schema_hint_has_shape(schema_hint: &SchemaHint) -> bool {
    match schema_hint {
        SchemaHint::Columns(columns) => !columns.is_empty(),
        SchemaHint::ArrowIpc(ipc_bytes) => !ipc_bytes.is_empty(),
        _ => false,
    }
}

/// Build a destination write contract for a stream.
pub(crate) fn prepare_stream_once(
    target_schema: &str,
    stream_name: &str,
    write_mode: Option<WriteMode>,
    _schema_hint: &SchemaHint,
    use_watermarks: bool,
    schema_policy: SchemaEvolutionPolicy,
    checkpoint: CheckpointConfig,
    copy_flush_bytes: Option<usize>,
    load_method: LoadMethod,
) -> Result<WriteContract, String> {
    if stream_name.trim().is_empty() {
        return Err("stream name must not be empty".to_string());
    }

    let is_replace = matches!(write_mode, Some(WriteMode::Replace));
    let effective_write_mode = if is_replace {
        Some(WriteMode::Append)
    } else {
        write_mode
    };

    Ok(WriteContract {
        target_schema: target_schema.to_string(),
        stream_name: stream_name.to_string(),
        effective_stream: stream_name.to_string(),
        qualified_table: decode::qualified_name(target_schema, stream_name),
        effective_write_mode,
        schema_policy,
        needs_schema_ensure: true,
        use_watermarks,
        checkpoint,
        copy_flush_bytes,
        load_method,
        is_replace,
        watermark_records: 0,
        ignored_columns: HashSet::new(),
        type_null_columns: HashSet::new(),
    })
}

pub(crate) async fn async_prepare_stream_once(
    ctx: &Context,
    client: &Client,
    schema_hint: &SchemaHint,
    mut contract: WriteContract,
) -> Result<WriteContract, String> {
    if contract.use_watermarks {
        crate::watermark::ensure_table(client, &contract.target_schema)
            .await
            .map_err(|e| format!("dest-postgres: watermarks table creation failed: {e}"))?;
    }

    if contract.is_replace {
        let staging_name =
            prepare_staging(ctx, client, &contract.target_schema, &contract.stream_name)
                .await
                .map_err(|e| format!("{e:#}"))?;
        ctx.log(
            LogLevel::Info,
            &format!("dest-postgres: Replace mode — writing to staging table '{staging_name}'"),
        );
        contract.effective_stream = staging_name;
        contract.effective_write_mode = Some(WriteMode::Append);
    }

    let mut schema_state = crate::ddl::SchemaState::new();
    if let Some(schema) = preflight_schema_from_hint(schema_hint) {
        schema_state
            .ensure_table(
                ctx,
                client,
                &contract.target_schema,
                &contract.effective_stream,
                contract.effective_write_mode.as_ref(),
                Some(&contract.schema_policy),
                &schema,
            )
            .await?;
        contract.needs_schema_ensure = false;
    }

    contract.qualified_table =
        decode::qualified_name(&contract.target_schema, &contract.effective_stream);
    contract.ignored_columns = schema_state.ignored_columns;
    contract.type_null_columns = schema_state.type_null_columns;

    contract.watermark_records = if contract.is_replace || !contract.use_watermarks {
        0
    } else {
        match crate::watermark::get(client, &contract.target_schema, &contract.stream_name).await {
            Ok(w) => {
                if w > 0 {
                    ctx.log(
                        LogLevel::Warn,
                        &format!(
                            "dest-postgres: ignoring stale watermark for stream '{}' ({w} committed records); row-count resume is disabled until checkpoint-safe recovery lands",
                            contract.stream_name
                        ),
                    );
                    let _ = crate::watermark::clear(
                        client,
                        &contract.target_schema,
                        &contract.stream_name,
                    )
                    .await;
                }
                0
            }
            Err(e) => {
                ctx.log(
                    LogLevel::Warn,
                    &format!("dest-postgres: watermark query failed (starting fresh): {e}"),
                );
                0
            }
        }
    };

    Ok(contract)
}

#[cfg(test)]
mod tests {
    use rapidbyte_sdk::catalog::{ColumnSchema, SchemaHint};
    use rapidbyte_sdk::prelude::ArrowDataType;

    use super::*;

    #[test]
    fn write_contract_clone_preserves_fields() {
        let contract = WriteContract {
            target_schema: "raw".to_string(),
            stream_name: "users".to_string(),
            effective_stream: "users".to_string(),
            qualified_table: "raw.users".to_string(),
            effective_write_mode: Some(WriteMode::Append),
            schema_policy: SchemaEvolutionPolicy::default(),
            needs_schema_ensure: true,
            use_watermarks: true,
            checkpoint: CheckpointConfig {
                bytes: 1024,
                rows: 100,
                seconds: 30,
            },
            copy_flush_bytes: Some(4 * 1024 * 1024),
            load_method: LoadMethod::Copy,
            is_replace: false,
            watermark_records: 0,
            ignored_columns: std::collections::HashSet::new(),
            type_null_columns: std::collections::HashSet::new(),
        };

        let cloned = contract.clone();
        assert_eq!(cloned.stream_name, "users");
        assert_eq!(cloned.qualified_table, "raw.users");
        assert_eq!(cloned.copy_flush_bytes, Some(4 * 1024 * 1024));
    }

    #[test]
    fn prepare_stream_once_requires_non_empty_stream_name() {
        let result = prepare_stream_once(
            "raw",
            "",
            Some(WriteMode::Append),
            &SchemaHint::Columns(Vec::new()),
            true,
            SchemaEvolutionPolicy::default(),
            CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            LoadMethod::Copy,
        );

        assert!(result.is_err());
        assert!(result
            .expect_err("empty stream must fail")
            .contains("stream name"));
    }

    #[test]
    fn prepare_stream_once_disables_watermarks_for_partitioned_writes() {
        let contract = prepare_stream_once(
            "raw",
            "users",
            Some(WriteMode::Append),
            &SchemaHint::Columns(Vec::new()),
            false,
            SchemaEvolutionPolicy::default(),
            CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            LoadMethod::Insert,
        )
        .expect("contract should build");

        assert!(!contract.use_watermarks);
    }

    #[test]
    fn preflight_schema_from_columns_builds_arrow_schema() {
        let hint = SchemaHint::Columns(vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: ArrowDataType::Utf8,
                nullable: true,
            },
        ]);

        let schema = preflight_schema_from_hint(&hint).expect("schema should be built");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }
}
