//! SQL and cursor parameter helpers for source incremental reads.

use std::fmt::Write as _;

use chrono::{DateTime, SecondsFormat, Utc};
use pg_escape::quote_identifier;
use rapidbyte_sdk::cursor::CursorType;
use rapidbyte_sdk::prelude::*;
use tokio_postgres::types::ToSql;

use crate::types::Column;

#[derive(Debug)]
pub(crate) enum CursorBindParam {
    Int64(i64),
    Text(String),
    Json(serde_json::Value),
}

impl CursorBindParam {
    pub(crate) fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int64(v) => v,
            Self::Text(v) => v,
            Self::Json(v) => v,
        }
    }
}

pub(crate) struct CursorQuery {
    pub(crate) sql: String,
    pub(crate) binds: Vec<CursorBindParam>,
}

#[derive(Debug, Clone)]
pub(crate) struct PartitionKey {
    pub(crate) name: String,
}

pub(crate) fn effective_cursor_type(
    cursor_type: CursorType,
    arrow_type: ArrowDataType,
) -> CursorType {
    match (cursor_type, arrow_type) {
        // Host state currently stores incremental cursor values as UTF-8.
        // If the actual cursor column is numeric, bind as Int64 for a valid
        // typed predicate instead of comparing against text.
        (CursorType::Utf8, ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64) => {
            CursorType::Int64
        }
        // Timestamp columns need ::timestamptz cast, not ::text.
        (CursorType::Utf8, ArrowDataType::TimestampMicros) => CursorType::TimestampMicros,
        _ => cursor_type,
    }
}

fn sql_cast_for_cursor_type(cursor_type: CursorType, column: Option<&Column>) -> &'static str {
    match cursor_type {
        CursorType::TimestampMillis | CursorType::TimestampMicros => column
            .map_or("text::timestamp", |column| match column.pg_type.as_str() {
                "timestamp with time zone" | "timestamptz" => "text::timestamptz",
                _ => "text::timestamp",
            }),
        CursorType::Int64 => "bigint",
        CursorType::Utf8 => "text",
        CursorType::Decimal => "numeric",
        CursorType::Json => "jsonb",
        CursorType::Lsn => "pg_lsn",
        _ => "text",
    }
}

fn inferred_cursor_type(arrow_type: ArrowDataType) -> CursorType {
    match arrow_type {
        ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64 => CursorType::Int64,
        ArrowDataType::TimestampMicros => CursorType::TimestampMicros,
        _ => CursorType::Utf8,
    }
}

pub(crate) fn quote_table_name(table: &str) -> String {
    if table.contains('.') {
        table
            .split('.')
            .filter(|part| !part.is_empty())
            .map(|part| quote_identifier(part).into_owned())
            .collect::<Vec<_>>()
            .join(".")
    } else {
        quote_identifier(table).into_owned()
    }
}

pub(crate) fn build_base_query(
    ctx: &Context,
    stream: &StreamContext,
    columns: &[Column],
    partition_range_bounds: Option<(i64, i64)>,
    partition_key: Option<&PartitionKey>,
) -> Result<CursorQuery, String> {
    let source_table_name = stream.source_stream_or_stream_name();
    let col_list = columns
        .iter()
        .map(|c| {
            let ident = quote_identifier(&c.name);
            if c.needs_cast {
                format!("{ident}::text AS {ident}")
            } else {
                ident.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    if let (SyncMode::Incremental, Some(ci)) = (&stream.sync_mode, &stream.cursor_info) {
        let table_name = quote_table_name(source_table_name);
        let cursor_field = quote_identifier(&ci.cursor_field);
        let cursor_column = columns.iter().find(|c| c.name == ci.cursor_field);
        let cursor_arrow_type = cursor_column.map_or(&ArrowDataType::Utf8, |c| &c.arrow_type);
        let tie_breaker = ci.tie_breaker_field.as_ref().map(|field| {
            let column = columns.iter().find(|c| c.name == *field);
            let arrow_type = column.map_or(ArrowDataType::Utf8, |c| c.arrow_type);
            let cursor_type = inferred_cursor_type(arrow_type);
            (
                field.clone(),
                quote_identifier(field).into_owned(),
                cursor_type,
                sql_cast_for_cursor_type(cursor_type, column),
            )
        });

        if let Some(last_value) = ci.last_value.as_ref() {
            if matches!(last_value, CursorValue::Null) {
                let order_clause = build_incremental_order_clause(&cursor_field, tie_breaker.as_ref());
                ctx.log(
                    LogLevel::Info,
                    &format!(
                        "Incremental read (null prior cursor): {} ORDER BY {}",
                        stream.stream_name, order_clause
                    ),
                );
                let mut sql = format!("SELECT {col_list} FROM {table_name} ORDER BY {order_clause}");
                if let Some(max) = stream.limits.max_records {
                    let _ = write!(sql, " LIMIT {max}");
                }
                return Ok(CursorQuery { sql, binds: vec![] });
            }

            let resolved_cursor_type = effective_cursor_type(ci.cursor_type, *cursor_arrow_type);
            if resolved_cursor_type != ci.cursor_type {
                ctx.log(
                    LogLevel::Debug,
                    &format!(
                        "Incremental cursor type adjusted: stream={} field={} declared={:?} inferred={:?} effective={:?}",
                        stream.stream_name,
                        ci.cursor_field,
                        ci.cursor_type,
                        cursor_arrow_type,
                        resolved_cursor_type
                    ),
                );
            }

            let (cursor_resume_value, tie_breaker_resume_value) =
                decode_resume_values(last_value).map_err(|e| {
                    format!(
                        "Invalid incremental cursor value for stream '{}' field '{}': {e}",
                        stream.stream_name, ci.cursor_field
                    )
                })?;
            let (bind, _) =
                cursor_bind_param(resolved_cursor_type, &cursor_resume_value).map_err(|e| {
                    format!(
                        "Invalid incremental cursor value for stream '{}' field '{}': {e}",
                        stream.stream_name, ci.cursor_field
                    )
                })?;
            let cast = sql_cast_for_cursor_type(resolved_cursor_type, cursor_column);

            let order_clause = build_incremental_order_clause(&cursor_field, tie_breaker.as_ref());
            let mut sql = format!("SELECT {col_list} FROM {table_name}");
            let mut binds = vec![bind];

            if let Some((
                tie_breaker_field,
                tie_breaker_ident,
                tie_breaker_type,
                tie_breaker_cast,
            )) = tie_breaker.as_ref()
            {
                if let Some(tie_breaker_resume_value) = tie_breaker_resume_value.as_ref() {
                    let (tie_bind, _) =
                        cursor_bind_param(*tie_breaker_type, tie_breaker_resume_value).map_err(
                            |e| {
                                format!(
                                    "Invalid incremental tie-breaker value for stream '{}' field '{}': {e}",
                                    stream.stream_name, tie_breaker_field
                                )
                            },
                        )?;
                    ctx.log(
                        LogLevel::Info,
                        &format!(
                            "Incremental read: {} WHERE ({} > $1::{} OR ({} = $1::{} AND {} > $2::{}))",
                            stream.stream_name,
                            cursor_field,
                            cast,
                            cursor_field,
                            cast,
                            tie_breaker_ident,
                            tie_breaker_cast
                        ),
                    );
                    let _ = write!(
                        sql,
                        " WHERE ({cursor_field} > $1::{cast} OR ({cursor_field} = $1::{cast} AND {tie_breaker_ident} > $2::{tie_breaker_cast})) ORDER BY {order_clause}"
                    );
                    binds.push(tie_bind);
                } else {
                    ctx.log(
                        LogLevel::Warn,
                        &format!(
                            "Incremental read for stream '{}' has tie_breaker_field '{}' configured but no persisted tie-breaker value yet; falling back to cursor-only predicate for this resume",
                            stream.stream_name, tie_breaker_field
                        ),
                    );
                    let _ = write!(
                        sql,
                        " WHERE {cursor_field} > $1::{cast} ORDER BY {order_clause}"
                    );
                }
            } else {
                ctx.log(
                    LogLevel::Info,
                    &format!(
                        "Incremental read: {} WHERE {} > $1::{}",
                        stream.stream_name, cursor_field, cast
                    ),
                );
                let _ = write!(
                    sql,
                    " WHERE {cursor_field} > $1::{cast} ORDER BY {order_clause}"
                );
            }
            if let Some(max) = stream.limits.max_records {
                let _ = write!(sql, " LIMIT {max}");
            }
            return Ok(CursorQuery { sql, binds });
        }

        let order_clause = build_incremental_order_clause(&cursor_field, tie_breaker.as_ref());
        ctx.log(
            LogLevel::Info,
            &format!(
                "Incremental read (no prior cursor): {} ORDER BY {}",
                stream.stream_name, order_clause
            ),
        );
        let mut sql = format!("SELECT {col_list} FROM {table_name} ORDER BY {order_clause}");
        if let Some(max) = stream.limits.max_records {
            let _ = write!(sql, " LIMIT {max}");
        }
        return Ok(CursorQuery { sql, binds: vec![] });
    }

    let table_name = quote_table_name(source_table_name);
    let mut sql = format!("SELECT {col_list} FROM {table_name}");

    if let Some((partition_count, partition_index)) = stream.partition_coordinates() {
        if let Some(partition_key) = partition_key {
            let quoted_partition_key = quote_identifier(&partition_key.name);
            if let Some((start, end)) = partition_range_bounds {
                let _ = write!(
                    sql,
                    " WHERE {quoted_partition_key} >= {start} AND {quoted_partition_key} <= {end}"
                );
            } else {
                let _ = write!(
                    sql,
                    " WHERE mod({quoted_partition_key}, {partition_count}) = {partition_index}"
                );
            }
        }
    } else if let (Some(partition_count), Some(partition_index)) =
        (stream.partition_count, stream.partition_index)
    {
        if partition_count == 0 || partition_index >= partition_count {
            ctx.log(
                LogLevel::Warn,
                &format!(
                    "Partitioning disabled for stream '{}': invalid shard metadata index={} count={}",
                    stream.stream_name, partition_index, partition_count
                ),
            );
        }
    }

    if let Some(max) = stream.limits.max_records {
        let _ = write!(sql, " LIMIT {max}");
    }
    Ok(CursorQuery { sql, binds: vec![] })
}

fn build_incremental_order_clause(
    cursor_field: &str,
    tie_breaker: Option<&(String, String, CursorType, &'static str)>,
) -> String {
    match tie_breaker {
        Some((_, tie_breaker_ident, _, _)) => format!("{cursor_field}, {tie_breaker_ident}"),
        None => cursor_field.to_string(),
    }
}

fn decode_resume_values(value: &CursorValue) -> Result<(CursorValue, Option<CursorValue>), String> {
    match value {
        CursorValue::Json { value } => {
            let cursor_value = value
                .get("cursor")
                .cloned()
                .map(serde_json::from_value::<CursorValue>)
                .transpose()
                .map_err(|e| format!("failed to parse composite cursor value: {e}"))?
                .unwrap_or(CursorValue::Null);
            let tie_breaker_value = value
                .get("tie_breaker")
                .cloned()
                .map(serde_json::from_value::<CursorValue>)
                .transpose()
                .map_err(|e| format!("failed to parse composite tie-breaker value: {e}"))?;
            Ok((cursor_value, tie_breaker_value))
        }
        other => Ok((other.clone(), None)),
    }
}

pub(crate) fn cursor_bind_param(
    cursor_type: CursorType,
    value: &CursorValue,
) -> Result<(CursorBindParam, &'static str), String> {
    match cursor_type {
        CursorType::Int64 => {
            let n = match value {
                CursorValue::Int64 { value: v } => *v,
                CursorValue::Utf8 { value: v } => v
                    .parse::<i64>()
                    .map_err(|e| format!("failed to parse '{v}' as i64: {e}"))?,
                CursorValue::Decimal { value, .. } => value
                    .parse::<i64>()
                    .map_err(|e| format!("failed to parse decimal '{value}' as i64: {e}"))?,
                _ => return Err("cursor value is incompatible with int64 cursor type".to_string()),
            };
            Ok((CursorBindParam::Int64(n), "bigint"))
        }
        CursorType::Utf8 => {
            let text = match value {
                CursorValue::Int64 { value: v } => v.to_string(),
                CursorValue::TimestampMillis { value: v } => timestamp_millis_to_rfc3339(*v)?,
                CursorValue::TimestampMicros { value: v } => timestamp_micros_to_rfc3339(*v)?,
                CursorValue::Decimal { value, .. } => value.clone(),
                CursorValue::Json { value: v } => v.to_string(),
                CursorValue::Utf8 { value: v } | CursorValue::Lsn { value: v } => v.clone(),
                CursorValue::Null => {
                    return Err("null cursor cannot be used as a predicate".to_string())
                }
                _ => return Err("cursor value is incompatible with utf8 cursor type".to_string()),
            };
            Ok((CursorBindParam::Text(text), "text"))
        }
        CursorType::TimestampMillis => {
            let ts = match value {
                CursorValue::TimestampMillis { value: v } | CursorValue::Int64 { value: v } => {
                    timestamp_millis_to_rfc3339(*v)?
                }
                CursorValue::Utf8 { value: v } => v.clone(),
                CursorValue::TimestampMicros { value: v } => timestamp_micros_to_rfc3339(*v)?,
                _ => {
                    return Err(
                        "cursor value is incompatible with timestamp_millis cursor type"
                            .to_string(),
                    )
                }
            };
            // Double-cast: bind as text (tokio-postgres supports String->text),
            // then PG casts text->timestamp for the comparison.
            Ok((CursorBindParam::Text(ts), "text::timestamp"))
        }
        CursorType::TimestampMicros => {
            let ts = match value {
                CursorValue::TimestampMicros { value: v } | CursorValue::Int64 { value: v } => {
                    timestamp_micros_to_rfc3339(*v)?
                }
                CursorValue::Utf8 { value: v } => v.clone(),
                CursorValue::TimestampMillis { value: v } => timestamp_millis_to_rfc3339(*v)?,
                _ => {
                    return Err(
                        "cursor value is incompatible with timestamp_micros cursor type"
                            .to_string(),
                    )
                }
            };
            // Double-cast: bind as text (tokio-postgres supports String->text),
            // then PG casts text->timestamp for the comparison.
            Ok((CursorBindParam::Text(ts), "text::timestamp"))
        }
        CursorType::Decimal => {
            let decimal = match value {
                CursorValue::Decimal { value, .. } => value.clone(),
                CursorValue::Utf8 { value: v } => v.clone(),
                CursorValue::Int64 { value: v } => v.to_string(),
                _ => {
                    return Err("cursor value is incompatible with decimal cursor type".to_string())
                }
            };
            Ok((CursorBindParam::Text(decimal), "numeric"))
        }
        CursorType::Json => {
            let json = match value {
                CursorValue::Json { value: v } => v.clone(),
                CursorValue::Utf8 { value: v } => serde_json::from_str::<serde_json::Value>(v)
                    .map_err(|e| format!("failed to parse '{v}' as json: {e}"))?,
                CursorValue::Null => serde_json::Value::Null,
                _ => return Err("cursor value is incompatible with json cursor type".to_string()),
            };
            Ok((CursorBindParam::Json(json), "jsonb"))
        }
        CursorType::Lsn => {
            // LSN cursors are used in CDC mode and are not applicable to
            // incremental queries. Treat as text if encountered here.
            let text = match value {
                CursorValue::Lsn { value: v } | CursorValue::Utf8 { value: v } => v.clone(),
                _ => return Err("cursor value is incompatible with lsn cursor type".to_string()),
            };
            Ok((CursorBindParam::Text(text), "pg_lsn"))
        }
        _ => Err(format!("unsupported cursor type: {cursor_type:?}")),
    }
}

pub(crate) fn timestamp_millis_to_rfc3339(ms: i64) -> Result<String, String> {
    let dt: DateTime<Utc> = DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| format!("invalid timestamp millis value: {ms}"))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

pub(crate) fn timestamp_micros_to_rfc3339(us: i64) -> Result<String, String> {
    let secs = us.div_euclid(1_000_000);
    // Safety: rem_euclid(1_000_000) is always in 0..999_999 which fits in u32.
    #[allow(clippy::cast_possible_truncation)]
    let micros = us.rem_euclid(1_000_000) as u32;
    let nanos = micros * 1_000;
    let dt: DateTime<Utc> = DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| format!("invalid timestamp micros value: {us}"))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Micros, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::catalog::SchemaHint;
    use rapidbyte_sdk::cursor::CursorInfo;
    use rapidbyte_sdk::stream::{StreamContext, StreamLimits, StreamPolicies};
    use rapidbyte_sdk::wire::SyncMode;

    fn columns_for_cursor() -> Vec<Column> {
        vec![
            Column::new("id", "bigint", false),
            Column::new("name", "text", true),
        ]
    }

    fn base_context() -> StreamContext {
        StreamContext {
            stream_name: "users".to_string(),
            source_stream_name: None,
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
            partition_key: None,
            partition_count: None,
            partition_index: None,
            effective_parallelism: None,
            partition_strategy: None,
            copy_flush_bytes_override: None,
        }
    }

    #[test]
    fn effective_cursor_type_promotes_numeric_utf8() {
        assert_eq!(
            effective_cursor_type(CursorType::Utf8, ArrowDataType::Int64),
            CursorType::Int64
        );
        assert_eq!(
            effective_cursor_type(CursorType::Utf8, ArrowDataType::Utf8),
            CursorType::Utf8
        );
    }

    #[test]
    fn effective_cursor_type_promotes_timestamp_utf8() {
        assert_eq!(
            effective_cursor_type(CursorType::Utf8, ArrowDataType::TimestampMicros),
            CursorType::TimestampMicros
        );
    }

    #[test]
    fn sql_cast_for_cursor_type_uses_pg_timestamp_flavor() {
        let timestamp = Column::new("updated_at", "timestamp", false);
        let timestamptz = Column::new("updated_at", "timestamptz", false);

        assert_eq!(
            sql_cast_for_cursor_type(CursorType::TimestampMicros, Some(&timestamp)),
            "text::timestamp"
        );
        assert_eq!(
            sql_cast_for_cursor_type(CursorType::TimestampMicros, Some(&timestamptz)),
            "text::timestamptz"
        );
    }

    #[test]
    fn cursor_bind_param_parses_int64_from_utf8() {
        let (bind, cast) =
            cursor_bind_param(CursorType::Int64, &CursorValue::Utf8 { value: "42".into() })
                .expect("bind should parse");
        match bind {
            CursorBindParam::Int64(v) => assert_eq!(v, 42),
            _ => panic!("expected int64 bind"),
        }
        assert_eq!(cast, "bigint");
    }

    #[test]
    fn cursor_bind_param_rejects_bad_int64() {
        let err = cursor_bind_param(
            CursorType::Int64,
            &CursorValue::Utf8 {
                value: "not_an_int".into(),
            },
        )
        .expect_err("invalid int64 should fail");
        assert!(err.contains("failed to parse 'not_an_int' as i64"));
    }

    #[test]
    fn build_base_query_full_refresh_quotes_identifiers() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.stream_name = "User".to_string();
        let columns = vec![Column::new("select", "text", true)];
        let query =
            build_base_query(&ctx, &stream, &columns, None, None).expect("query should build");
        assert_eq!(query.sql, "SELECT \"select\" FROM \"User\"");
        assert!(query.binds.is_empty());
    }

    #[test]
    fn build_base_query_incremental_with_bind() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.sync_mode = SyncMode::Incremental;
        stream.cursor_info = Some(CursorInfo {
            cursor_field: "id".to_string(),
            tie_breaker_field: None,
            cursor_type: CursorType::Int64,
            last_value: Some(CursorValue::Int64 { value: 7 }),
        });

        let query = build_base_query(&ctx, &stream, &columns_for_cursor(), None, None)
            .expect("query should build");
        assert_eq!(
            query.sql,
            "SELECT id, name FROM users WHERE id > $1::bigint ORDER BY id"
        );
        assert_eq!(query.binds.len(), 1);
    }

    #[test]
    fn build_base_query_full_refresh_with_max_records() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.limits.max_records = Some(100);
        let columns = vec![Column::new("id", "bigint", false)];
        let query =
            build_base_query(&ctx, &stream, &columns, None, None).expect("query should build");
        assert!(
            query.sql.ends_with(" LIMIT 100"),
            "expected LIMIT clause in: {}",
            query.sql
        );
    }

    #[test]
    fn build_base_query_incremental_with_max_records() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.sync_mode = SyncMode::Incremental;
        stream.cursor_info = Some(CursorInfo {
            cursor_field: "id".to_string(),
            tie_breaker_field: None,
            cursor_type: CursorType::Int64,
            last_value: Some(CursorValue::Int64 { value: 7 }),
        });
        stream.limits.max_records = Some(50);
        let query = build_base_query(&ctx, &stream, &columns_for_cursor(), None, None)
            .expect("query should build");
        assert!(
            query.sql.ends_with(" LIMIT 50"),
            "expected LIMIT clause in: {}",
            query.sql
        );
        assert_eq!(query.binds.len(), 1);
    }

    #[test]
    fn build_base_query_incremental_no_prior_cursor_with_max_records() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.sync_mode = SyncMode::Incremental;
        stream.cursor_info = Some(CursorInfo {
            cursor_field: "id".to_string(),
            tie_breaker_field: None,
            cursor_type: CursorType::Int64,
            last_value: None,
        });
        stream.limits.max_records = Some(25);
        let query = build_base_query(&ctx, &stream, &columns_for_cursor(), None, None)
            .expect("query should build");
        assert!(
            query.sql.ends_with(" LIMIT 25"),
            "expected LIMIT clause in: {}",
            query.sql
        );
        assert!(query.binds.is_empty());
    }

    #[test]
    fn build_base_query_no_limit_when_max_records_none() {
        let ctx = Context::new("source-postgres", "");
        let stream = base_context();
        let columns = vec![Column::new("id", "bigint", false)];
        let query =
            build_base_query(&ctx, &stream, &columns, None, None).expect("query should build");
        assert!(
            !query.sql.contains("LIMIT"),
            "expected no LIMIT clause in: {}",
            query.sql
        );
    }

    #[test]
    fn build_base_query_applies_text_cast_for_uuid() {
        let ctx = Context::new("source-postgres", "");
        let stream = base_context();
        let columns = vec![
            Column::new("id", "bigint", false),
            Column::new("external_id", "uuid", true),
        ];
        let query =
            build_base_query(&ctx, &stream, &columns, None, None).expect("query should build");
        // uuid needs ::text cast, bigint does not
        assert!(
            query.sql.contains("external_id::text AS external_id"),
            "expected uuid text cast in: {}",
            query.sql
        );
        // "id" column (bigint) should appear as just "id", not cast
        assert!(
            query.sql.starts_with("SELECT id, "),
            "expected id without text cast in: {}",
            query.sql
        );
    }

    #[test]
    fn build_base_query_full_refresh_partitioned_by_id() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.stream_name = "users#p1".to_string();
        stream.source_stream_name = Some("users".to_string());
        stream.partition_count = Some(4);
        stream.partition_index = Some(1);

        let query = build_base_query(
            &ctx,
            &stream,
            &columns_for_cursor(),
            None,
            Some(&PartitionKey {
                name: "id".to_string(),
            }),
        )
        .expect("query should build");
        assert_eq!(query.sql, "SELECT id, name FROM users WHERE mod(id, 4) = 1");
        assert!(query.binds.is_empty());
    }

    #[test]
    fn build_base_query_full_refresh_partition_fallback_without_id() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.partition_count = Some(4);
        stream.partition_index = Some(1);

        let query = build_base_query(
            &ctx,
            &stream,
            &[Column::new("name", "text", true)],
            None,
            None,
        )
        .expect("query should build");
        assert_eq!(query.sql, "SELECT name FROM users");
        assert!(query.binds.is_empty());
    }

    #[test]
    fn build_base_query_full_refresh_partitioned_by_id_range_bounds() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.partition_count = Some(4);
        stream.partition_index = Some(1);

        let query = build_base_query(
            &ctx,
            &stream,
            &columns_for_cursor(),
            Some((10, 19)),
            Some(&PartitionKey {
                name: "id".to_string(),
            }),
        )
        .expect("query should build");
        assert_eq!(
            query.sql,
            "SELECT id, name FROM users WHERE id >= 10 AND id <= 19"
        );
        assert!(query.binds.is_empty());
    }

    #[test]
    fn build_base_query_full_refresh_with_schema_qualified_source_table() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.source_stream_name = Some("public.users".to_string());

        let query = build_base_query(&ctx, &stream, &columns_for_cursor(), None, None)
            .expect("query should build");
        assert_eq!(query.sql, "SELECT id, name FROM public.users");
    }

    #[test]
    fn build_base_query_incremental_with_schema_qualified_source_table() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.source_stream_name = Some("public.users".to_string());
        stream.sync_mode = SyncMode::Incremental;
        stream.cursor_info = Some(CursorInfo {
            cursor_field: "id".to_string(),
            tie_breaker_field: None,
            cursor_type: CursorType::Int64,
            last_value: Some(CursorValue::Int64 { value: 7 }),
        });

        let query = build_base_query(&ctx, &stream, &columns_for_cursor(), None, None)
            .expect("query should build");
        assert_eq!(
            query.sql,
            "SELECT id, name FROM public.users WHERE id > $1::bigint ORDER BY id"
        );
        assert_eq!(query.binds.len(), 1);
    }

    #[test]
    fn build_base_query_incremental_with_tie_breaker_resume() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.sync_mode = SyncMode::Incremental;
        stream.cursor_info = Some(CursorInfo {
            cursor_field: "updated_at".to_string(),
            tie_breaker_field: Some("id".to_string()),
            cursor_type: CursorType::Utf8,
            last_value: Some(CursorValue::Json {
                value: serde_json::json!({
                    "cursor": { "type": "utf8", "value": "2024-01-01 00:00:00.000000" },
                    "tie_breaker": { "type": "int64", "value": 7 }
                }),
            }),
        });
        let columns = vec![
            Column::new("updated_at", "timestamp", false),
            Column::new("id", "bigint", false),
        ];

        let query =
            build_base_query(&ctx, &stream, &columns, None, None).expect("query should build");
        assert_eq!(
            query.sql,
            "SELECT updated_at, id FROM users WHERE (updated_at > $1::text::timestamp OR (updated_at = $1::text::timestamp AND id > $2::bigint)) ORDER BY updated_at, id"
        );
        assert_eq!(query.binds.len(), 2);
    }

    #[test]
    fn build_base_query_incremental_uses_timestamptz_cast_for_timestamptz_columns() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.sync_mode = SyncMode::Incremental;
        stream.cursor_info = Some(CursorInfo {
            cursor_field: "updated_at".to_string(),
            tie_breaker_field: None,
            cursor_type: CursorType::Utf8,
            last_value: Some(CursorValue::Utf8 {
                value: "2024-01-01T00:00:00Z".to_string(),
            }),
        });
        let columns = vec![
            Column::new("updated_at", "timestamptz", false),
            Column::new("id", "bigint", false),
        ];

        let query =
            build_base_query(&ctx, &stream, &columns, None, None).expect("query should build");
        assert_eq!(
            query.sql,
            "SELECT updated_at, id FROM users WHERE updated_at > $1::text::timestamptz ORDER BY updated_at"
        );
        assert_eq!(query.binds.len(), 1);
    }

    #[test]
    fn build_base_query_full_refresh_partitioned_by_configured_key() {
        let ctx = Context::new("source-postgres", "");
        let mut stream = base_context();
        stream.partition_key = Some("tenant_id".to_string());
        stream.partition_count = Some(4);
        stream.partition_index = Some(1);
        let columns = vec![
            Column::new("tenant_id", "bigint", false),
            Column::new("name", "text", true),
        ];

        let query = build_base_query(
            &ctx,
            &stream,
            &columns,
            None,
            Some(&PartitionKey {
                name: "tenant_id".to_string(),
            }),
        )
        .expect("query should build");
        assert_eq!(
            query.sql,
            "SELECT tenant_id, name FROM users WHERE mod(tenant_id, 4) = 1"
        );
    }
}
