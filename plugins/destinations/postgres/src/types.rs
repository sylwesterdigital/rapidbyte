//! Arrow <-> `PostgreSQL` type mapping and typed-column helpers.

use rapidbyte_sdk::arrow::array::{
    BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, TimestampMicrosecondArray,
};
use rapidbyte_sdk::arrow::datatypes::DataType;

/// Map Arrow data types back to `PostgreSQL` column types.
#[must_use]
pub(crate) fn arrow_to_pg_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Boolean => "BOOLEAN",
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ",
        DataType::Timestamp(_, None) => "TIMESTAMP",
        DataType::Date32 => "DATE",
        DataType::Binary => "BYTEA",
        // Utf8 and all other unsupported types default to TEXT.
        _ => "TEXT",
    }
}

fn normalize_pg_type(t: &str) -> &str {
    match t {
        "int" | "int4" | "integer" | "serial" => "integer",
        "int2" | "smallint" | "smallserial" => "smallint",
        "int8" | "bigint" | "bigserial" => "bigint",
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        "bool" | "boolean" => "boolean",
        "varchar" | "character varying" | "text" | "character" | "char" | "name" | "bpchar" => {
            "text"
        }
        "timestamp without time zone" | "timestamp" => "timestamp",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        "numeric" | "decimal" => "numeric",
        "date" => "date",
        "time without time zone" | "time" => "time",
        "time with time zone" | "timetz" => "timetz",
        "bytea" => "bytea",
        "json" => "json",
        "jsonb" => "jsonb",
        "uuid" => "uuid",
        "interval" => "interval",
        "inet" => "inet",
        "cidr" => "cidr",
        "macaddr" | "macaddr8" => "macaddr",
        other => other,
    }
}

/// Check if an `information_schema` type and DDL type refer to the same type.
#[must_use]
pub(crate) fn pg_types_compatible(info_schema_type: &str, ddl_type: &str) -> bool {
    let a = info_schema_type.to_lowercase();
    let b = ddl_type.to_lowercase();
    normalize_pg_type(&a) == normalize_pg_type(&b)
}

/// Pre-downcast Arrow column reference. Eliminates per-cell `downcast_ref()` calls
/// by resolving the concrete array type once per column per batch.
#[derive(Debug)]
pub(crate) enum TypedCol<'a> {
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Boolean(&'a BooleanArray),
    Utf8(&'a rapidbyte_sdk::arrow::array::StringArray),
    TimestampMicros(&'a TimestampMicrosecondArray),
    Date32(&'a Date32Array),
    Binary(&'a BinaryArray),
    Null,
}

/// Pre-downcast active columns from a `RecordBatch` into `TypedCol` references.
///
/// # Errors
///
/// Returns `Err` if an Arrow column has an unsupported data type for the
/// `PostgreSQL` write path.
pub(crate) fn downcast_columns<'a>(
    batch: &'a RecordBatch,
    active_cols: &[usize],
) -> Result<Vec<TypedCol<'a>>, String> {
    active_cols
        .iter()
        .map(|&i| {
            let col = batch.column(i);
            match col.data_type() {
                DataType::Int16 => Ok(TypedCol::Int16(col.as_any().downcast_ref().ok_or_else(
                    || format!("downcast failed for column {i} (expected Int16Array)"),
                )?)),
                DataType::Int32 => Ok(TypedCol::Int32(col.as_any().downcast_ref().ok_or_else(
                    || format!("downcast failed for column {i} (expected Int32Array)"),
                )?)),
                DataType::Int64 => Ok(TypedCol::Int64(col.as_any().downcast_ref().ok_or_else(
                    || format!("downcast failed for column {i} (expected Int64Array)"),
                )?)),
                DataType::Float32 => Ok(TypedCol::Float32(
                    col.as_any().downcast_ref().ok_or_else(|| {
                        format!("downcast failed for column {i} (expected Float32Array)")
                    })?,
                )),
                DataType::Float64 => Ok(TypedCol::Float64(
                    col.as_any().downcast_ref().ok_or_else(|| {
                        format!("downcast failed for column {i} (expected Float64Array)")
                    })?,
                )),
                DataType::Boolean => Ok(TypedCol::Boolean(
                    col.as_any().downcast_ref().ok_or_else(|| {
                        format!("downcast failed for column {i} (expected BooleanArray)")
                    })?,
                )),
                DataType::Utf8 => Ok(TypedCol::Utf8(col.as_any().downcast_ref().ok_or_else(
                    || format!("downcast failed for column {i} (expected StringArray)"),
                )?)),
                DataType::Timestamp(rapidbyte_sdk::arrow::datatypes::TimeUnit::Microsecond, _) => {
                    Ok(TypedCol::TimestampMicros(
                        col.as_any().downcast_ref().ok_or_else(|| {
                            format!(
                                "downcast failed for column {i} (expected TimestampMicrosecondArray)"
                            )
                        })?,
                    ))
                }
                DataType::Date32 => Ok(TypedCol::Date32(col.as_any().downcast_ref().ok_or_else(
                    || format!("downcast failed for column {i} (expected Date32Array)"),
                )?)),
                DataType::Binary => Ok(TypedCol::Binary(col.as_any().downcast_ref().ok_or_else(
                    || format!("downcast failed for column {i} (expected BinaryArray)"),
                )?)),
                DataType::Null => Ok(TypedCol::Null),
                other => Err(format!(
                    "unsupported Arrow type for column {i}: {other:?}. Cast in a transform before writing to dest-postgres"
                )),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rapidbyte_sdk::arrow::array::{BinaryArray, Date32Array, TimestampMicrosecondArray};
    use rapidbyte_sdk::arrow::datatypes::{Field, Schema, TimeUnit};
    use rapidbyte_sdk::arrow::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn pg_types_compatible_normalizes_aliases() {
        assert!(pg_types_compatible("integer", "INT4"));
        assert!(pg_types_compatible("character varying", "TEXT"));
        assert!(pg_types_compatible(
            "timestamp with time zone",
            "timestamptz"
        ));
        assert!(!pg_types_compatible("bigint", "text"));
    }

    #[test]
    fn arrow_to_pg_type_maps_timestamp_date_binary() {
        assert_eq!(
            arrow_to_pg_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "TIMESTAMP"
        );
        assert_eq!(
            arrow_to_pg_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            "TIMESTAMPTZ"
        );
        assert_eq!(arrow_to_pg_type(&DataType::Date32), "DATE");
        assert_eq!(arrow_to_pg_type(&DataType::Binary), "BYTEA");
    }

    #[test]
    fn pg_types_compatible_comprehensive() {
        assert!(pg_types_compatible("integer", "INT4"));
        assert!(pg_types_compatible("serial", "INTEGER"));
        assert!(pg_types_compatible("bigserial", "BIGINT"));
        assert!(pg_types_compatible("smallserial", "SMALLINT"));
        assert!(pg_types_compatible("character varying", "TEXT"));
        assert!(pg_types_compatible("name", "TEXT"));
        assert!(pg_types_compatible("bpchar", "TEXT"));
        assert!(pg_types_compatible(
            "timestamp with time zone",
            "timestamptz"
        ));
        assert!(pg_types_compatible(
            "timestamp without time zone",
            "TIMESTAMP"
        ));
        assert!(pg_types_compatible("numeric", "DECIMAL"));
        assert!(pg_types_compatible("date", "DATE"));
        assert!(pg_types_compatible("time without time zone", "TIME"));
        assert!(pg_types_compatible("bytea", "BYTEA"));
        assert!(pg_types_compatible("uuid", "UUID"));
        assert!(pg_types_compatible("json", "JSON"));
        assert!(pg_types_compatible("jsonb", "JSONB"));
        assert!(!pg_types_compatible("bigint", "text"));
        assert!(!pg_types_compatible("timestamp", "date"));
        assert!(!pg_types_compatible("json", "jsonb"));
    }

    #[test]
    fn downcast_columns_handles_timestamp_date_binary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("d", DataType::Date32, true),
            Field::new("b", DataType::Binary, true),
        ]));
        let ts_arr = TimestampMicrosecondArray::from(vec![Some(1705312200000000i64)]);
        let d_arr = Date32Array::from(vec![Some(19737)]);
        let b_arr = BinaryArray::from(vec![Some(b"test" as &[u8])]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(ts_arr), Arc::new(d_arr), Arc::new(b_arr)],
        )
        .unwrap();
        let cols = downcast_columns(&batch, &[0, 1, 2]).unwrap();
        assert!(matches!(cols[0], TypedCol::TimestampMicros(_)));
        assert!(matches!(cols[1], TypedCol::Date32(_)));
        assert!(matches!(cols[2], TypedCol::Binary(_)));
    }

    #[test]
    fn downcast_columns_rejects_unsupported_types() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::LargeUtf8, true)]));
        let arr = rapidbyte_sdk::arrow::array::LargeStringArray::from(vec![Some("alice")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let err = downcast_columns(&batch, &[0]).expect_err("LargeUtf8 should be rejected");
        assert!(err.contains("unsupported Arrow type"));
    }
}
