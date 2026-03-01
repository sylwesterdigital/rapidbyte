//! Arrow <-> `PostgreSQL` type mapping helpers.

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

#[cfg(test)]
mod tests {
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
            arrow_to_pg_type(&DataType::Timestamp(
                rapidbyte_sdk::arrow::datatypes::TimeUnit::Microsecond,
                None
            )),
            "TIMESTAMP"
        );
        assert_eq!(
            arrow_to_pg_type(&DataType::Timestamp(
                rapidbyte_sdk::arrow::datatypes::TimeUnit::Microsecond,
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
}
