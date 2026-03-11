//! SQL transform plugin powered by Apache DataFusion.

mod config;
mod transform;

use datafusion::sql::parser::{DFParser, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{Query, SetExpr, Statement, TableFactor, TableWithJoins};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use rapidbyte_sdk::prelude::*;

fn query_references_table_name(query: &str, table_name: &str) -> Result<bool, String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, query)
        .map_err(|e| format!("failed to parse SQL query: {e}"))?;
    Ok(statements
        .into_iter()
        .any(|stmt| statement_has_table_name(&stmt, table_name)))
}

fn statement_has_table_name(statement: &Statement, table_name: &str) -> bool {
    match statement {
        Statement::Query(query) => query_has_table_name(query, table_name),
        _ => false,
    }
}

fn query_has_table_name(query: &Query, table_name: &str) -> bool {
    query.with.as_ref().is_some_and(|with| {
        with.cte_tables
            .iter()
            .any(|cte| query_has_table_name(&cte.query, table_name))
    }) || set_expr_has_table_name(&query.body, table_name)
}

fn set_expr_has_table_name(set_expr: &SetExpr, table_name: &str) -> bool {
    match set_expr {
        SetExpr::Select(select) => select
            .from
            .iter()
            .any(|table_with_joins| table_with_joins_has_table_name(table_with_joins, table_name)),
        SetExpr::Query(query) => query_has_table_name(query, table_name),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_has_table_name(left, table_name) || set_expr_has_table_name(right, table_name)
        }
        _ => false,
    }
}

fn table_with_joins_has_table_name(table_with_joins: &TableWithJoins, table_name: &str) -> bool {
    table_factor_has_table_name(&table_with_joins.relation, table_name)
        || table_with_joins
            .joins
            .iter()
            .any(|join| table_factor_has_table_name(&join.relation, table_name))
}

fn table_factor_has_table_name(table_factor: &TableFactor, table_name: &str) -> bool {
    match table_factor {
        TableFactor::Table { name, .. } => name
            .0
            .last()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case(table_name)),
        TableFactor::Derived { subquery, .. } => query_has_table_name(subquery, table_name),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_with_joins_has_table_name(table_with_joins, table_name),
        _ => false,
    }
}

fn normalize_and_parse_query(config: &config::Config) -> Result<String, String> {
    let query = config.normalized_query()?;
    let _ = parse_plannable_statement(&query)?;
    Ok(query)
}

pub(crate) fn validate_query_for_stream_name(query: &str, stream_name: &str) -> Result<(), String> {
    if !query_references_table_name(query, stream_name)? {
        return Err(format!(
            "SQL query must reference current stream table '{stream_name}'"
        ));
    }
    Ok(())
}

fn parse_plannable_statement(query: &str) -> Result<DfStatement, String> {
    let mut statements =
        DFParser::parse_sql(query).map_err(|e| format!("failed to parse SQL query: {e}"))?;
    let statement = statements
        .pop_front()
        .ok_or_else(|| "SQL query must contain exactly one statement".to_string())?;
    if !statements.is_empty() {
        return Err("SQL query must contain exactly one statement".to_string());
    }
    Ok(statement)
}

#[rapidbyte_sdk::plugin(transform)]
pub struct TransformSql {
    config: config::Config,
    statement: DfStatement,
}

impl Transform for TransformSql {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
        let query = normalize_and_parse_query(&config)
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        let statement = parse_plannable_statement(&query)
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        Ok((
            Self {
                config: config::Config { query },
                statement,
            },
            PluginInfo {
                protocol_version: ProtocolVersion::V5,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        match normalize_and_parse_query(config)
            .and_then(|query| validate_query_for_stream_name(&query, ctx.stream_name()))
        {
            Ok(()) => Ok(ValidationResult {
                status: ValidationStatus::Success,
                message: "SQL query configuration is valid".to_string(),
                warnings: Vec::new(),
            }),
            Err(message) => Ok(ValidationResult {
                status: ValidationStatus::Failed,
                message,
                warnings: Vec::new(),
            }),
        }
    }

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError> {
        validate_query_for_stream_name(&self.config.query, ctx.stream_name())
            .map_err(|message| PluginError::config("SQL_CONFIG", message))?;
        transform::run(ctx, &stream, &self.config, &self.statement).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn init_rejects_empty_query() {
        let result = TransformSql::init(config::Config {
            query: "   ".to_string(),
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn init_trims_query_before_storing() {
        let (plugin, _info) = TransformSql::init(config::Config {
            query: "  SELECT * FROM users  ".to_string(),
        })
        .await
        .expect("init should succeed");

        assert_eq!(plugin.config.query, "SELECT * FROM users");
    }

    #[tokio::test]
    async fn init_accepts_query_without_stream_specific_validation() {
        let result = TransformSql::init(config::Config {
            query: "SELECT 1".to_string(),
        })
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn validate_fails_when_query_does_not_reference_current_stream_name() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT 1".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
    }

    #[tokio::test]
    async fn validate_succeeds_when_query_references_current_stream_name() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT id FROM users".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Success);
    }

    #[tokio::test]
    async fn validate_fails_for_legacy_input_table_name() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT id FROM input".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("users"));
    }

    #[tokio::test]
    async fn init_rejects_invalid_sql_with_parse_context() {
        let result = TransformSql::init(config::Config {
            query: "SELECT FROM users".to_string(),
        })
        .await;

        match result {
            Ok(_) => panic!("invalid SQL should fail init"),
            Err(err) => {
                assert_eq!(err.code, "SQL_CONFIG");
                assert!(err.message.contains("failed to parse SQL query"));
            }
        }
    }

    #[tokio::test]
    async fn init_rejects_multiple_statements() {
        let result = TransformSql::init(config::Config {
            query: "SELECT * FROM users; SELECT * FROM users".to_string(),
        })
        .await;

        match result {
            Ok(_) => panic!("multiple statements should fail init"),
            Err(err) => {
                assert_eq!(err.code, "SQL_CONFIG");
                assert!(err.message.contains("exactly one statement"));
            }
        }
    }

    #[tokio::test]
    async fn validate_reports_parse_error_for_invalid_sql() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT FROM users".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("failed to parse SQL query"));
    }
}
