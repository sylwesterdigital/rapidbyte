//! SQL transform plugin powered by Apache DataFusion.

mod config;
mod transform;

use datafusion::sql::parser::{DFParser, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{Query, SetExpr, Statement, TableFactor, TableWithJoins};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use rapidbyte_sdk::prelude::*;

fn query_references_input_table(query: &str) -> Result<bool, String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, query)
        .map_err(|e| format!("failed to parse SQL query: {e}"))?;
    Ok(statements
        .into_iter()
        .any(|stmt| statement_has_input_table(&stmt)))
}

fn statement_has_input_table(statement: &Statement) -> bool {
    match statement {
        Statement::Query(query) => query_has_input_table(query),
        _ => false,
    }
}

fn query_has_input_table(query: &Query) -> bool {
    query
        .with
        .as_ref()
        .is_some_and(|with| with.cte_tables.iter().any(|cte| query_has_input_table(&cte.query)))
        || set_expr_has_input_table(&query.body)
}

fn set_expr_has_input_table(set_expr: &SetExpr) -> bool {
    match set_expr {
        SetExpr::Select(select) => select.from.iter().any(table_with_joins_has_input),
        SetExpr::Query(query) => query_has_input_table(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_has_input_table(left) || set_expr_has_input_table(right)
        }
        _ => false,
    }
}

fn table_with_joins_has_input(table_with_joins: &TableWithJoins) -> bool {
    table_factor_has_input(&table_with_joins.relation)
        || table_with_joins
            .joins
            .iter()
            .any(|join| table_factor_has_input(&join.relation))
}

fn table_factor_has_input(table_factor: &TableFactor) -> bool {
    match table_factor {
        TableFactor::Table { name, .. } => name
            .0
            .last()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case("input")),
        TableFactor::Derived { subquery, .. } => query_has_input_table(subquery),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_with_joins_has_input(table_with_joins),
        _ => false,
    }
}

fn validate_query_contract(config: &config::Config) -> Result<String, String> {
    let query = config.normalized_query()?;
    if !query_references_input_table(&query)? {
        return Err("SQL query must reference input table 'input'".to_string());
    }
    Ok(query)
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
        let query = validate_query_contract(&config)
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
        let _ = ctx;
        match validate_query_contract(config) {
            Ok(_) => Ok(ValidationResult {
                status: ValidationStatus::Success,
                message: "SQL query configuration is valid".to_string(),
            }),
            Err(message) => Ok(ValidationResult {
                status: ValidationStatus::Failed,
                message,
            }),
        }
    }

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError> {
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
            query: "  SELECT * FROM input  ".to_string(),
        })
        .await
        .expect("init should succeed");

        assert_eq!(plugin.config.query, "SELECT * FROM input");
    }

    #[tokio::test]
    async fn init_rejects_query_without_input_reference() {
        let result = TransformSql::init(config::Config {
            query: "SELECT 1".to_string(),
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn validate_fails_when_query_does_not_reference_input() {
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
    async fn init_rejects_query_when_input_is_not_a_from_table() {
        let result = TransformSql::init(config::Config {
            query: "SELECT input FROM events".to_string(),
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn init_accepts_query_with_input_in_cte() {
        let result = TransformSql::init(config::Config {
            query: "WITH c AS (SELECT * FROM input) SELECT * FROM c".to_string(),
        })
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn init_rejects_invalid_sql_with_parse_context() {
        let result = TransformSql::init(config::Config {
            query: "SELECT FROM input".to_string(),
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
            query: "SELECT * FROM input; SELECT * FROM input".to_string(),
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
                query: "SELECT FROM input".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("failed to parse SQL query"));
    }
}
