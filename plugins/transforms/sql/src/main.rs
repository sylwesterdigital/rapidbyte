//! SQL transform plugin powered by Apache DataFusion.

mod config;
mod transform;

use std::collections::{BTreeSet, HashSet};
use std::ops::ControlFlow;

use datafusion::sql::parser::{DFParser, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, GroupByExpr, JoinConstraint, JoinOperator,
    NamedWindowExpr, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Visit, Visitor, WindowType,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use rapidbyte_sdk::prelude::*;

fn query_external_table_references(query: &str) -> Result<BTreeSet<String>, String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, query)
        .map_err(|e| format!("failed to parse SQL query: {e}"))?;
    let mut references = BTreeSet::new();
    let scope = HashSet::new();
    for statement in &statements {
        collect_statement_external_table_references(statement, &scope, &mut references);
    }
    Ok(references)
}

fn collect_statement_external_table_references(
    statement: &Statement,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    if let Statement::Query(query) = statement {
        collect_query_external_table_references(query, scope, references);
    }
}

fn collect_query_external_table_references(
    query: &Query,
    parent_scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    let mut scope = parent_scope.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_external_table_references(&cte.query, &scope, references);
            scope.insert(cte.alias.name.value.to_ascii_lowercase());
        }
    }
    collect_set_expr_external_table_references(&query.body, &scope, references);
    if let Some(order_by) = &query.order_by {
        for expr in &order_by.exprs {
            collect_expr_external_table_references(&expr.expr, &scope, references);
            if let Some(with_fill) = &expr.with_fill {
                if let Some(from) = &with_fill.from {
                    collect_expr_external_table_references(from, &scope, references);
                }
                if let Some(to) = &with_fill.to {
                    collect_expr_external_table_references(to, &scope, references);
                }
                if let Some(step) = &with_fill.step {
                    collect_expr_external_table_references(step, &scope, references);
                }
            }
        }
        if let Some(interpolate) = &order_by.interpolate {
            if let Some(exprs) = &interpolate.exprs {
                for expr in exprs {
                    collect_interpolate_expr_external_table_references(expr, &scope, references);
                }
            }
        }
    }
    if let Some(limit) = &query.limit {
        collect_expr_external_table_references(limit, &scope, references);
    }
    for expr in &query.limit_by {
        collect_expr_external_table_references(expr, &scope, references);
    }
    if let Some(offset) = &query.offset {
        collect_expr_external_table_references(&offset.value, &scope, references);
    }
    if let Some(fetch) = &query.fetch {
        if let Some(quantity) = &fetch.quantity {
            collect_expr_external_table_references(quantity, &scope, references);
        }
    }
}

fn collect_set_expr_external_table_references(
    set_expr: &SetExpr,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    match set_expr {
        SetExpr::Select(select) => collect_select_external_table_references(select, scope, references),
        SetExpr::Query(query) => collect_query_external_table_references(query, scope, references),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_external_table_references(left, scope, references);
            collect_set_expr_external_table_references(right, scope, references);
        }
        SetExpr::Values(values) => {
            for row in &values.rows {
                for expr in row {
                    collect_expr_external_table_references(expr, scope, references);
                }
            }
        }
        SetExpr::Table(table) => {
            if let Some(table_name) = &table.table_name {
                let relation = if let Some(schema_name) = &table.schema_name {
                    format!("{schema_name}.{table_name}")
                } else {
                    table_name.clone()
                };
                collect_relation_name_external_reference(&relation, scope, references);
            }
        }
        SetExpr::Insert(_) | SetExpr::Update(_) => {}
    }
}

fn collect_select_external_table_references(
    select: &Select,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    for item in &select.projection {
        collect_select_item_external_table_references(item, scope, references);
    }
    for table_with_joins in &select.from {
        collect_table_with_joins_external_table_references(table_with_joins, scope, references);
    }
    for lateral_view in &select.lateral_views {
        collect_expr_external_table_references(&lateral_view.lateral_view, scope, references);
    }
    if let Some(prewhere) = &select.prewhere {
        collect_expr_external_table_references(prewhere, scope, references);
    }
    if let Some(selection) = &select.selection {
        collect_expr_external_table_references(selection, scope, references);
    }
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for expr in exprs {
            collect_expr_external_table_references(expr, scope, references);
        }
    }
    for expr in &select.cluster_by {
        collect_expr_external_table_references(expr, scope, references);
    }
    for expr in &select.distribute_by {
        collect_expr_external_table_references(expr, scope, references);
    }
    for expr in &select.sort_by {
        collect_expr_external_table_references(expr, scope, references);
    }
    if let Some(having) = &select.having {
        collect_expr_external_table_references(having, scope, references);
    }
    if let Some(qualify) = &select.qualify {
        collect_expr_external_table_references(qualify, scope, references);
    }
    if let Some(connect_by) = &select.connect_by {
        collect_expr_external_table_references(&connect_by.condition, scope, references);
    }
    for named_window in &select.named_window {
        collect_named_window_external_table_references(&named_window.1, scope, references);
    }
}

fn collect_select_item_external_table_references(
    item: &SelectItem,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            collect_expr_external_table_references(expr, scope, references);
        }
        SelectItem::ExprWithAlias { expr, .. } => {
            collect_expr_external_table_references(expr, scope, references);
        }
        SelectItem::QualifiedWildcard(_, additional_options)
        | SelectItem::Wildcard(additional_options) => {
            if let Some(replace) = &additional_options.opt_replace {
                for item in &replace.items {
                    collect_expr_external_table_references(&item.expr, scope, references);
                }
            }
        }
    }
}

fn collect_table_with_joins_external_table_references(
    table_with_joins: &TableWithJoins,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    collect_table_factor_external_table_references(&table_with_joins.relation, scope, references);
    for join in &table_with_joins.joins {
        collect_table_factor_external_table_references(&join.relation, scope, references);
        collect_join_operator_external_table_references(&join.join_operator, scope, references);
    }
}

fn collect_join_operator_external_table_references(
    join_operator: &JoinOperator,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    match join_operator {
        JoinOperator::Inner(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint) => {
            collect_join_constraint_external_table_references(constraint, scope, references);
        }
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            collect_expr_external_table_references(match_condition, scope, references);
            collect_join_constraint_external_table_references(constraint, scope, references);
        }
        JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {}
    }
}

fn collect_join_constraint_external_table_references(
    constraint: &JoinConstraint,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    if let JoinConstraint::On(expr) = constraint {
        collect_expr_external_table_references(expr, scope, references);
    }
}

fn collect_table_factor_external_table_references(
    table_factor: &TableFactor,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    match table_factor {
        TableFactor::Table {
            name,
            args,
            with_hints,
            ..
        } => {
            let relation = normalize_relation_name(name);
            collect_relation_name_external_reference(&relation, scope, references);
            if let Some(args) = args {
                for arg in &args.args {
                    collect_function_arg_external_table_references(arg, scope, references);
                }
            }
            for expr in with_hints {
                collect_expr_external_table_references(expr, scope, references);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_query_external_table_references(subquery, scope, references);
        }
        TableFactor::TableFunction { expr, .. } => {
            collect_expr_external_table_references(expr, scope, references);
        }
        TableFactor::Function { args, .. } => {
            for arg in args {
                collect_function_arg_external_table_references(arg, scope, references);
            }
        }
        TableFactor::UNNEST { array_exprs, .. } => {
            for expr in array_exprs {
                collect_expr_external_table_references(expr, scope, references);
            }
        }
        TableFactor::JsonTable { json_expr, .. } => {
            collect_expr_external_table_references(json_expr, scope, references);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_table_with_joins_external_table_references(table_with_joins, scope, references),
        TableFactor::Pivot { table, .. } => {
            collect_table_factor_external_table_references(table, scope, references);
        }
        TableFactor::Unpivot { table, .. } => {
            collect_table_factor_external_table_references(table, scope, references);
        }
        TableFactor::MatchRecognize {
            table,
            partition_by,
            order_by,
            measures,
            ..
        } => {
            collect_table_factor_external_table_references(table, scope, references);
            for expr in partition_by {
                collect_expr_external_table_references(expr, scope, references);
            }
            for expr in order_by {
                collect_expr_external_table_references(&expr.expr, scope, references);
            }
            for measure in measures {
                collect_expr_external_table_references(&measure.expr, scope, references);
            }
        }
    }
}

fn collect_function_arg_external_table_references(
    arg: &FunctionArg,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    match arg {
        FunctionArg::Named { arg, .. } | FunctionArg::Unnamed(arg) => {
            collect_function_arg_expr_external_table_references(arg, scope, references);
        }
    }
}

fn collect_function_arg_expr_external_table_references(
    arg: &FunctionArgExpr,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    if let FunctionArgExpr::Expr(expr) = arg {
        collect_expr_external_table_references(expr, scope, references);
    }
}

fn collect_named_window_external_table_references(
    window: &NamedWindowExpr,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    if let NamedWindowExpr::WindowSpec(window) = window {
        collect_named_window_type_external_table_references(
            &WindowType::WindowSpec(window.clone()),
            scope,
            references,
        );
    }
}

fn collect_named_window_type_external_table_references(
    window: &WindowType,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    if let WindowType::WindowSpec(spec) = window {
        for expr in &spec.partition_by {
            collect_expr_external_table_references(expr, scope, references);
        }
        for expr in &spec.order_by {
            collect_expr_external_table_references(&expr.expr, scope, references);
        }
    }
}

fn collect_expr_external_table_references(
    expr: &Expr,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    let mut collector = ExpressionQueryCollector { scope, references };
    let _ = expr.visit(&mut collector);
}

struct ExpressionQueryCollector<'a> {
    scope: &'a HashSet<String>,
    references: &'a mut BTreeSet<String>,
}

impl Visitor for ExpressionQueryCollector<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        collect_query_external_table_references(query, self.scope, self.references);
        ControlFlow::Continue(())
    }
}

fn collect_interpolate_expr_external_table_references(
    expr: &datafusion::sql::sqlparser::ast::InterpolateExpr,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    if let Some(expr) = &expr.expr {
        collect_expr_external_table_references(expr, scope, references);
    }
}

fn collect_relation_name_external_reference(
    relation: &str,
    scope: &HashSet<String>,
    references: &mut BTreeSet<String>,
) {
    if is_cte_reference_name(relation, scope) {
        return;
    }
    references.insert(relation.to_string());
}

fn is_cte_reference_name(relation: &str, scope: &HashSet<String>) -> bool {
    !relation.contains('.') && scope.contains(&relation.to_ascii_lowercase())
}

fn normalize_relation_name(relation: &ObjectName) -> String {
    relation
        .0
        .iter()
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

fn normalize_and_parse_query(config: &config::Config) -> Result<String, String> {
    let query = config.normalized_query()?;
    let _ = parse_plannable_statement(&query)?;
    Ok(query)
}

pub(crate) fn validate_query_for_stream_name(query: &str, stream_name: &str) -> Result<(), String> {
    let references = query_external_table_references(query)?;
    if references.is_empty() || !references.iter().any(|name| name.eq_ignore_ascii_case(stream_name))
    {
        return Err(format!(
            "SQL query must reference current stream table '{stream_name}'"
        ));
    }
    let invalid_references = references
        .into_iter()
        .filter(|name| !name.eq_ignore_ascii_case(stream_name))
        .collect::<Vec<_>>();
    if !invalid_references.is_empty() {
        return Err(format!(
            "SQL query may only reference current stream table '{stream_name}'; found: {}",
            invalid_references.join(", ")
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
    async fn validate_rejects_queries_that_reference_multiple_external_tables() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT users.id FROM users JOIN orders ON users.id = orders.user_id"
                    .to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("may only reference current stream table"));
        assert!(validation.message.contains("orders"));
    }

    #[tokio::test]
    async fn validate_allows_ctes_derived_from_current_stream() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "WITH filtered AS (SELECT id FROM users) SELECT id FROM filtered"
                    .to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Success);
    }

    #[tokio::test]
    async fn validate_rejects_cte_aliases_that_shadow_disallowed_tables() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "WITH orders AS (SELECT * FROM orders) SELECT * FROM users".to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("orders"));
    }

    #[tokio::test]
    async fn validate_allows_quoted_current_stream_identifier() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: r#"SELECT id FROM "users""#.to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Success);
    }

    #[tokio::test]
    async fn validate_rejects_cross_stream_subquery_references() {
        let ctx = Context::new("transform-sql", "users");
        let validation = TransformSql::validate(
            &config::Config {
                query: "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
                    .to_string(),
            },
            &ctx,
        )
        .await
        .expect("validate should not return plugin error");

        assert_eq!(validation.status, ValidationStatus::Failed);
        assert!(validation.message.contains("orders"));
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
