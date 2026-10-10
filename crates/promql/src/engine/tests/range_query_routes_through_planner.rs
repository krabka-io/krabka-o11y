use promql_parser::parser::Expr;

use crate::{DurationExprContext, parse_promql_with_duration_context};

/// Whether the range-query gate routes `query`, parsed for the `grid`
/// context with its outer parentheses stripped, through the planner.
pub(crate) fn range_query_routes_through_planner(query: &str, grid: DurationExprContext) -> bool {
    let expr = parse_promql_with_duration_context(query, grid)
        .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
    let mut probe = &expr;
    while let Expr::Paren(paren) = probe {
        probe = &paren.expr;
    }
    super::super::range_expr_routes_through_planner(probe)
}
