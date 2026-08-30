use super::{DurationExprContext, Expr, Result, parse_promql_with_duration_context};

/// Parses a `PromQL` expression into the upstream parser AST.
///
/// # Errors
///
/// Returns [`PromqlError::Parse`] when the upstream parser rejects the query.
pub fn parse_promql(query: &str) -> Result<Expr> {
    parse_promql_with_duration_context(query, DurationExprContext::instant(0))
}
