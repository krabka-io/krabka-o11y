use super::{ DurationExprContext, Expr,
    PromqlError, Result,
    normalize_duration_expressions, parse, parse_experimental_zero_arg_helper,
    strip_extended_selector_modifiers, wrap_extended_selectors};

/// Parses `PromQL` and first folds Prometheus duration expressions to fixed durations.
///
/// The parser crate stores selector ranges, subquery resolutions, and offsets as
/// concrete [`Duration`] values. Prometheus 3.x accepts scalar expressions in
/// those positions, so Krabka normalizes them before it sends the query to the
/// parser.
///
/// # Errors
///
/// Returns [`PromqlError::Parse`] when normalization or the upstream parser
/// rejects the query.
#[tracing::instrument(
    name = "promql.parse",
    level = "debug",
    skip_all,
    fields(query = %query),
    err
)]
pub fn parse_promql_with_duration_context(
    query: &str,
    context: DurationExprContext,
) -> Result<Expr> {
    let (query, selector_modifier) = strip_extended_selector_modifiers(query)?;
    let normalized = normalize_duration_expressions(&query, context)?;
    match parse(&normalized) {
        Ok(expr) => Ok(selector_modifier.map_or(expr.clone(), |modifier| {
            wrap_extended_selectors(expr, modifier)
        })),
        Err(error) => parse_experimental_zero_arg_helper(&query).ok_or(PromqlError::Parse(error)),
    }
}
