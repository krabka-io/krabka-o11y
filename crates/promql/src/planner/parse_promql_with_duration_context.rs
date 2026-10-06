use super::{
    DurationExprContext, Expr, PromqlError, Result,
    histogram_trim_operators::{normalize_histogram_trim, restore_histogram_trim},
    info_label_selector::{
        normalize_info_selectors, normalize_start_timestamp, restore_info_selectors,
    },
    normalize_duration_expressions, parse, parse_experimental_zero_arg_helper,
    strip_extended_selector_modifiers, wrap_extended_selectors,
};

/// Parses `PromQL` and first folds Prometheus duration expressions to fixed durations.
///
/// The parser crate stores selector ranges, subquery resolutions, and offsets as
/// concrete [`std::time::Duration`] values. Prometheus 3.x accepts scalar expressions in
/// those positions, so Krabka normalizes them before it sends the query to the
/// parser.
///
/// # Errors
///
/// Returns [`crate::PromqlError::Parse`] when normalization or the upstream parser
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
    let (query, byte_strings) = super::normalize_utf8_strings::normalize_utf8_strings(query)?;
    let (query, selector_modifier) = strip_extended_selector_modifiers(&query)?;
    let normalized = normalize_duration_expressions(&query, context)?;
    let (normalized, mut trim_operators) = normalize_histogram_trim(&normalized);
    let (normalized, info_marker) = normalize_info_selectors(&normalized);
    let (normalized, mut start_functions) = normalize_start_timestamp(&normalized);
    if !cfg!(feature = "experimental-functions") && start_functions.iter().any(|start| *start) {
        return Err(PromqlError::Parse(
            "function start_timestamp is not enabled".to_string(),
        ));
    }
    match parse(&normalized) {
        Ok(mut expr) => {
            restore_histogram_trim(&mut expr, &mut trim_operators);
            super::byte_string_expr::restore_byte_strings(&mut expr, &byte_strings);
            restore_info_selectors(&mut expr, &info_marker, &mut start_functions);
            Ok(selector_modifier.map_or(expr.clone(), |modifier| {
                wrap_extended_selectors(expr, modifier)
            }))
        }
        Err(error) => parse_experimental_zero_arg_helper(&query).ok_or(PromqlError::Parse(error)),
    }
}
