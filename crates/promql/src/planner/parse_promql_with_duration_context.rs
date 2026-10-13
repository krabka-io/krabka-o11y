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
    let normalized_strings = super::normalize_utf8_strings::normalize_utf8_strings(query)?;
    let query = normalized_strings.query;
    let byte_strings = normalized_strings.byte_strings;
    let stripped = strip_extended_selector_modifiers(&query)?;
    let query = stripped.query;
    let selector_modifier = stripped.modifier;
    let normalized = normalize_duration_expressions(&query, context)?;
    let normalized_trim = normalize_histogram_trim(&normalized);
    let normalized = normalized_trim.query;
    let mut trim_operators = normalized_trim.operators;
    let normalized_info = normalize_info_selectors(&normalized);
    let normalized = normalized_info.query;
    let info_marker = normalized_info.marker;
    let normalized_start = normalize_start_timestamp(&normalized);
    let normalized = normalized_start.query;
    let mut start_functions = normalized_start.start_functions;
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
