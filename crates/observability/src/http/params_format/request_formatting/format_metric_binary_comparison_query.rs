use super::*;

pub(crate) fn format_metric_binary_comparison_query(query: &str) -> Option<String> {
    let (left_text, _, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let right_text = right_text
        .strip_prefix("bool")
        .map_or(right_text, str::trim_start);
    let (_, right_text) = split_leading_vector_binary_modifiers(right_text);
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let comparison = parse_metric_binary_comparison_query(query).ok()?;
    let left = format_metric_query(&comparison.left)?;
    let right = format_metric_query(&comparison.right)?;
    let operator = format_metric_scalar_comparison_operator(comparison.op)?;
    Some(format_metric_binary_expression(
        &left,
        operator,
        comparison.bool_modifier,
        comparison.matching.as_ref(),
        &right,
    ))
}
