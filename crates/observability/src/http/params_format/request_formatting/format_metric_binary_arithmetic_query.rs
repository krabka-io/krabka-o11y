use super::{
    format_metric_binary_expression, format_metric_query, format_metric_scalar_arithmetic_operator,
    parse_metric_binary_arithmetic_query, parse_metric_query,
    split_leading_vector_binary_modifiers, split_top_level_arithmetic_query,
};

pub(crate) fn format_metric_binary_arithmetic_query(query: &str) -> Option<String> {
    let (left_text, _, right_text) = split_top_level_arithmetic_query(query)?;
    let (_, right_text) = split_leading_vector_binary_modifiers(right_text);
    parse_metric_query(left_text.trim()).ok()?;
    parse_metric_query(right_text.trim()).ok()?;
    let arithmetic = parse_metric_binary_arithmetic_query(query).ok()?;
    let left = format_metric_query(&arithmetic.left)?;
    let right = format_metric_query(&arithmetic.right)?;
    let operator = format_metric_scalar_arithmetic_operator(arithmetic.op);
    Some(format_metric_binary_expression(
        &left,
        operator,
        false,
        arithmetic.matching.as_ref(),
        &right,
    ))
}
