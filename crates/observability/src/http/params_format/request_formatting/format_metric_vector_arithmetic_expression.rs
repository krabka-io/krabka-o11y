use super::{
    format_metric_vector_binary_expression, format_simple_metric_query,
    format_vector_function_text, parse_metric_query, split_leading_vector_binary_modifiers,
    split_top_level_arithmetic_query,
};

pub(crate) fn format_metric_vector_arithmetic_expression(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_arithmetic_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let (left, right) = if let (Some(left), Some(right)) = (
        parse_metric_query(left_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
        format_vector_function_text(right_text.trim()),
    ) {
        (left, right)
    } else if let (Some(left), Some(right)) = (
        format_vector_function_text(left_text.trim()),
        parse_metric_query(right_text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query)),
    ) {
        (left, right)
    } else {
        return None;
    };

    Some(format_metric_vector_binary_expression(
        &left, operator, modifiers, &right,
    ))
}
