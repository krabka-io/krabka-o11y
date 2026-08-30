use super::{
    format_vector_arithmetic_expression, format_vector_comparison_expression,
    format_vector_function_text, format_vector_set_expression,
};

pub(crate) fn format_vector_only_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_vector_function_text(query) {
        return Some(formatted);
    }

    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    if let Some(formatted) = format_vector_set_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_arithmetic_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_comparison_expression(&query) {
        return Some(formatted);
    }
    None
}
