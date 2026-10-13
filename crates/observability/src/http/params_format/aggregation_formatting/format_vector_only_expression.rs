use super::{format_vector_binary_expression, format_vector_function_text};

pub(crate) fn format_vector_only_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_vector_function_text(query) {
        return Some(formatted);
    }

    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    format_vector_binary_expression(&query)
}
