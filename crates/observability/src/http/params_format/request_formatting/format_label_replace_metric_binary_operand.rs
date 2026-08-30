use super::*;

pub(crate) fn format_label_replace_metric_binary_operand(query: &str) -> Option<(String, bool)> {
    if let Some(formatted) = format_metric_label_replace_query(query) {
        return Some((formatted, true));
    }
    if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
        return Some((formatted, true));
    }
    if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
        return Some((formatted, true));
    }
    if let Some(formatted) = format_vector_function_text(query) {
        return Some((formatted, false));
    }
    parse_metric_query(query)
        .ok()
        .and_then(|query| format_simple_metric_query(&query))
        .map(|formatted| (formatted, false))
}
