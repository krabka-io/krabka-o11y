use super::*;

pub(crate) fn format_loki_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_vector_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_comparison_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_set_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_comparison_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_label_replace_function(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_scalar_vector_expression(query) {
        return Some(formatted);
    }
    parse_metric_query(query)
        .ok()
        .and_then(|query| format_metric_query(&query))
}
