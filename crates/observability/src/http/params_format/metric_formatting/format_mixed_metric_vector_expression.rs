use super::*;

pub(crate) fn format_mixed_metric_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_vector_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_comparison_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_vector_set_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_sort_vector_expression(query) {
        return Some(formatted);
    }
    None
}
