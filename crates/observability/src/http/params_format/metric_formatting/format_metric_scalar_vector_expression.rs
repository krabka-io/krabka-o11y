use super::*;

pub(crate) fn format_metric_scalar_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_metric_scalar_arithmetic_expression(query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_metric_scalar_comparison_expression(query) {
        return Some(formatted);
    }
    None
}
