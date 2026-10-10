use super::{
    format_metric_vector_arithmetic_expression, format_metric_vector_comparison_expression,
    format_metric_vector_set_expression,
};

/// Formats a binary operation between two vectors: arithmetic, comparison,
/// or a set operator.
pub(crate) fn format_metric_vector_operation(query: &str) -> Option<String> {
    format_metric_vector_arithmetic_expression(query)
        .or_else(|| format_metric_vector_comparison_expression(query))
        .or_else(|| format_metric_vector_set_expression(query))
}
