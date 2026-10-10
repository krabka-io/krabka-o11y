use super::{format_metric_vector_operation, format_sort_vector_expression};

pub(crate) fn format_mixed_metric_vector_expression(query: &str) -> Option<String> {
    format_metric_vector_operation(query).or_else(|| format_sort_vector_expression(query))
}
