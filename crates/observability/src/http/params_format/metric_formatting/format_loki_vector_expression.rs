use super::{
    format_label_replace_metric_scalar_expression, format_label_replace_metric_vector_expression,
    format_metric_query, format_metric_scalar_arithmetic_expression,
    format_metric_scalar_comparison_expression, format_metric_vector_operation,
    format_scalar_vector_expression, format_vector_label_replace_function, parse_metric_query,
};

pub(crate) fn format_loki_vector_expression(query: &str) -> Option<String> {
    let formatters: [fn(&str) -> Option<String>; 7] = [
        format_metric_vector_operation,
        format_metric_scalar_arithmetic_expression,
        format_metric_scalar_comparison_expression,
        format_vector_label_replace_function,
        format_label_replace_metric_scalar_expression,
        format_label_replace_metric_vector_expression,
        format_scalar_vector_expression,
    ];
    formatters
        .into_iter()
        .find_map(|format| format(query))
        .or_else(|| {
            parse_metric_query(query)
                .ok()
                .and_then(|query| format_metric_query(&query))
        })
}
