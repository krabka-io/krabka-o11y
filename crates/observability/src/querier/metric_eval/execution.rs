use crate::{
    HttpQueryError, LabelReplaceMetricBinaryExpression, MetricVectorArithmeticExpression,
    MetricVectorComparisonExpression, MetricVectorSetExpression, QuerierState, QueryKind,
    SortVectorExpression, TimeRange, Value, add_loki_query_stats,
    apply_label_replace_to_loki_result, apply_metric_binary_arithmetic_to_loki_result,
    apply_metric_binary_comparison_to_loki_result, apply_metric_binary_set_to_loki_result,
    execute_http_metric_binary_arithmetic_query, execute_http_metric_binary_comparison_query,
    execute_http_metric_binary_set_query, execute_http_metric_query,
    execute_http_metric_scalar_arithmetic_query, execute_http_metric_scalar_comparison_query,
    execute_http_scalar_vector_expression_result, json, loki_instant_scalar_or_vector_response,
    loki_range_vector_response, merge_loki_query_stats, parse_label_replace_expression,
    parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
    parse_metric_binary_set_query, parse_metric_label_replace_query, parse_metric_query,
    parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_metric_vector_arithmetic_expression, parse_metric_vector_comparison_expression,
    parse_metric_vector_set_expression, parse_sort_vector_expression, resolved_range_step,
    retain_metric_binary_on_labels, scalar_vector_expression_result, scalar_vector_query_is_vector,
    sort_loki_vector_result, split_logql_function_arguments, strip_outer_parenthesized_expression,
    unix_ns_string_to_loki_seconds,
};

// === split-modules: generated submodules ===
mod execute_http_label_replace_metric_binary_expression;
mod execute_http_metric_binary_operand;
mod execute_http_metric_expression_query;
mod execute_http_metric_vector_arithmetic_expression;
mod execute_http_metric_vector_comparison_expression;
mod execute_http_metric_vector_set_expression;
mod execute_http_sort_vector_expression;
mod normalize_loki_vector_sample_timestamps_to_seconds;

pub (crate) use execute_http_label_replace_metric_binary_expression::execute_http_label_replace_metric_binary_expression;
pub (crate) use execute_http_metric_binary_operand::execute_http_metric_binary_operand;
pub (crate) use execute_http_metric_expression_query::execute_http_metric_expression_query;
pub (crate) use execute_http_metric_vector_arithmetic_expression::execute_http_metric_vector_arithmetic_expression;
pub (crate) use execute_http_metric_vector_comparison_expression::execute_http_metric_vector_comparison_expression;
pub (crate) use execute_http_metric_vector_set_expression::execute_http_metric_vector_set_expression;
pub (crate) use execute_http_sort_vector_expression::execute_http_sort_vector_expression;
pub (crate) use normalize_loki_vector_sample_timestamps_to_seconds::normalize_loki_vector_sample_timestamps_to_seconds;
