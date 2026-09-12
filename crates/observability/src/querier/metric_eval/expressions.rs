use crate::{
    BTreeMap, TimeRange, Value, VectorScalarExpressionParser, eval_times, json, loki_success_value,
    unix_ns_string_to_loki_seconds,
};
#[cfg(test)]
use crate::{
    ComparisonOp, MetricBinarySetOp, MetricScalarArithmeticOp, MetricVectorGroupModifier,
    MetricVectorMatching, parse_logql_string_argument, scalar_vector_query_is_vector,
    split_logql_function_arguments, split_top_level_arithmetic_query,
    split_top_level_comparison_query, split_top_level_set_query,
};

#[cfg(test)]
mod label_replace_expression;
#[cfg(test)]
mod label_replace_metric_binary_expression;
mod loki_instant_scalar_or_vector_response;
mod loki_range_vector_response;
#[cfg(test)]
mod metric_vector_arithmetic_expression;
#[cfg(test)]
mod metric_vector_comparison_expression;
#[cfg(test)]
mod metric_vector_set_expression;
#[cfg(test)]
mod parse_label_replace_expression;
#[cfg(test)]
mod parse_label_replace_metric_binary_expression;
#[cfg(test)]
mod parse_leading_label_list;
#[cfg(test)]
mod parse_leading_metric_vector_group_modifier;
#[cfg(test)]
mod parse_leading_metric_vector_matching_modifier;
#[cfg(test)]
mod parse_metric_arithmetic_operator;
#[cfg(test)]
mod parse_metric_comparison_operator;
#[cfg(test)]
mod parse_metric_set_operator;
#[cfg(test)]
mod parse_metric_vector_arithmetic_expression;
#[cfg(test)]
mod parse_metric_vector_comparison_expression;
#[cfg(test)]
mod parse_metric_vector_set_expression;
#[cfg(test)]
mod parse_sort_vector_expression;
mod scalar_vector_expression_result;
#[cfg(test)]
mod sort_vector_expression;
mod strip_outer_parenthesized_expression;

#[cfg(test)]
pub(crate) use label_replace_expression::LabelReplaceExpression;
#[cfg(test)]
pub(crate) use label_replace_metric_binary_expression::LabelReplaceMetricBinaryExpression;
pub(crate) use loki_instant_scalar_or_vector_response::loki_instant_scalar_or_vector_response;
pub(crate) use loki_range_vector_response::loki_range_vector_response;
#[cfg(test)]
pub(crate) use metric_vector_arithmetic_expression::MetricVectorArithmeticExpression;
#[cfg(test)]
pub(crate) use metric_vector_comparison_expression::MetricVectorComparisonExpression;
#[cfg(test)]
pub(crate) use metric_vector_set_expression::MetricVectorSetExpression;
#[cfg(test)]
pub(crate) use parse_label_replace_expression::parse_label_replace_expression;
#[cfg(test)]
pub(crate) use parse_label_replace_metric_binary_expression::parse_label_replace_metric_binary_expression;
#[cfg(test)]
pub(crate) use parse_leading_label_list::parse_leading_label_list;
#[cfg(test)]
pub(crate) use parse_leading_metric_vector_group_modifier::parse_leading_metric_vector_group_modifier;
#[cfg(test)]
pub(crate) use parse_leading_metric_vector_matching_modifier::parse_leading_metric_vector_matching_modifier;
#[cfg(test)]
pub(crate) use parse_metric_arithmetic_operator::parse_metric_arithmetic_operator;
#[cfg(test)]
pub(crate) use parse_metric_comparison_operator::parse_metric_comparison_operator;
#[cfg(test)]
pub(crate) use parse_metric_set_operator::parse_metric_set_operator;
#[cfg(test)]
pub(crate) use parse_metric_vector_arithmetic_expression::parse_metric_vector_arithmetic_expression;
#[cfg(test)]
pub(crate) use parse_metric_vector_comparison_expression::parse_metric_vector_comparison_expression;
#[cfg(test)]
pub(crate) use parse_metric_vector_set_expression::parse_metric_vector_set_expression;
#[cfg(test)]
pub(crate) use parse_sort_vector_expression::parse_sort_vector_expression;
pub(crate) use scalar_vector_expression_result::{
    ScalarVectorExpressionResult, scalar_vector_expression_result,
};
#[cfg(test)]
pub(crate) use sort_vector_expression::SortVectorExpression;
pub(crate) use strip_outer_parenthesized_expression::strip_outer_parenthesized_expression;
