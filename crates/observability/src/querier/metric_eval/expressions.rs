#![cfg_attr(test, allow(dead_code, unused_imports))]

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

#[cfg(test)]
struct ParsedBoolModifier<'a> {
    bool_modifier: bool,
    remaining: &'a str,
}

#[cfg(test)]
fn parse_leading_bool_modifier(query: &str) -> ParsedBoolModifier<'_> {
    let query = query.trim_start();
    if let Some(rest) = query.strip_prefix("bool") {
        ParsedBoolModifier {
            bool_modifier: true,
            remaining: rest.trim_start(),
        }
    } else {
        ParsedBoolModifier {
            bool_modifier: false,
            remaining: query,
        }
    }
}
mod scalar_vector_expression_result;
#[cfg(test)]
mod sort_vector_expression;
mod strip_outer_parenthesized_expression;

#[cfg(test)]
struct OrderedMetricVectors<'a> {
    left: &'a str,
    right: &'a str,
    scalar_on_left: bool,
}

#[cfg(test)]
fn metric_vector_order<'a>(left: &'a str, right: &'a str) -> Option<OrderedMetricVectors<'a>> {
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    if !left_is_vector && right_is_vector {
        Some(OrderedMetricVectors {
            left,
            right,
            scalar_on_left: false,
        })
    } else if left_is_vector && !right_is_vector {
        Some(OrderedMetricVectors {
            left: right,
            right: left,
            scalar_on_left: true,
        })
    } else {
        None
    }
}

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
