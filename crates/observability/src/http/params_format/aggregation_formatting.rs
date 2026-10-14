use std::fmt::Write as _;

mod apply_approx_metric_selection;
pub(crate) use apply_approx_metric_selection::apply_approx_metric_selection;

use crate::{
    ComparisonResult, Quantile, RangeAggregation, ScalarSample, ScalarVectorExpressionResult,
    VectorAggregation, VectorAggregationOp, VectorGrouping, format_logql_quoted_string,
    parse_formatted_vector_function, parse_scalar_sample, parse_vector_arithmetic_operator,
    scalar_vector_expression_result,
};

mod format_label_replace_arguments;
mod format_loki_decimal_unit;
mod format_loki_duration_ns;
mod format_loki_offset_duration_ns;
mod format_loki_offset_seconds;
mod format_quantile;
mod format_range_aggregation_name;
mod format_scalar_vector_expression;
mod format_vector_aggregation_query;
mod format_vector_arithmetic_expression;
mod format_vector_comparison_expression;
mod format_vector_comparison_text;
mod format_vector_function_text;
mod format_vector_grouping;
mod format_vector_label_replace_function;
mod format_vector_only_expression;
mod format_vector_set_expression;
mod formatted_vector_binary_modifiers;
mod parse_logql_string_argument;
mod parse_vector_binary_modifiers;
mod parse_vector_comparison_operator;
mod parse_vector_group_modifier;
mod parse_vector_matching_modifier;
mod split_logql_function_arguments;

pub(crate) struct ParsedVectorBinaryRight {
    pub(crate) modifiers: Option<FormattedVectorBinaryModifiers>,
    pub(crate) right: String,
}

fn format_vector_binary_expression(query: &str) -> Option<String> {
    format_vector_set_expression(query)
        .or_else(|| format_vector_arithmetic_expression(query))
        .or_else(|| format_vector_comparison_expression(query))
}

fn format_vector_binary_operands(
    query: &str,
    left: &str,
    operator: &str,
    right_position: usize,
) -> Option<String> {
    let parsed_right = parse_vector_binary_right(query, right_position)?;
    Some(match parsed_right.modifiers {
        Some(modifiers) => format!(
            "({left} {operator} {}{}{right})",
            modifiers.text,
            modifiers.right_separator,
            right = parsed_right.right
        ),
        None => format!("({left} {operator} {})", parsed_right.right),
    })
}

fn parse_vector_binary_right(
    query: &str,
    mut right_position: usize,
) -> Option<ParsedVectorBinaryRight> {
    let modifiers =
        if let Some(parsed_modifiers) = parse_vector_binary_modifiers(query, right_position) {
            right_position = parsed_modifiers.position;
            Some(parsed_modifiers.value)
        } else {
            None
        };
    let right = parse_formatted_vector_function(query, right_position)?;
    (right.position == query.len()).then_some(ParsedVectorBinaryRight {
        modifiers,
        right: right.value,
    })
}

pub(crate) use format_label_replace_arguments::format_label_replace_arguments;
pub(crate) use format_loki_decimal_unit::format_loki_decimal_unit;
pub(crate) use format_loki_duration_ns::format_loki_duration_ns;
pub(crate) use format_loki_offset_duration_ns::format_loki_offset_duration_ns;
pub(crate) use format_loki_offset_seconds::format_loki_offset_seconds;
pub(crate) use format_quantile::format_quantile;
pub(crate) use format_range_aggregation_name::format_range_aggregation_name;
pub(crate) use format_scalar_vector_expression::format_scalar_vector_expression;
pub(crate) use format_vector_aggregation_query::format_vector_aggregation_query;
pub(crate) use format_vector_arithmetic_expression::format_vector_arithmetic_expression;
pub(crate) use format_vector_comparison_expression::format_vector_comparison_expression;
pub(crate) use format_vector_comparison_text::{
    VectorComparisonText, format_vector_comparison_text,
};
pub(crate) use format_vector_function_text::format_vector_function_text;
pub(crate) use format_vector_grouping::format_vector_grouping;
pub(crate) use format_vector_label_replace_function::format_vector_label_replace_function;
pub(crate) use format_vector_only_expression::format_vector_only_expression;
pub(crate) use format_vector_set_expression::format_vector_set_expression;
pub(crate) use formatted_vector_binary_modifiers::FormattedVectorBinaryModifiers;
pub(crate) use parse_logql_string_argument::parse_logql_string_argument;
pub(crate) use parse_vector_binary_modifiers::parse_vector_binary_modifiers;
pub(crate) use parse_vector_comparison_operator::parse_vector_comparison_operator;
pub(crate) use parse_vector_group_modifier::parse_vector_group_modifier;
pub(crate) use parse_vector_matching_modifier::parse_vector_matching_modifier;
pub(crate) use split_logql_function_arguments::split_logql_function_arguments;
