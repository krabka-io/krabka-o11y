use super::{
    ComparisonResult, format_binary_operator_line, format_label_replace_metric_binary_expression,
};
use crate::{http::params_format::split_vector_binary_query, split_top_level_set_query};

pub(crate) fn format_label_replace_metric_binary_set(query: &str) -> Option<String> {
    let split = split_vector_binary_query(query, split_top_level_set_query)?;
    let operator =
        format_binary_operator_line(split.operator, ComparisonResult::Filter, split.modifiers);
    format_label_replace_metric_binary_expression(split.left.trim(), &operator, split.right.trim())
}
