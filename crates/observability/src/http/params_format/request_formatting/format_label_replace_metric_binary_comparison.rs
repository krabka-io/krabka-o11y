use super::{
    ComparisonResult, format_binary_operator_line, format_label_replace_metric_binary_expression,
};

pub(crate) fn format_label_replace_metric_binary_comparison(query: &str) -> Option<String> {
    let split = super::super::split_vector_comparison_query(query)?;
    let left_text = split.left;
    let operator = split.operator;
    let comparison_result = if split.bool_modifier {
        ComparisonResult::Bool
    } else {
        ComparisonResult::Filter
    };
    let modifiers = split.modifiers;
    let operator = format_binary_operator_line(operator, comparison_result, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, split.right.trim())
}
