use super::{
    ComparisonResult, format_binary_operator_line, format_label_replace_metric_binary_expression,
    split_leading_vector_binary_modifiers, split_top_level_set_query,
};

pub(crate) fn format_label_replace_metric_binary_set(query: &str) -> Option<String> {
    let split = split_top_level_set_query(query)?;
    let left_text = split.left;
    let operator = split.operator;
    let right_text = split.right;
    let split_modifiers = split_leading_vector_binary_modifiers(right_text);
    let modifiers = split_modifiers.modifiers;
    let right_text = split_modifiers.remainder;
    let operator = format_binary_operator_line(operator, ComparisonResult::Filter, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}
