use super::{
    format_binary_operator_line, format_label_replace_metric_binary_expression,
    split_leading_vector_binary_modifiers, split_top_level_set_query,
};

pub(crate) fn format_label_replace_metric_binary_set(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_set_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, false, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}
