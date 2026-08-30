use super::{
    format_binary_operator_line, format_label_replace_metric_binary_expression,
    split_leading_vector_binary_modifiers, split_top_level_comparison_query,
};

pub(crate) fn format_label_replace_metric_binary_comparison(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let (bool_modifier, right_text) = if let Some(rest) = right_text.strip_prefix("bool") {
        (true, rest.trim_start())
    } else {
        (false, right_text)
    };
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, bool_modifier, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}
