use super::{
    ComparisonResult, format_binary_operator_line, format_label_replace_metric_binary_expression,
    split_leading_vector_binary_modifiers, split_top_level_comparison_query,
};

pub(crate) fn format_label_replace_metric_binary_comparison(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let (comparison_result, right_text) = if let Some(rest) = right_text.strip_prefix("bool") {
        (ComparisonResult::Bool, rest.trim_start())
    } else {
        (ComparisonResult::Filter, right_text)
    };
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let operator = format_binary_operator_line(operator, comparison_result, modifiers);
    format_label_replace_metric_binary_expression(left_text.trim(), &operator, right_text.trim())
}
