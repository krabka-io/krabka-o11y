use super::{format_label_replace_metric_binary_operand, indent_logql_lines};

pub(crate) fn format_label_replace_metric_binary_expression(
    left_text: &str,
    operator: &str,
    right_text: &str,
) -> Option<String> {
    let (left, left_is_label_replace) = format_label_replace_metric_binary_operand(left_text)?;
    let (right, right_is_label_replace) = format_label_replace_metric_binary_operand(right_text)?;
    if !left_is_label_replace && !right_is_label_replace {
        return None;
    }
    Some(format!(
        "{}\n{operator}\n{}",
        indent_logql_lines(&left, "  "),
        indent_logql_lines(&right, "  "),
    ))
}
