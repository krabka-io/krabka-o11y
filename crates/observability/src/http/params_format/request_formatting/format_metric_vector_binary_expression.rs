use super::FormattedVectorBinaryModifiers;

pub(crate) fn format_metric_vector_binary_expression(
    left: &str,
    operator: &str,
    modifiers: Option<FormattedVectorBinaryModifiers>,
    right: &str,
) -> String {
    match modifiers {
        Some(modifiers) => format!(
            "({left} {operator} {}{}{right})",
            modifiers.text, modifiers.right_separator
        ),
        None => format!("({left} {operator} {right})"),
    }
}
