use super::{
    OperandTexts, format_metric_and_vector_operands, split_leading_vector_binary_modifiers,
    split_top_level_comparison_query,
};

pub(crate) fn format_metric_vector_comparison_expression(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_comparison_query(query)?;
    let right_text = right_text.trim_start();
    let (bool_modifier, right_text) = if let Some(rest) = right_text.strip_prefix("bool") {
        (true, rest.trim_start())
    } else {
        (false, right_text)
    };
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let (left, right) = format_metric_and_vector_operands(OperandTexts {
        left: left_text,
        right: right_text,
    })?;

    match (bool_modifier, modifiers) {
        (true, Some(modifiers)) => Some(format!(
            "({left} {operator} bool {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (true, None) => Some(format!("({left} {operator} bool {right})")),
        (false, Some(modifiers)) => Some(format!(
            "({left} {operator} {}{}{right})",
            modifiers.text, modifiers.right_separator
        )),
        (false, None) => Some(format!("({left} {operator} {right})")),
    }
}
