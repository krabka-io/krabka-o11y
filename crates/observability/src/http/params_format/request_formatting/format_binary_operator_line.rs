use super::*;

pub(crate) fn format_binary_operator_line(
    operator: &str,
    bool_modifier: bool,
    modifiers: Option<FormattedVectorBinaryModifiers>,
) -> String {
    let mut formatted = operator.to_string();
    if bool_modifier {
        formatted.push_str(" bool");
    }
    if let Some(modifiers) = modifiers {
        formatted.push(' ');
        formatted.push_str(&modifiers.text);
    }
    formatted
}
