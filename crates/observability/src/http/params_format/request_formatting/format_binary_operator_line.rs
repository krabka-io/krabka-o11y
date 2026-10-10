use super::{ComparisonResult, FormattedVectorBinaryModifiers};

pub(crate) fn format_binary_operator_line(
    operator: &str,
    comparison_result: ComparisonResult,
    modifiers: Option<FormattedVectorBinaryModifiers>,
) -> String {
    let mut formatted = operator.to_string();
    if comparison_result == ComparisonResult::Bool {
        formatted.push_str(" bool");
    }
    if let Some(modifiers) = modifiers {
        formatted.push(' ');
        formatted.push_str(&modifiers.text);
    }
    formatted
}
