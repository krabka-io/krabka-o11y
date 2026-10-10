use super::{ComparisonResult, FormattedVectorBinaryModifiers};

/// The parsed parts of a vector comparison, ready to print.
pub(crate) struct VectorComparisonText<'a> {
    pub(crate) left: &'a str,
    pub(crate) operator: &'a str,
    pub(crate) comparison_result: ComparisonResult,
    pub(crate) modifiers: Option<FormattedVectorBinaryModifiers>,
    pub(crate) right: &'a str,
}

/// Prints a vector comparison as Loki's `format_query` writes it: wrapped in
/// parentheses, with `bool` and any matching modifiers between the operator
/// and the right operand.
pub(crate) fn format_vector_comparison_text(comparison: VectorComparisonText<'_>) -> String {
    let VectorComparisonText {
        left,
        operator,
        comparison_result,
        modifiers,
        right,
    } = comparison;
    let bool_text = match comparison_result {
        ComparisonResult::Bool => "bool ",
        ComparisonResult::Filter => "",
    };
    let modifier_text = modifiers.map_or_else(String::new, |modifiers| {
        format!("{}{}", modifiers.text, modifiers.right_separator)
    });
    format!("({left} {operator} {bool_text}{modifier_text}{right})")
}
