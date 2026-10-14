use super::{
    ComparisonResult, OperandTexts, VectorComparisonText, format_metric_and_vector_operands,
    format_vector_comparison_text,
};

pub(crate) fn format_metric_vector_comparison_expression(query: &str) -> Option<String> {
    let split = super::super::split_vector_comparison_query(query)?;
    let left_text = split.left;
    let operator = split.operator;
    let right_text = split.right;
    let bool_modifier = split.bool_modifier;
    let modifiers = split.modifiers;
    let operands = format_metric_and_vector_operands(OperandTexts {
        left: left_text,
        right: right_text,
    })?;

    Some(format_vector_comparison_text(VectorComparisonText {
        left: &operands.left,
        operator,
        comparison_result: ComparisonResult::from_bool_modifier(bool_modifier),
        modifiers,
        right: &operands.right,
    }))
}
