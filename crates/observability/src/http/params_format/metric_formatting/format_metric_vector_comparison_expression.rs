use super::{
    ComparisonResult, OperandTexts, VectorComparisonText, format_metric_and_vector_operands,
    format_vector_comparison_text, split_leading_vector_binary_modifiers,
    split_top_level_comparison_query,
};

pub(crate) fn format_metric_vector_comparison_expression(query: &str) -> Option<String> {
    let split = split_top_level_comparison_query(query)?;
    let left_text = split.left;
    let operator = split.operator;
    let right_text = split.right;
    let right_text = right_text.trim_start();
    let bool_modifier = right_text.starts_with("bool");
    let right_text = if let Some(rest) = right_text.strip_prefix("bool") {
        rest.trim_start()
    } else {
        right_text
    };
    let split_modifiers = split_leading_vector_binary_modifiers(right_text);
    let modifiers = split_modifiers.modifiers;
    let right_text = split_modifiers.remainder;
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
