use super::{
    OperandTexts, format_metric_and_vector_operands, format_metric_vector_binary_expression,
    split_leading_vector_binary_modifiers, split_top_level_set_query,
};

pub(crate) fn format_metric_vector_set_expression(query: &str) -> Option<String> {
    let split = split_top_level_set_query(query)?;
    let left_text = split.left;
    let operator = split.operator;
    let right_text = split.right;
    let split_modifiers = split_leading_vector_binary_modifiers(right_text);
    let modifiers = split_modifiers.modifiers;
    let right_text = split_modifiers.remainder;
    let operands = format_metric_and_vector_operands(OperandTexts {
        left: left_text,
        right: right_text,
    })?;

    Some(format_metric_vector_binary_expression(
        &operands.left,
        operator,
        modifiers,
        &operands.right,
    ))
}
