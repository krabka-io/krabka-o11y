use super::{
    OperandTexts, format_metric_and_vector_operands, format_metric_vector_binary_expression,
    split_leading_vector_binary_modifiers, split_top_level_arithmetic_query,
};

pub(crate) fn format_metric_vector_arithmetic_expression(query: &str) -> Option<String> {
    let (left_text, operator, right_text) = split_top_level_arithmetic_query(query)?;
    let (modifiers, right_text) = split_leading_vector_binary_modifiers(right_text);
    let (left, right) = format_metric_and_vector_operands(OperandTexts {
        left: left_text,
        right: right_text,
    })?;

    Some(format_metric_vector_binary_expression(
        &left, operator, modifiers, &right,
    ))
}
