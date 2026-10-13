use super::{
    OperandTexts, format_metric_and_vector_operands, format_metric_vector_binary_expression,
};
use crate::{http::params_format::split_vector_binary_query, split_top_level_set_query};

pub(crate) fn format_metric_vector_set_expression(query: &str) -> Option<String> {
    let split = split_vector_binary_query(query, split_top_level_set_query)?;
    let operands = format_metric_and_vector_operands(OperandTexts {
        left: split.left,
        right: split.right,
    })?;

    Some(format_metric_vector_binary_expression(
        &operands.left,
        split.operator,
        split.modifiers,
        &operands.right,
    ))
}
