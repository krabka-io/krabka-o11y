use super::{format_simple_metric_query, format_vector_function_text, parse_metric_query};

/// The text on either side of a binary operator.
#[derive(Clone, Copy)]
pub(crate) struct OperandTexts<'a> {
    pub(crate) left: &'a str,
    pub(crate) right: &'a str,
}

pub(crate) struct FormattedOperands {
    pub(crate) left: String,
    pub(crate) right: String,
}

/// Formats the operands of a binary expression between a simple metric query
/// and a vector function, which may stand on either side.
pub(crate) fn format_metric_and_vector_operands(
    operands: OperandTexts<'_>,
) -> Option<FormattedOperands> {
    let OperandTexts {
        left: left_text,
        right: right_text,
    } = operands;
    let format_metric = |text: &str| {
        parse_metric_query(text.trim())
            .ok()
            .and_then(|query| format_simple_metric_query(&query))
    };
    if let Some(left) = format_metric(left_text)
        && let Some(right) = format_vector_function_text(right_text.trim())
    {
        return Some(FormattedOperands { left, right });
    }
    if let Some(left) = format_vector_function_text(left_text.trim())
        && let Some(right) = format_metric(right_text)
    {
        return Some(FormattedOperands { left, right });
    }
    None
}
