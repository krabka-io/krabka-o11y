use super::{
    format_label_replace_metric_scalar_expression, format_label_replace_metric_vector_expression,
    format_metric_label_replace_query, format_simple_metric_query, format_vector_function_text,
    parse_metric_query,
};

pub(crate) struct FormattedBinaryOperand {
    pub(crate) text: String,
    pub(crate) is_label_replace: bool,
}

pub(crate) fn format_label_replace_metric_binary_operand(
    query: &str,
) -> Option<FormattedBinaryOperand> {
    if let Some(formatted) = format_metric_label_replace_query(query) {
        return Some(FormattedBinaryOperand {
            text: formatted,
            is_label_replace: true,
        });
    }
    if let Some(formatted) = format_label_replace_metric_scalar_expression(query) {
        return Some(FormattedBinaryOperand {
            text: formatted,
            is_label_replace: true,
        });
    }
    if let Some(formatted) = format_label_replace_metric_vector_expression(query) {
        return Some(FormattedBinaryOperand {
            text: formatted,
            is_label_replace: true,
        });
    }
    if let Some(formatted) = format_vector_function_text(query) {
        return Some(FormattedBinaryOperand {
            text: formatted,
            is_label_replace: false,
        });
    }
    parse_metric_query(query)
        .ok()
        .and_then(|query| format_simple_metric_query(&query))
        .map(|text| FormattedBinaryOperand {
            text,
            is_label_replace: false,
        })
}
