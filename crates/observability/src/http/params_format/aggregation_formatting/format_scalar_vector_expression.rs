use super::*;

pub(crate) fn format_scalar_vector_expression(query: &str) -> Option<String> {
    if let Some(formatted) = format_vector_label_replace_function(query) {
        return Some(formatted);
    }

    let query = query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    if let Some(scalar) = query
        .strip_prefix("vector(")
        .and_then(|query| query.strip_suffix(')'))
    {
        if scalar.starts_with(['+', '-']) {
            return None;
        }
        if let Some(sample) = parse_scalar_sample(scalar) {
            return Some(format!("vector({})", sample.format_fixed_six()));
        }
    }
    if let Some(formatted) = format_vector_set_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_arithmetic_expression(&query) {
        return Some(formatted);
    }
    if let Some(formatted) = format_vector_comparison_expression(&query) {
        return Some(formatted);
    }
    match scalar_vector_expression_result(&query)? {
        ScalarVectorExpressionResult::Scalar { sample } => Some(sample),
        ScalarVectorExpressionResult::Vector { .. } => None,
    }
}
