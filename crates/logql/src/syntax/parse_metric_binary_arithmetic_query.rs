use super::*;

/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_binary_arithmetic_query(
    input: &str,
) -> Result<MetricBinaryArithmetic, ParseError> {
    Parser::new(input).parse_metric_binary_arithmetic()
}
