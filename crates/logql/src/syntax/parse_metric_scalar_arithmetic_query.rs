use super::{MetricScalarArithmetic, ParseError, Parser};

/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_metric_scalar_arithmetic_query(
    input: &str,
) -> Result<MetricScalarArithmetic, ParseError> {
    Parser::new(input).parse_metric_scalar_arithmetic()
}
