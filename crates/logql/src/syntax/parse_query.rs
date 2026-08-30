use super::*;

#[tracing::instrument(level = "info", skip_all, fields(query = %input), err)]
/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn parse_query(input: &str) -> Result<StreamQuery, ParseError> {
    Parser::new(input).parse()
}
