use super::{DestinationLabel, JsonExpressionPath, JsonPath, ParseError, template_parse_error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonExtraction {
    pub(crate) destination: DestinationLabel,
    pub(crate) expression: JsonExpressionPath,
    pub(crate) path: JsonPath,
}

impl JsonExtraction {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(
        destination: DestinationLabel,
        expression: JsonExpressionPath,
    ) -> Result<Self, ParseError> {
        if destination.0.is_empty() {
            return Err(template_parse_error("expected json label name"));
        }
        let path = JsonPath::parse(&expression.0)?;
        Ok(Self {
            destination,
            expression,
            path,
        })
    }

    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination.0
    }

    #[must_use]
    pub fn expression(&self) -> &str {
        &self.expression.0
    }

    pub(crate) fn evaluate_remaining<'a>(
        &self,
        value: &'a serde_json::value::RawValue,
        remaining: usize,
    ) -> Option<super::json_path::JsonPathMatch<'a>> {
        JsonPath::evaluate_parts(value, &self.path.parts[self.path.parts.len() - remaining..])
    }

    pub(crate) fn evaluate<'a>(
        &self,
        value: &'a serde_json::value::RawValue,
    ) -> Option<super::json_path::JsonPathMatch<'a>> {
        self.path.evaluate(value)
    }
}
