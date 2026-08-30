use super::{JsonPathParser, JsonPathPart, ParseError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonPath {
    pub(crate) parts: Vec<JsonPathPart>,
}

impl JsonPath {
    pub(crate) fn parse(expression: &str) -> Result<Self, ParseError> {
        let mut parser = JsonPathParser::new(expression);
        parser.parse()
    }

    pub(crate) fn evaluate<'a>(
        &self,
        value: &'a serde_json::Value,
    ) -> Option<&'a serde_json::Value> {
        let mut current = value;
        for part in &self.parts {
            match part {
                JsonPathPart::Field(name) => {
                    current = current.as_object()?.get(name)?;
                }
                JsonPathPart::Index(index) => {
                    current = current.as_array()?.get(*index)?;
                }
            }
        }
        Some(current)
    }
}
