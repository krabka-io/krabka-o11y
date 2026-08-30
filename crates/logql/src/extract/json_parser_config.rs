use super::{JsonExtraction, ParseError, template_parse_error, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonParserConfig {
    pub(crate) extractions: Vec<JsonExtraction>,
}

impl JsonParserConfig {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(extractions: Vec<JsonExtraction>) -> Result<Self, ParseError> {
        if extractions.is_empty() {
            return Err(template_parse_error("expected json extraction"));
        }
        let mut destinations = BTreeSet::new();
        for extraction in &extractions {
            if !destinations.insert(extraction.destination.clone()) {
                return Err(template_parse_error(
                    "json extraction destination appears more than once",
                ));
            }
        }
        Ok(Self { extractions })
    }

    #[must_use]
    pub fn extractions(&self) -> &[JsonExtraction] {
        &self.extractions
    }
}
