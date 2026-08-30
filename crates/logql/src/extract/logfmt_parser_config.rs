use super::{BTreeSet, LogfmtExtraction, ParseError, template_parse_error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogfmtParserConfig {
    pub(crate) extractions: Vec<LogfmtExtraction>,
    pub(crate) strict: bool,
    pub(crate) keep_empty: bool,
}

impl LogfmtParserConfig {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(extractions: Vec<LogfmtExtraction>) -> Result<Self, ParseError> {
        Self::with_options(extractions, false, false)
    }

    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn flags(strict: bool, keep_empty: bool) -> Result<Self, ParseError> {
        Self::with_options(Vec::new(), strict, keep_empty)
    }

    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn with_options(
        extractions: Vec<LogfmtExtraction>,
        strict: bool,
        keep_empty: bool,
    ) -> Result<Self, ParseError> {
        if extractions.is_empty() {
            if strict || keep_empty {
                return Ok(Self {
                    extractions,
                    strict,
                    keep_empty,
                });
            }
            return Err(template_parse_error("expected logfmt extraction"));
        }
        let mut destinations = BTreeSet::new();
        for extraction in &extractions {
            if !destinations.insert(extraction.destination.clone()) {
                return Err(template_parse_error(
                    "logfmt extraction destination appears more than once",
                ));
            }
        }
        Ok(Self {
            extractions,
            strict,
            keep_empty,
        })
    }

    #[must_use]
    pub fn extractions(&self) -> &[LogfmtExtraction] {
        &self.extractions
    }

    #[must_use]
    pub fn strict(&self) -> bool {
        self.strict
    }

    #[must_use]
    pub fn keep_empty(&self) -> bool {
        self.keep_empty
    }
}
