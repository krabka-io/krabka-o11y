use super::{BTreeSet, DestinationLabel, ParseError, SourceLabel, template_parse_error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogfmtExtraction {
    pub(crate) destination: DestinationLabel,
    pub(crate) source: SourceLabel,
}

impl LogfmtExtraction {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn same(name: impl Into<String>) -> Result<Self, ParseError> {
        let name = name.into();
        Self::rename(DestinationLabel(name.clone()), SourceLabel(name))
    }

    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn rename(destination: DestinationLabel, source: SourceLabel) -> Result<Self, ParseError> {
        let extraction = Self {
            destination,
            source,
        };
        extraction.validate()?;
        Ok(extraction)
    }

    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination.0
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source.0
    }

    pub(crate) fn validate(&self) -> Result<(), ParseError> {
        if self.destination.0.is_empty() || self.source.0.is_empty() {
            return Err(template_parse_error("expected logfmt label name"));
        }
        Ok(())
    }
}

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
