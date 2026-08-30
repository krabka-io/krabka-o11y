use super::{DestinationLabel, SourceLabel, ParseError, template_parse_error};

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
