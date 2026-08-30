use super::{LabelSelectionMatcher, ParseError, Labels, Regex, anchored_regex_pattern, template_parse_error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelSelection {
    pub(crate) name: String,
    pub(crate) matcher: Option<LabelSelectionMatcher>,
}

impl LabelSelection {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn name(name: impl Into<String>) -> Result<Self, ParseError> {
        let selection = Self {
            name: name.into(),
            matcher: None,
        };
        selection.validate()?;
        Ok(selection)
    }

    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn equal(name: impl Into<String>, value: impl Into<String>) -> Result<Self, ParseError> {
        let selection = Self {
            name: name.into(),
            matcher: Some(LabelSelectionMatcher::Equal(value.into())),
        };
        selection.validate()?;
        Ok(selection)
    }

    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn regex(name: impl Into<String>, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let selection = Self {
            name: name.into(),
            matcher: Some(LabelSelectionMatcher::Regex(pattern.into())),
        };
        selection.validate()?;
        Ok(selection)
    }

    #[must_use]
    pub fn name_str(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn matcher(&self) -> Option<&LabelSelectionMatcher> {
        self.matcher.as_ref()
    }

    #[must_use]
    pub(crate) fn matches(&self, fields: &Labels) -> bool {
        let Some(value) = fields.get(&self.name) else {
            return false;
        };
        match &self.matcher {
            None => true,
            Some(LabelSelectionMatcher::Equal(expected)) => value == expected,
            Some(LabelSelectionMatcher::Regex(pattern)) => {
                Regex::new(&anchored_regex_pattern(pattern))
                    .expect("label selection regex validated at construction")
                    .is_match(value)
            }
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ParseError> {
        if self.name.is_empty() {
            return Err(template_parse_error("expected label name"));
        }
        if let Some(LabelSelectionMatcher::Regex(pattern)) = &self.matcher {
            Regex::new(&anchored_regex_pattern(pattern)).map_err(|source| {
                ParseError::InvalidRegex {
                    pattern: pattern.clone(),
                    source,
                }
            })?;
        }
        Ok(())
    }
}
