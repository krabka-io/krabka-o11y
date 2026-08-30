use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

impl LabelMatcher {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(
        name: impl Into<String>,
        op: MatchOp,
        value: impl Into<String>,
    ) -> Result<Self, ParseError> {
        let matcher = Self {
            name: name.into(),
            op,
            value: value.into(),
        };
        matcher.validate()?;
        Ok(matcher)
    }

    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        let candidate = labels.get(&self.name);
        match self.op {
            MatchOp::Equal => candidate == Some(&self.value),
            MatchOp::NotEqual => candidate != Some(&self.value),
            MatchOp::RegexEqual => self.regex().is_match(candidate.map_or("", String::as_str)),
            MatchOp::RegexNotEqual => candidate.is_none_or(|value| !self.regex().is_match(value)),
        }
    }

    #[must_use]
    pub fn matches_empty_value(&self) -> bool {
        match self.op {
            MatchOp::Equal => self.value.is_empty(),
            MatchOp::NotEqual => !self.value.is_empty(),
            MatchOp::RegexEqual => self.regex().is_match(""),
            MatchOp::RegexNotEqual => !self.regex().is_match(""),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ParseError> {
        if matches!(self.op, MatchOp::RegexEqual | MatchOp::RegexNotEqual) {
            Regex::new(&anchored_regex_pattern(&self.value)).map_err(|source| {
                ParseError::InvalidRegex {
                    pattern: self.value.clone(),
                    source,
                }
            })?;
        }
        Ok(())
    }

    pub(crate) fn regex(&self) -> Regex {
        Regex::new(&anchored_regex_pattern(&self.value))
            .expect("regex matcher validated at construction")
    }
}
