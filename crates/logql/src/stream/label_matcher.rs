use std::borrow::Cow;

use super::{Labels, MatchOp, ParseError, Regex, anchored_regex_pattern};
use crate::regex_cache::{RegexCache, resolve_regex};

#[derive(Clone, Debug)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
    compiled_regex: Option<RegexCache>,
}

impl LabelMatcher {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(
        name: impl Into<String>,
        op: MatchOp,
        value: impl Into<String>,
    ) -> Result<Self, ParseError> {
        let mut matcher = Self {
            name: name.into(),
            op,
            value: value.into(),
            compiled_regex: None,
        };
        matcher.validate()?;
        Ok(matcher)
    }

    #[must_use]
    pub fn matches(&self, labels: &Labels) -> bool {
        self.op
            .accepts_named_label(&self.name, &self.value, labels, |value| {
                self.regex().is_match(value)
            })
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

    pub(crate) fn validate(&mut self) -> Result<(), ParseError> {
        if matches!(self.op, MatchOp::RegexEqual | MatchOp::RegexNotEqual) {
            let regex = Regex::new(&anchored_regex_pattern(&self.value)).map_err(|source| {
                ParseError::InvalidRegex {
                    pattern: self.value.clone(),
                    source,
                }
            })?;
            self.compiled_regex = Some(RegexCache::new(self.value.clone(), regex));
        }
        Ok(())
    }

    pub(crate) fn regex(&self) -> Cow<'_, Regex> {
        // The source fields are public. A caller can change the pattern or
        // operation after construction; never use a stale compiled pattern.
        resolve_regex(
            self.compiled_regex.as_ref(),
            &self.value,
            anchored_regex_pattern,
            "regex matcher validated at construction",
        )
    }
}

impl PartialEq for LabelMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.op == other.op && self.value == other.value
    }
}

impl Eq for LabelMatcher {}
