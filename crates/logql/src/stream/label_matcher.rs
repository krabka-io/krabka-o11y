use std::borrow::Cow;

use super::{Labels, MatchOp, ParseError, Regex, anchored_regex_pattern};

#[derive(Clone, Debug)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
    compiled_regex: Option<(String, Regex)>,
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

    pub(crate) fn validate(&mut self) -> Result<(), ParseError> {
        if matches!(self.op, MatchOp::RegexEqual | MatchOp::RegexNotEqual) {
            let regex = Regex::new(&anchored_regex_pattern(&self.value)).map_err(|source| {
                ParseError::InvalidRegex {
                    pattern: self.value.clone(),
                    source,
                }
            })?;
            self.compiled_regex = Some((self.value.clone(), regex));
        }
        Ok(())
    }

    pub(crate) fn regex(&self) -> Cow<'_, Regex> {
        if let Some((pattern, regex)) = &self.compiled_regex
            && pattern == &self.value
        {
            return Cow::Borrowed(regex);
        }
        // The source fields are public. A caller can change the pattern or
        // operation after construction; never use a stale compiled pattern.
        Cow::Owned(
            Regex::new(&anchored_regex_pattern(&self.value))
                .expect("regex matcher validated at construction"),
        )
    }
}

impl PartialEq for LabelMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.op == other.op && self.value == other.value
    }
}

impl Eq for LabelMatcher {}
