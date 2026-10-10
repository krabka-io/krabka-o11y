use std::borrow::Cow;

use super::{IpMatcher, LineFilterOp, ParseError, Regex, line_matches_pattern};

#[derive(Clone, Debug)]
/// A substring, regular-expression, pattern, or IP filter over one log line.
pub struct LineFilter {
    /// The operation applied to the line.
    pub op: LineFilterOp,
    /// The original filter pattern.
    pub pattern: String,
    pub(crate) ip_matcher: Option<IpMatcher>,
    compiled_regex: Option<(String, Regex)>,
}

impl LineFilter {
    /// Creates a line filter and validates its pattern.
    #[tracing::instrument(level = "debug", skip_all, fields(op = ?op), err)]
    /// # Errors
    /// Returns an error when a regular expression is invalid.
    pub fn new(op: LineFilterOp, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let mut filter = Self {
            op,
            pattern: pattern.into(),
            ip_matcher: None,
            compiled_regex: None,
        };
        filter.validate()?;
        Ok(filter)
    }

    /// Creates an IP line filter and validates its operation and pattern.
    #[tracing::instrument(level = "debug", skip_all, fields(op = ?op), err)]
    /// # Errors
    /// Returns an error when the operation or IP pattern is invalid.
    pub fn ip(op: LineFilterOp, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let pattern = pattern.into();
        let mut filter = Self {
            op,
            ip_matcher: Some(IpMatcher::parse(&pattern)?),
            pattern,
            compiled_regex: None,
        };
        filter.validate()?;
        Ok(filter)
    }

    #[must_use]
    /// Reports whether this filter matches IP addresses in the line.
    pub fn is_ip_matcher(&self) -> bool {
        self.ip_matcher.is_some()
    }

    #[must_use]
    /// Tests the line against this filter.
    pub fn matches(&self, line: &str) -> bool {
        if let Some(matcher) = &self.ip_matcher {
            return match self.op {
                LineFilterOp::Contains => matcher.matches_line(line),
                LineFilterOp::NotContains => !matcher.matches_line(line),
                _ => false,
            };
        }
        match self.op {
            LineFilterOp::Contains => line.contains(&self.pattern),
            LineFilterOp::NotContains => !line.contains(&self.pattern),
            LineFilterOp::Regex => self.regex().is_match(line),
            LineFilterOp::NotRegex => !self.regex().is_match(line),
            LineFilterOp::Pattern => line_matches_pattern(line, &self.pattern),
            LineFilterOp::NotPattern => !line_matches_pattern(line, &self.pattern),
        }
    }

    pub(crate) fn validate(&mut self) -> Result<(), ParseError> {
        if self.ip_matcher.is_some()
            && !matches!(self.op, LineFilterOp::Contains | LineFilterOp::NotContains)
        {
            return Err(ParseError::Syntax {
                message: "ip line filters only support |= and !=".to_string(),
                position: 0,
            });
        }
        if matches!(self.op, LineFilterOp::Regex | LineFilterOp::NotRegex) {
            let regex = Regex::new(&self.pattern).map_err(|source| ParseError::InvalidRegex {
                pattern: self.pattern.clone(),
                source,
            })?;
            self.compiled_regex = Some((self.pattern.clone(), regex));
        }
        Ok(())
    }

    pub(crate) fn regex(&self) -> Cow<'_, Regex> {
        if let Some((pattern, regex)) = &self.compiled_regex
            && pattern == &self.pattern
        {
            return Cow::Borrowed(regex);
        }
        // Public source fields can change after construction. Preserve their
        // current meaning instead of matching with a stale compiled pattern.
        Cow::Owned(Regex::new(&self.pattern).expect("line regex filter validated at construction"))
    }
}

impl PartialEq for LineFilter {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op && self.pattern == other.pattern && self.ip_matcher == other.ip_matcher
    }
}

impl Eq for LineFilter {}
