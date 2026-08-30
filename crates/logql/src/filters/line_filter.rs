use super::{LineFilterOp, IpMatcher, ParseError, line_matches_pattern, Regex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineFilter {
    pub op: LineFilterOp,
    pub pattern: String,
    pub(crate) ip_matcher: Option<IpMatcher>,
}

impl LineFilter {
    #[tracing::instrument(level = "debug", skip_all, fields(op = ?op), err)]
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(op: LineFilterOp, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let filter = Self {
            op,
            pattern: pattern.into(),
            ip_matcher: None,
        };
        filter.validate()?;
        Ok(filter)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(op = ?op), err)]
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn ip(op: LineFilterOp, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let pattern = pattern.into();
        let filter = Self {
            op,
            ip_matcher: Some(IpMatcher::parse(&pattern)?),
            pattern,
        };
        filter.validate()?;
        Ok(filter)
    }

    #[must_use]
    pub fn is_ip_matcher(&self) -> bool {
        self.ip_matcher.is_some()
    }

    #[must_use]
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

    pub(crate) fn validate(&self) -> Result<(), ParseError> {
        if self.ip_matcher.is_some()
            && !matches!(self.op, LineFilterOp::Contains | LineFilterOp::NotContains)
        {
            return Err(ParseError::Syntax {
                message: "ip line filters only support |= and !=".to_string(),
                position: 0,
            });
        }
        if matches!(self.op, LineFilterOp::Regex | LineFilterOp::NotRegex) {
            Regex::new(&self.pattern).map_err(|source| ParseError::InvalidRegex {
                pattern: self.pattern.clone(),
                source,
            })?;
        }
        Ok(())
    }

    pub(crate) fn regex(&self) -> Regex {
        Regex::new(&self.pattern).expect("line regex filter validated at construction")
    }
}
