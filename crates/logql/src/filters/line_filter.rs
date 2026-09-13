use super::{IpMatcher, LineFilterOp, ParseError, Regex, line_matches_pattern};

#[derive(Clone, Debug, Eq, PartialEq)]
/// A substring, regular-expression, pattern, or IP filter over one log line.
pub struct LineFilter {
    /// The operation applied to the line.
    pub op: LineFilterOp,
    /// The original filter pattern.
    pub pattern: String,
    pub(crate) ip_matcher: Option<IpMatcher>,
}

impl LineFilter {
    /// Creates a line filter and validates its pattern.
    #[tracing::instrument(level = "debug", skip_all, fields(op = ?op), err)]
    /// # Errors
    /// Returns an error when a regular expression is invalid.
    pub fn new(op: LineFilterOp, pattern: impl Into<String>) -> Result<Self, ParseError> {
        let filter = Self {
            op,
            pattern: pattern.into(),
            ip_matcher: None,
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
        let filter = Self {
            op,
            ip_matcher: Some(IpMatcher::parse(&pattern)?),
            pattern,
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
