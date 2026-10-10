#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
}

impl MatchOp {
    /// Reports whether a label value satisfies this operator.
    ///
    /// `candidate` is the label's value, or `None` when the label is absent.
    /// `regex_matches` tests a value against the matcher's anchored regex; only
    /// the regex operators call it. An absent label matches `=~` as the empty
    /// string and always matches `!~`.
    #[must_use]
    pub fn accepts(
        self,
        expected: &str,
        candidate: Option<&str>,
        regex_matches: impl Fn(&str) -> bool,
    ) -> bool {
        match self {
            Self::Equal => candidate == Some(expected),
            Self::NotEqual => candidate != Some(expected),
            Self::RegexEqual => regex_matches(candidate.unwrap_or("")),
            Self::RegexNotEqual => candidate.is_none_or(|value| !regex_matches(value)),
        }
    }
}
