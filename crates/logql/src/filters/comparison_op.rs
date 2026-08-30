use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    RegexEqual,
    RegexNotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

impl ComparisonOp {
    pub(crate) fn compare_numbers(self, candidate: f64, expected: f64) -> bool {
        self.matches_ordering(candidate.partial_cmp(&expected))
    }

    pub(crate) fn compare_sizes(self, candidate: ByteSize, expected: ByteSize) -> bool {
        self.matches_ordering(candidate.partial_cmp(&expected))
    }

    pub(crate) fn matches_ordering(self, ordering: Option<Ordering>) -> bool {
        ordering.is_some_and(|ordering| match self {
            Self::Equal => ordering == Ordering::Equal,
            Self::NotEqual => ordering != Ordering::Equal,
            Self::RegexEqual | Self::RegexNotEqual => false,
            Self::Greater => ordering == Ordering::Greater,
            Self::GreaterEqual => matches!(ordering, Ordering::Greater | Ordering::Equal),
            Self::Less => ordering == Ordering::Less,
            Self::LessEqual => matches!(ordering, Ordering::Less | Ordering::Equal),
        })
    }

    pub(crate) fn compare_strings(self, candidate: &str, expected: &str) -> bool {
        match self {
            Self::Equal => candidate == expected,
            Self::NotEqual => candidate != expected,
            Self::RegexEqual => Regex::new(expected).is_ok_and(|regex| regex.is_match(candidate)),
            Self::RegexNotEqual => {
                Regex::new(expected).is_ok_and(|regex| !regex.is_match(candidate))
            }
            Self::Greater => candidate > expected,
            Self::GreaterEqual => candidate >= expected,
            Self::Less => candidate < expected,
            Self::LessEqual => candidate <= expected,
        }
    }
}
