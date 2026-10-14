use super::{MatchCmp, MatchValue};

struct PresentValueComparison<'a> {
    operator: MatchCmp,
    expected: &'a MatchValue,
}

/// The answer to a nil comparison on a present value, or `None` for any
/// other comparison.
#[must_use]
pub fn present_value_matches(op: MatchCmp, expected: &MatchValue) -> Option<bool> {
    match (PresentValueComparison {
        operator: op,
        expected,
    }) {
        PresentValueComparison {
            operator: MatchCmp::Eq,
            expected: MatchValue::Nil,
        } => Some(false),
        PresentValueComparison {
            operator: MatchCmp::Neq,
            expected: MatchValue::Nil,
        } => Some(true),
        _ => None,
    }
}
