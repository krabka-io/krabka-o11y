use super::{MatchCmp, MatchValue};

/// The answer to a nil comparison on a present value, or `None` for any
/// other comparison.
#[must_use]
pub fn present_value_matches(op: MatchCmp, expected: &MatchValue) -> Option<bool> {
    match (op, expected) {
        (MatchCmp::Eq, MatchValue::Nil) => Some(false),
        (MatchCmp::Neq, MatchValue::Nil) => Some(true),
        _ => None,
    }
}
