use super::{MatchCmp, MatchValue};

/// Whether an absent value satisfies `op` against `expected`.
#[must_use]
pub fn nil_matches(op: MatchCmp, expected: &MatchValue) -> bool {
    matches!((op, expected), (MatchCmp::Eq, MatchValue::Nil))
}
