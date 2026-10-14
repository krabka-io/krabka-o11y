use super::{MatchCmp, MatchValue, matches_ordering, present_value_matches};

/// Whether `value` satisfies `op` against an integer `expected`.
#[must_use]
pub fn int_matches(value: i64, op: MatchCmp, expected: &MatchValue) -> bool {
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    let expected = match expected {
        MatchValue::Int(value) => *value,
        _ => return false,
    };
    matches_ordering(Some(value.cmp(&expected)), op)
}
