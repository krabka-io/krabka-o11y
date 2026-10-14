use super::{MatchCmp, MatchValue, matches_ordering};

/// Whether `value` satisfies `op` against a float `expected`.
///
/// NaN is unequal to everything and ordered against nothing.
#[must_use]
pub fn float_matches(value: f64, op: MatchCmp, expected: &MatchValue) -> bool {
    let expected = match expected {
        MatchValue::Float(value) => *value,
        _ => return false,
    };
    matches_ordering(value.partial_cmp(&expected), op)
}
