use super::{MatchCmp, MatchValue};

/// Whether `value` satisfies `op` against a float `expected`.
///
/// NaN is unequal to everything and ordered against nothing.
#[must_use]
pub fn float_matches(value: f64, op: MatchCmp, expected: &MatchValue) -> bool {
    let expected = match expected {
        MatchValue::Float(value) => *value,
        _ => return false,
    };
    match op {
        MatchCmp::Eq => value.partial_cmp(&expected) == Some(std::cmp::Ordering::Equal),
        MatchCmp::Neq => value.partial_cmp(&expected) != Some(std::cmp::Ordering::Equal),
        MatchCmp::Lt => value < expected,
        MatchCmp::Lte => value <= expected,
        MatchCmp::Gt => value > expected,
        MatchCmp::Gte => value >= expected,
        MatchCmp::Re | MatchCmp::Nre => false,
    }
}
