use super::{MatchCmp, MatchValue, ordered_matches};

pub(crate) fn float_matches(value: f64, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Float(expected) = expected else {
        return false;
    };
    ordered_matches(value, op, *expected)
}
