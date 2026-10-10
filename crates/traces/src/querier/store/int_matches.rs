use super::{MatchCmp, MatchValue, ordered_matches, present_value_matches};

pub(crate) fn int_matches(value: i64, op: MatchCmp, expected: &MatchValue) -> bool {
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    let MatchValue::Int(expected) = expected else {
        return false;
    };
    ordered_matches(value, op, *expected)
}
