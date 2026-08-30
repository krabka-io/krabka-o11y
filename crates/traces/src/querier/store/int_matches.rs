use super::*;

pub(crate) fn int_matches(value: i64, op: MatchCmp, expected: &MatchValue) -> bool {
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    let expected = match expected {
        MatchValue::Int(value) => *value,
        _ => return false,
    };
    match op {
        MatchCmp::Eq => value
            .partial_cmp(&expected)
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Neq => !value
            .partial_cmp(&expected)
            .is_some_and(std::cmp::Ordering::is_eq),
        MatchCmp::Lt => value < expected,
        MatchCmp::Lte => value <= expected,
        MatchCmp::Gt => value > expected,
        MatchCmp::Gte => value >= expected,
        MatchCmp::Re | MatchCmp::Nre => false,
    }
}
