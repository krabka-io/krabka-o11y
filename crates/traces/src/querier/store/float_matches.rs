use super::{MatchCmp, MatchValue};

pub(crate) fn float_matches(value: f64, op: MatchCmp, expected: &MatchValue) -> bool {
    let expected = match expected {
        MatchValue::Float(value) => *value,
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
