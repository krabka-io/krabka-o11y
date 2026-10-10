use super::MatchCmp;

/// Compare `value` with `expected` under `op`. A value that does not compare,
/// such as NaN, is unequal to everything and ordered against nothing.
pub(crate) fn ordered_matches<T: PartialOrd + Copy>(value: T, op: MatchCmp, expected: T) -> bool {
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
