use super::*;

pub(crate) fn bool_matches(value: bool, op: MatchCmp, expected: &MatchValue) -> bool {
    let MatchValue::Bool(expected) = expected else {
        return false;
    };
    match op {
        MatchCmp::Eq => value == *expected,
        MatchCmp::Neq => value != *expected,
        MatchCmp::Lt
        | MatchCmp::Lte
        | MatchCmp::Gt
        | MatchCmp::Gte
        | MatchCmp::Re
        | MatchCmp::Nre => false,
    }
}
