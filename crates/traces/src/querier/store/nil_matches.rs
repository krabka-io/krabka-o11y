use super::*;

pub(crate) fn nil_matches(op: MatchCmp, expected: &MatchValue) -> bool {
    matches!((op, expected), (MatchCmp::Eq, MatchValue::Nil))
}
