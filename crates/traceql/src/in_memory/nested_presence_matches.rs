use super::{MatchCmp, MatchValue};

pub(crate) fn nested_presence_matches(
    has_values: bool,
    op: MatchCmp,
    expected: &MatchValue,
) -> Option<bool> {
    match (op, expected) {
        (MatchCmp::Eq, MatchValue::Nil) => Some(!has_values),
        (MatchCmp::Neq, MatchValue::Nil) => Some(has_values),
        _ => None,
    }
}
