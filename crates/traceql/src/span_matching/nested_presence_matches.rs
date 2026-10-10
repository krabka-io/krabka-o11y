use super::{MatchCmp, MatchValue};

/// The answer to a nil comparison on a nested field, or `None` for any
/// other comparison.
///
/// `has_values` says whether the span holds the field at all.
#[must_use]
pub fn nested_presence_matches(
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
