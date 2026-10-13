use super::{MatchCmp, MatchValue};

struct NestedPresenceComparison<'a> {
    operator: MatchCmp,
    expected: &'a MatchValue,
}

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
    match (NestedPresenceComparison {
        operator: op,
        expected,
    }) {
        NestedPresenceComparison {
            operator: MatchCmp::Eq,
            expected: MatchValue::Nil,
        } => Some(!has_values),
        NestedPresenceComparison {
            operator: MatchCmp::Neq,
            expected: MatchValue::Nil,
        } => Some(has_values),
        _ => None,
    }
}
