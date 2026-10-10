use super::{MatchCmp, MatchValue, int_matches, present_value_matches};

/// Whether an enumerated intrinsic such as `span:kind` satisfies `op`.
///
/// `enum_value` maps a lowercase name to its integer, so `expected` can be
/// a name or a number.
#[must_use]
pub fn enum_int_matches(
    value: i64,
    op: MatchCmp,
    expected: &MatchValue,
    enum_value: fn(&str) -> Option<i32>,
) -> bool {
    let expected = match expected {
        MatchValue::Str(name) => enum_value(&name.to_ascii_lowercase()).map(i64::from),
        MatchValue::Int(value) => Some(*value),
        MatchValue::Nil => return present_value_matches(op, expected).unwrap_or(false),
        MatchValue::Float(_) | MatchValue::Bool(_) => None,
    };
    expected.is_some_and(|expected| int_matches(value, op, &MatchValue::Int(expected)))
}
