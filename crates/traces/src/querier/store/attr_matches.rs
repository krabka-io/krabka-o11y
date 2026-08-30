use super::{AttrValue, MatchCmp, MatchValue, present_value_matches, string_matches, int_matches, float_matches, bool_matches};

pub(crate) fn attr_matches(value: &AttrValue, op: MatchCmp, expected: &MatchValue) -> bool {
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    match value {
        AttrValue::Str(value) => string_matches(value, op, expected),
        AttrValue::Int(value) => int_matches(*value, op, expected),
        AttrValue::Float(value) => float_matches(*value, op, expected),
        AttrValue::Bool(value) => bool_matches(*value, op, expected),
    }
}
