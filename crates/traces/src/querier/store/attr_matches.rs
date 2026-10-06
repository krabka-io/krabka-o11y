use super::{
    AttrValue, MatchCmp, MatchValue, bool_matches, float_matches, int_matches,
    present_value_matches, string_matches,
};

pub(crate) fn attr_matches(value: &AttrValue, op: MatchCmp, expected: &MatchValue) -> bool {
    if matches!(value, AttrValue::Unsupported(_)) {
        return false;
    }
    if let AttrValue::Array(values) = value {
        if values.is_empty() {
            return matches!((op, expected), (MatchCmp::Eq, MatchValue::Nil));
        }
        if values.len() == 1 {
            return attr_matches(&values[0], op, expected);
        }
    }
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    match value {
        AttrValue::Unsupported(_) => false,
        AttrValue::Array(values) => {
            if matches!(op, MatchCmp::Neq | MatchCmp::Nre) {
                values.iter().all(|value| attr_matches(value, op, expected))
            } else {
                values.iter().any(|value| attr_matches(value, op, expected))
            }
        }
        AttrValue::Str(value) => string_matches(value, op, expected),
        AttrValue::Int(value) => int_matches(*value, op, expected),
        AttrValue::Float(value) => float_matches(*value, op, expected),
        AttrValue::Bool(value) => bool_matches(*value, op, expected),
    }
}
