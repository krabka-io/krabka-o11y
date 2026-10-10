use super::{AttrValue, MatchCmp, MatchValue, attr_matches, nil_matches, present_value_matches};

/// Whether the values one key holds satisfy `op` against `expected`.
///
/// With no comparable value the key is absent, and only `= nil` matches.
#[must_use]
pub fn attr_values_match(values: &[&AttrValue], op: MatchCmp, expected: &MatchValue) -> bool {
    let values = values
        .iter()
        .filter_map(|value| match value {
            AttrValue::Unsupported(_) => None,
            AttrValue::Array(values) if values.is_empty() => None,
            AttrValue::Array(values) if values.len() == 1 => values.first(),
            value => Some(*value),
        })
        .collect::<Vec<_>>();
    if values.is_empty() {
        return nil_matches(op, expected);
    }
    if let Some(matches) = present_value_matches(op, expected) {
        return matches;
    }
    match op {
        MatchCmp::Neq | MatchCmp::Nre => {
            values.iter().all(|value| attr_matches(value, op, expected))
        }
        MatchCmp::Eq
        | MatchCmp::Re
        | MatchCmp::Lt
        | MatchCmp::Lte
        | MatchCmp::Gt
        | MatchCmp::Gte => values.iter().any(|value| attr_matches(value, op, expected)),
    }
}
