use super::{AttrValue, MatchCmp, MatchValue, attr_matches, nil_matches, present_value_matches};

pub(crate) fn attr_values_match(
    values: &[&AttrValue],
    op: MatchCmp,
    expected: &MatchValue,
) -> bool {
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
