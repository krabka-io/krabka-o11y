use super::*;

pub(crate) fn span_attr_matches(span: &InputSpan, key: &str, op: MatchCmp, expected: &MatchValue) -> bool {
    let values = span
        .attrs
        .iter()
        .filter(|(attr_key, _)| attr_key == key)
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    attr_values_match(&values, op, expected)
}
