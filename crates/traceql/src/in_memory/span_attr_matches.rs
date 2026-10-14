use super::{InputSpan, MatchCmp, MatchValue, attr_values_match};

pub(crate) fn span_attr_matches(
    span: &InputSpan,
    key: &str,
    op: MatchCmp,
    expected: &MatchValue,
) -> bool {
    let values = span
        .attrs
        .iter()
        .filter(|attribute| attribute.key == key)
        .map(|attribute| &attribute.value)
        .collect::<Vec<_>>();
    attr_values_match(&values, op, expected)
}
