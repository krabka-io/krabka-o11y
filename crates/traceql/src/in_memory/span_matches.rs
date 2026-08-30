use super::*;

pub(crate) fn span_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matchers: &[SpanMatcher],
) -> bool {
    if !nested_event_matchers_match(span, matchers) || !nested_link_matchers_match(span, matchers) {
        return false;
    }
    matchers
        .iter()
        .filter(|matcher| !is_event_matcher(matcher) && !is_link_matcher(matcher))
        .all(|matcher| matcher_matches(trace, span, nested_sets, idx, matcher))
}
