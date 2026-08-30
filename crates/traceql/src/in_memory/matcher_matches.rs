use super::{
    InputSpan, MatchScope, NestedSet, SpanMatcher, StoredTrace, attr_values_match,
    instrumentation_matches, intrinsic_matches, resource_matches, span_attr_matches,
};

pub(crate) fn matcher_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matcher: &SpanMatcher,
) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => span.events.iter().any(|event| {
            let values = event
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }),
        MatchScope::Link => span.links.iter().any(|link| {
            let values = link
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }),
        MatchScope::Intrinsic => intrinsic_matches(trace, span, nested_sets, idx, matcher),
        MatchScope::Resource => resource_matches(trace, matcher),
        MatchScope::Instrumentation => instrumentation_matches(span, matcher),
        MatchScope::Both => {
            resource_matches(trace, matcher)
                || span_attr_matches(span, &matcher.key, matcher.op, &matcher.value)
        }
        MatchScope::Span => span_attr_matches(span, &matcher.key, matcher.op, &matcher.value),
        MatchScope::Parent => true,
    };
    is_match != matcher.negated
}
