use super::{
    InputSpan, MatchScope, NestedSet, SpanMatcher, StoredTrace, instrumentation_matches,
    intrinsic_matches, matcher_attributes_match, resource_matches, span_attr_matches,
};

pub(crate) fn matcher_matches(
    trace: &StoredTrace,
    span: &InputSpan,
    nested_sets: &[NestedSet],
    idx: usize,
    matcher: &SpanMatcher,
) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => span
            .events
            .iter()
            .any(|event| matcher_attributes_match(&event.attributes, matcher)),
        MatchScope::Link => span
            .links
            .iter()
            .any(|link| matcher_attributes_match(&link.attributes, matcher)),
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
