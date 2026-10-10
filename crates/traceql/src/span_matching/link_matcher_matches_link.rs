use super::{
    LinkRef, MatchScope, SpanMatcher, bytes_to_hex, matcher_attributes_match,
    nested_presence_matches, string_matches,
};

/// Whether a link matcher matches one link.
#[must_use]
pub fn link_matcher_matches_link(link: &LinkRef, matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Link => matcher_attributes_match(&link.attributes, matcher),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "link:traceID" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    string_matches(&bytes_to_hex(&link.trace_id), matcher.op, &matcher.value)
                }),
            "link:spanID" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    string_matches(&bytes_to_hex(&link.span_id), matcher.op, &matcher.value)
                }),
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}
