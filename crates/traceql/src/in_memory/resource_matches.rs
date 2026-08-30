use super::{SpanMatcher, StoredTrace, nil_matches, string_matches};

pub(crate) fn resource_matches(trace: &StoredTrace, matcher: &SpanMatcher) -> bool {
    match matcher.key.as_str() {
        "service.name" => string_matches(&trace.root_service_name, matcher.op, &matcher.value),
        _ => nil_matches(matcher.op, &matcher.value),
    }
}
