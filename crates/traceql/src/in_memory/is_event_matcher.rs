use super::SpanMatcher;

pub(crate) fn is_event_matcher(matcher: &SpanMatcher) -> bool {
    crate::scope_is_event_matcher(matcher.scope, &matcher.key)
}
