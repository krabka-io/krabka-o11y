use super::SpanMatcher;

pub(crate) fn is_event_matcher(matcher: &SpanMatcher) -> bool {
    krabka_traceql::scope_is_event_matcher(matcher.scope, &matcher.key)
}
