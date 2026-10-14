use super::SpanMatcher;

pub(crate) fn is_link_matcher(matcher: &SpanMatcher) -> bool {
    krabka_traceql::scope_is_link_matcher(matcher.scope, &matcher.key)
}
