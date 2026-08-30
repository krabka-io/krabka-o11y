use super::*;

pub(crate) fn is_link_matcher(matcher: &SpanMatcher) -> bool {
    matcher.scope == MatchScope::Link
        || (matcher.scope == MatchScope::Intrinsic && matcher.key.starts_with("link:"))
}
