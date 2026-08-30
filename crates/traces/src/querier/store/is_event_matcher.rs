use super::{SpanMatcher, MatchScope};

pub(crate) fn is_event_matcher(matcher: &SpanMatcher) -> bool {
    matcher.scope == MatchScope::Event
        || (matcher.scope == MatchScope::Intrinsic && matcher.key.starts_with("event:"))
}
