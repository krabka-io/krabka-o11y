use super::*;

pub(crate) fn event_matcher_matches_absence(matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => nil_matches(matcher.op, &matcher.value),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "event:name" | "event:timeSinceStart" => {
                nested_presence_matches(false, matcher.op, &matcher.value).unwrap_or(false)
            }
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}
