use super::{
    EventRef, MatchScope, SpanMatcher, TimeExt as _, int_matches, matcher_attributes_match,
    nested_presence_matches, string_matches,
};

/// Whether an event matcher matches one event.
#[must_use]
pub fn event_matcher_matches_event(event: &EventRef, matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => matcher_attributes_match(&event.attributes, matcher),
        MatchScope::Intrinsic => match matcher.key.as_str() {
            "event:name" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| string_matches(&event.name, matcher.op, &matcher.value)),
            "event:timeSinceStart" => nested_presence_matches(true, matcher.op, &matcher.value)
                .unwrap_or_else(|| {
                    int_matches(
                        event.time_since_start.nanos_i64(),
                        matcher.op,
                        &matcher.value,
                    )
                }),
            _ => false,
        },
        _ => false,
    };
    is_match != matcher.negated
}
