use super::{EventRef, SpanMatcher, MatchScope, attr_values_match, nested_presence_matches, string_matches, int_matches, TimeExt};

pub(crate) fn event_matcher_matches_event(event: &EventRef, matcher: &SpanMatcher) -> bool {
    let is_match = match matcher.scope {
        MatchScope::Event => {
            let values = event
                .attributes
                .iter()
                .filter(|(key, _)| key == &matcher.key)
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            attr_values_match(&values, matcher.op, &matcher.value)
        }
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
