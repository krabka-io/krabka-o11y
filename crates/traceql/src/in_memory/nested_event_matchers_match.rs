use super::*;

pub(crate) fn nested_event_matchers_match(span: &InputSpan, matchers: &[SpanMatcher]) -> bool {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    if event_matchers.is_empty() {
        return true;
    }
    if span.events.is_empty() {
        return event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_absence(matcher));
    }
    span.events.iter().any(|event| {
        event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_event(event, matcher))
    })
}
