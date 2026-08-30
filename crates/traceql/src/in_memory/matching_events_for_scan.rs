use super::*;

pub(crate) fn matching_events_for_scan<'a>(
    span: &'a InputSpan,
    matchers: &[SpanMatcher],
) -> Vec<Option<&'a EventRef>> {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    if event_matchers.is_empty() {
        return vec![span.events.first()];
    }
    if span.events.is_empty() {
        return vec![None];
    }
    span.events
        .iter()
        .filter(|event| {
            event_matchers
                .iter()
                .all(|matcher| event_matcher_matches_event(event, matcher))
        })
        .map(Some)
        .collect()
}
