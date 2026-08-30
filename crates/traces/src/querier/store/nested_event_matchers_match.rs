use super::{RecordBatch, SpanMatcher, TraceqlError, is_event_matcher, Array, event_values, event_matcher_matches_absence, event_matcher_matches_event};

pub(crate) fn nested_event_matchers_match(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<bool, TraceqlError> {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    if event_matchers.is_empty() {
        return Ok(true);
    }
    let events = event_values(batch, row)?;
    if events.is_empty() {
        return Ok(event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_absence(matcher)));
    }
    Ok(events.iter().any(|event| {
        event_matchers
            .iter()
            .all(|matcher| event_matcher_matches_event(event, matcher))
    }))
}
