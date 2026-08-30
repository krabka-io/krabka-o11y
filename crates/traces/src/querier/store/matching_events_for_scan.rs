use super::{Array, EventRef, RecordBatch, SpanMatcher, TraceqlError, event_matcher_matches_event, event_values, is_event_matcher};

pub(crate) fn matching_events_for_scan(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<Vec<Option<EventRef>>, TraceqlError> {
    let event_matchers = matchers
        .iter()
        .filter(|matcher| is_event_matcher(matcher))
        .collect::<Vec<_>>();
    let events = event_values(batch, row)?;
    if event_matchers.is_empty() {
        return Ok(vec![events.into_iter().next()]);
    }
    if events.is_empty() {
        return Ok(vec![None]);
    }
    Ok(events
        .into_iter()
        .filter(|event| {
            event_matchers
                .iter()
                .all(|matcher| event_matcher_matches_event(event, matcher))
        })
        .map(Some)
        .collect())
}
