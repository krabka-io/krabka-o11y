use super::{RecordBatch, SpanMatcher, TraceqlError, is_event_matcher, is_link_matcher, nested_event_matchers_match, nested_link_matchers_match, row_matcher_matches};

pub(crate) fn row_matches(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<bool, TraceqlError> {
    if !nested_event_matchers_match(batch, row, matchers)?
        || !nested_link_matchers_match(batch, row, matchers)?
    {
        return Ok(false);
    }
    matchers.iter().try_fold(true, |matched, matcher| {
        if !matched {
            return Ok(false);
        }
        if is_event_matcher(matcher) || is_link_matcher(matcher) {
            return Ok(true);
        }
        row_matcher_matches(batch, row, matcher)
    })
}
