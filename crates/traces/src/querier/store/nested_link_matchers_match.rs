use super::{Array, RecordBatch, SpanMatcher, TraceqlError, is_link_matcher, link_matcher_matches_absence, link_matcher_matches_link, link_values};

pub(crate) fn nested_link_matchers_match(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<bool, TraceqlError> {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    if link_matchers.is_empty() {
        return Ok(true);
    }
    let links = link_values(batch, row)?;
    if links.is_empty() {
        return Ok(link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_absence(matcher)));
    }
    Ok(links.iter().any(|link| {
        link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_link(link, matcher))
    }))
}
