use super::{RecordBatch, SpanMatcher, LinkRef, TraceqlError, is_link_matcher, link_values, Array, link_matcher_matches_link};

pub(crate) fn matching_links_for_scan(
    batch: &RecordBatch,
    row: usize,
    matchers: &[SpanMatcher],
) -> Result<Vec<Option<LinkRef>>, TraceqlError> {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    let links = link_values(batch, row)?;
    if link_matchers.is_empty() {
        return Ok(vec![links.into_iter().next()]);
    }
    if links.is_empty() {
        return Ok(vec![None]);
    }
    Ok(links
        .into_iter()
        .filter(|link| {
            link_matchers
                .iter()
                .all(|matcher| link_matcher_matches_link(link, matcher))
        })
        .map(Some)
        .collect())
}
