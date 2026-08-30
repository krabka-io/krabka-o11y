use super::{InputSpan, LinkRef, SpanMatcher, is_link_matcher, link_matcher_matches_link};

pub(crate) fn matching_links_for_scan<'a>(
    span: &'a InputSpan,
    matchers: &[SpanMatcher],
) -> Vec<Option<&'a LinkRef>> {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    if link_matchers.is_empty() {
        return vec![span.links.first()];
    }
    if span.links.is_empty() {
        return vec![None];
    }
    span.links
        .iter()
        .filter(|link| {
            link_matchers
                .iter()
                .all(|matcher| link_matcher_matches_link(link, matcher))
        })
        .map(Some)
        .collect()
}
