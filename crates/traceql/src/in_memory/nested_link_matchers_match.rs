use super::{
    InputSpan, SpanMatcher, is_link_matcher, link_matcher_matches_absence,
    link_matcher_matches_link,
};

pub(crate) fn nested_link_matchers_match(span: &InputSpan, matchers: &[SpanMatcher]) -> bool {
    let link_matchers = matchers
        .iter()
        .filter(|matcher| is_link_matcher(matcher))
        .collect::<Vec<_>>();
    if link_matchers.is_empty() {
        return true;
    }
    if span.links.is_empty() {
        return link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_absence(matcher));
    }
    span.links.iter().any(|link| {
        link_matchers
            .iter()
            .all(|matcher| link_matcher_matches_link(link, matcher))
    })
}
