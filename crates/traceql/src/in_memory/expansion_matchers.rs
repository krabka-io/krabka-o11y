use super::*;

pub(crate) fn expansion_matchers(
    matchers: &[SpanMatcher],
    projection_matchers: &[SpanMatcher],
) -> Vec<SpanMatcher> {
    let mut out = Vec::with_capacity(matchers.len() + projection_matchers.len());
    out.extend_from_slice(matchers);
    out.extend_from_slice(projection_matchers);
    out
}
