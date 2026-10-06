use super::SpanMatcher;

/// Projection dependencies do not filter or expand event/link observations.
/// Pinned vParquet5 resolves the first fetched value for each dynamic field.
pub(crate) fn expansion_matchers(
    matchers: &[SpanMatcher],
    _projection_matchers: &[SpanMatcher],
) -> Vec<SpanMatcher> {
    matchers.to_vec()
}
