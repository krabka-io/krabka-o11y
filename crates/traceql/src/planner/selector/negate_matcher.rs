use super::SpanMatcher;

pub(crate) fn negate_matcher(mut matcher: SpanMatcher) -> SpanMatcher {
    matcher.negated = !matcher.negated;
    matcher
}
