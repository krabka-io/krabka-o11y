use super::{Labels, PreparedMatcher, SeriesFingerprint};

pub(crate) fn all_match(
    fp: SeriesFingerprint,
    labels: &Labels,
    matchers: &[PreparedMatcher],
) -> bool {
    for matcher in matchers {
        if !matcher.matches(fp, labels) {
            return false;
        }
    }
    true
}
