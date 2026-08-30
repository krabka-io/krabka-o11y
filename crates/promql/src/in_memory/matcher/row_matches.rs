use super::*;

pub(crate) fn row_matches(
    fp: SeriesFingerprint,
    labels: &Labels,
    ts_ms: i64,
    matchers: &[PreparedMatcher],
    start_ms: i64,
    end_ms: i64,
) -> bool {
    if ts_ms.cmp(&start_ms).is_lt() {
        return false;
    }
    if ts_ms.cmp(&end_ms).is_gt() {
        return false;
    }
    all_match(fp, labels, matchers)
}
