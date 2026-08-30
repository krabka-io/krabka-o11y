use super::*;

#[test]
pub(crate) fn row_matches_rejects_outside_bounds_before_matching_labels() {
    let labels = lbls(&[("__name__", "up"), ("job", "api")]);
    let matchers =
        prepare_matchers(&[LabelMatcher::new("__name__", MatchOp::Eq, "up")]).expect("matchers");
    let fp = labels.fingerprint();

    for (ts_ms, want) in [(999, false), (1_000, true), (2_000, true), (2_001, false)] {
        assert2::assert!(row_matches(fp, &labels, ts_ms, &matchers, 1_000, 2_000) == want);
    }

    let mismatch =
        prepare_matchers(&[LabelMatcher::new("job", MatchOp::Eq, "worker")]).expect("matchers");
    assert2::assert!(!row_matches(fp, &labels, 1_500, &mismatch, 1_000, 2_000));
}
