use super::*;

struct TimestampMatchCase {
    timestamp_ms: i64,
    expected: bool,
}

#[test]
pub(crate) fn row_matches_rejects_outside_bounds_before_matching_labels() {
    let labels = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let matchers =
        prepare_matchers(&[LabelMatcher::new("__name__", MatchOp::Eq, "up")]).expect("matchers");
    let fp = labels.fingerprint();

    for case in [
        TimestampMatchCase {
            timestamp_ms: 999,
            expected: false,
        },
        TimestampMatchCase {
            timestamp_ms: 1_000,
            expected: true,
        },
        TimestampMatchCase {
            timestamp_ms: 2_000,
            expected: true,
        },
        TimestampMatchCase {
            timestamp_ms: 2_001,
            expected: false,
        },
    ] {
        assert2::assert!(
            row_matches(fp, &labels, case.timestamp_ms, &matchers, 1_000, 2_000) == case.expected
        );
    }

    let mismatch =
        prepare_matchers(&[LabelMatcher::new("job", MatchOp::Eq, "worker")]).expect("matchers");
    assert2::assert!(!row_matches(fp, &labels, 1_500, &mismatch, 1_000, 2_000));
}
