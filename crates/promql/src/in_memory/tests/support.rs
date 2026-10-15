//! Shared test helpers for this test area.
use super::*;

pub(crate) mod assert_query_shard_selects {
    use super::*;

    /// Checks the `series` an `__query_shard__` matcher with `op` and `1_of_2`
    /// selects out of twelve `up` series: `=` keeps the fingerprints that are even
    /// (shard 1 of 2) and `!=` keeps the odd ones.
    pub(crate) async fn assert_query_shard_selects(op: MatchOp) {
        let mut store = InMemoryMetricStore::new();
        let series = (0..12)
            .map(|id| {
                lbls(&[
                    crate::test_support::TestLabelPair::new("__name__", "up"),
                    crate::test_support::TestLabelPair::new("series", &id.to_string()),
                ])
            })
            .collect::<Vec<_>>();
        for labels in &series {
            store.push_float("t", labels.clone(), 1, 1.0);
        }

        let keeps_even = op == MatchOp::Eq;
        let mut expected_by_fingerprint = BTreeMap::new();
        for labels in &series {
            if labels.fingerprint().is_multiple_of(2) == keeps_even {
                expected_by_fingerprint.insert(labels.fingerprint(), labels.clone());
            }
        }
        let expected = expected_by_fingerprint.into_values().collect::<Vec<_>>();
        assert2::assert!(!expected.is_empty());
        assert2::assert!(expected.len() < series.len());

        let matchers = [
            LabelMatcher::new("__name__", MatchOp::Eq, "up"),
            LabelMatcher::new("__query_shard__", op, "1_of_2"),
        ];
        let got = store.series("t", &matchers, 0, 10).await.unwrap();

        assert2::assert!(got == expected);
    }
}

pub(crate) mod lbls {
    use super::*;
    use crate::test_support::TestLabelPair;

    pub(crate) fn lbls(pairs: &[TestLabelPair<'_>]) -> Labels {
        let mut labels = Labels::new();
        for pair in pairs {
            labels.insert(pair.name, pair.value);
        }
        labels
    }
}

pub(crate) mod store_with_float_and_hist_series {
    use super::*;

    #[derive(bon::Builder)]
    pub(crate) struct FloatAndHistogramStoreFixture {
        pub(crate) store: InMemoryMetricStore,
        pub(crate) up_api: Labels,
    }

    pub(crate) fn store_with_float_and_hist_series() -> FloatAndHistogramStoreFixture {
        let mut store = InMemoryMetricStore::new();
        let up_api = lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        let up_worker = lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "worker"),
        ]);
        let latency = lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        store.push_float("tenant-a", up_api.clone(), 1_000, 1.0);
        store.push_float("tenant-a", up_worker, 2_000, 2.0);
        store.push_histogram("tenant-a", latency, 3_000, count_two_sum_three_histogram());
        FloatAndHistogramStoreFixture::builder()
            .store(store)
            .up_api(up_api)
            .build()
    }
}
