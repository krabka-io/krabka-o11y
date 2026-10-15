use super::*;

mod scan_filters_by_matcher_and_time_and_registers_float_table {
    use super::*;

    #[tokio::test]
    pub(crate) async fn scan_filters_by_matcher_and_time_and_registers_float_table() {
        let mut store = InMemoryMetricStore::new();
        push_up_sample(&mut store, "a", 1000, 1.0);
        push_up_sample(&mut store, "a", 2000, 1.0);
        push_up_sample(&mut store, "b", 1000, 0.0);
        store.push_float(
            "t",
            lbls(&[crate::test_support::TestLabelPair::new("__name__", "down")]),
            1000,
            9.0,
        );

        let matchers = [
            LabelMatcher::new("__name__", MatchOp::Eq, "up"),
            LabelMatcher::new("job", MatchOp::Eq, "a"),
        ];
        let result = store.scan("t", &matchers, 0, 1500).await.unwrap();
        let table = result.float_table.clone().unwrap();
        assert2::assert!(result.histogram_table.is_none());
        assert2::assert!(count_rows(&result, &table).await == 1);
    }
}

mod scan_filters_histograms_by_matcher_tenant_and_time {
    use super::*;

    #[tokio::test]
    pub(crate) async fn scan_filters_histograms_by_matcher_tenant_and_time() {
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "t",
            lbls(&[
                crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            1_000,
            count_two_sum_three_histogram(),
        );
        store.push_histogram(
            "t",
            lbls(&[
                crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            5_000,
            count_two_sum_three_histogram(),
        );
        store.push_histogram(
            "t",
            lbls(&[
                crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
                crate::test_support::TestLabelPair::new("job", "worker"),
            ]),
            1_000,
            count_two_sum_three_histogram(),
        );
        store.push_histogram(
            "other",
            lbls(&[
                crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            1_000,
            count_two_sum_three_histogram(),
        );

        let matchers = [
            LabelMatcher::new("__name__", MatchOp::Eq, "latency_seconds"),
            LabelMatcher::new("job", MatchOp::Eq, "api"),
        ];
        let result = store.scan("t", &matchers, 0, 1_500).await.unwrap();
        assert2::assert!(result.float_table.is_none());
        let table = result.histogram_table.clone().unwrap();
        assert2::assert!(count_rows(&result, &table).await == 1);
    }
}

mod scan_validates_regex_matchers_before_row_iteration {
    use super::*;

    #[tokio::test]
    pub(crate) async fn scan_validates_regex_matchers_before_row_iteration() {
        let store = InMemoryMetricStore::new();
        let matchers = [LabelMatcher::new("__name__", MatchOp::Re, "[")];

        let Err(error) = store.scan("missing", &matchers, 0, 5000).await else {
            panic!("expected invalid regex to fail before scanning rows");
        };

        assert2::assert!(matches!(error, PromqlError::Plan(_)));
        assert2::assert!(error.to_string().contains("bad regex"));
    }
}

mod scan_with_no_match_returns_none_tables {
    use super::*;

    #[tokio::test]
    pub(crate) async fn scan_with_no_match_returns_none_tables() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "t",
            lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
            1000,
            1.0,
        );
        let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "absent")];
        let result = store.scan("t", &matchers, 0, 5000).await.unwrap();
        assert2::assert!(result.float_table.is_none());
        assert2::assert!(result.histogram_table.is_none());
    }
}

mod label_values_returns_distinct_for_name {
    use super::*;

    #[tokio::test]
    pub(crate) async fn label_values_returns_distinct_for_name() {
        let mut store = InMemoryMetricStore::new();
        push_up_sample(&mut store, "a", 1, 1.0);
        push_up_sample(&mut store, "b", 1, 1.0);
        let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
        let values = store
            .label_values("t", "job", &matchers, 0, 10)
            .await
            .unwrap();
        assert2::assert!(values == vec!["a".to_string(), "b".to_string()]);
    }
}

mod series_filters_histograms_by_matcher_and_time {
    use super::*;

    #[tokio::test]
    pub(crate) async fn series_filters_histograms_by_matcher_and_time() {
        let mut store = InMemoryMetricStore::new();
        let api = lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        let worker = lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
            crate::test_support::TestLabelPair::new("job", "worker"),
        ]);
        store.push_histogram("t", api.clone(), 1_000, count_two_sum_three_histogram());
        store.push_histogram("t", api.clone(), 5_000, count_two_sum_three_histogram());
        store.push_histogram("t", worker, 1_000, count_two_sum_three_histogram());

        let matchers = [
            LabelMatcher::new("__name__", MatchOp::Eq, "latency_seconds"),
            LabelMatcher::new("job", MatchOp::Eq, "api"),
        ];
        let series = store.series("t", &matchers, 0, 1_500).await.unwrap();
        assert2::assert!(series == vec![api]);
    }
}
