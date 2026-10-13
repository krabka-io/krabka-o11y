use super::*;

struct InfoSampleFixture {
    labels: crate::PromqlLabels,
    value: f64,
}

fn matching_metric_labels() -> Labels {
    labels(&[
        crate::test_support::TestLabelPair::new("__name__", "metric"),
        crate::test_support::TestLabelPair::new("instance", "a"),
        crate::test_support::TestLabelPair::new("job", "1"),
        crate::test_support::TestLabelPair::new("label", "value"),
    ])
}

/// Differential parity for `info(v [, data_label_selector])` in the planner.
///
/// The recursive planner recurses the input vector. It selects the
/// `target_info` series or the custom-selector series through the shared
/// interpreter helper, then applies the shared `apply_info` join. The result
/// must equal the interpreter's `eval_info_call` byte-for-byte.
#[tokio::test]
pub(crate) async fn info_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A store mirroring the conformance corpus: a base metric, a metric whose
    // identifying labels don't match any target_info, a metric with an
    // overlapping data label, plus `target_info` and `build_info` series.
    let mut store = InMemoryMetricStore::new();
    for fixture in [
        InfoSampleFixture {
            labels: matching_metric_labels().into(),
            value: 2.0,
        },
        InfoSampleFixture {
            labels: labels(&[
                crate::test_support::TestLabelPair::new(
                    "__name__",
                    "metric_not_matching_target_info",
                ),
                crate::test_support::TestLabelPair::new("instance", "a"),
                crate::test_support::TestLabelPair::new("job", "2"),
                crate::test_support::TestLabelPair::new("label", "value"),
            ])
            .into(),
            value: 2.0,
        },
        InfoSampleFixture {
            labels: labels(&[
                crate::test_support::TestLabelPair::new(
                    "__name__",
                    "metric_with_overlapping_label",
                ),
                crate::test_support::TestLabelPair::new("instance", "a"),
                crate::test_support::TestLabelPair::new("job", "1"),
                crate::test_support::TestLabelPair::new("label", "value"),
                crate::test_support::TestLabelPair::new("data", "base"),
            ])
            .into(),
            value: 2.0,
        },
        InfoSampleFixture {
            labels: labels(&[
                crate::test_support::TestLabelPair::new("__name__", "target_info"),
                crate::test_support::TestLabelPair::new("instance", "a"),
                crate::test_support::TestLabelPair::new("job", "1"),
                crate::test_support::TestLabelPair::new("data", "info"),
                crate::test_support::TestLabelPair::new("another_data", "another info"),
            ])
            .into(),
            value: 1.0,
        },
        InfoSampleFixture {
            labels: labels(&[
                crate::test_support::TestLabelPair::new("__name__", "build_info"),
                crate::test_support::TestLabelPair::new("instance", "a"),
                crate::test_support::TestLabelPair::new("job", "1"),
                crate::test_support::TestLabelPair::new("build_data", "build"),
            ])
            .into(),
            value: 1.0,
        },
    ] {
        store.push_float("t", fixture.labels, 600_000, fixture.value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter exactly: default target_info enrichment, single/all-label
    // restriction, non-matching identifying labels (passthrough), a required
    // matcher not matching empty (drop), overlapping-label passthrough, and
    // explicit `__name__` selectors (target_info / build_info / both /
    // non-existent), plus the input-as-bare-selector form.
    let queries = [
        "info(metric)",
        r#"info(metric, {data=~".+"})"#,
        "info(metric_not_matching_target_info)",
        r#"info(metric, {non_existent=~".+"})"#,
        r#"info(metric, {data=~".+", non_existent=~".*"})"#,
        "info(metric_with_overlapping_label)",
        r#"info(metric, {__name__="target_info"})"#,
        r#"info(metric, {__name__="non_existent"})"#,
        r#"info(metric, {__name__="build_info"})"#,
        r#"info(metric, {__name__=~".+_info"})"#,
        r#"info(build_info, {__name__=~".+_info", build_data=~".+"})"#,
        // Input as a bare brace-only selector.
        r#"info({job="1"}, {__name__="target_info"})"#,
    ];

    for query in queries {
        let parity = planned_and_interpreted(&engine, query, 600_000).await;
        let via_operators = parity.via_operators;
        let via_interpreter = parity.via_interpreter;

        let normalize = |result: QueryResult| fingerprint_sorted(result, query);

        let interpreter = normalize(via_interpreter);
        let operators = normalize(via_operators);
        assert2::assert!(instant_samples_match(&interpreter, &operators));
    }

    // A histogram info-series match errors identically (the info series must be
    // float-typed). Pin that the planner surfaces the same error class.
    let mut hist_store = InMemoryMetricStore::new();
    hist_store.push_float("t", matching_metric_labels(), 600_000, 2.0);
    hist_store.push_histogram(
        "t",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "hist"),
            crate::test_support::TestLabelPair::new("instance", "a"),
            crate::test_support::TestLabelPair::new("job", "1"),
        ]),
        600_000,
        native_histogram(4.0, 10.0),
    );
    let hist_engine = PromqlEngine::new(Arc::new(hist_store), EngineOpts::default());
    let hist_query = r#"info(metric, {__name__="hist"})"#;
    let hist_expr =
        parse_promql_with_duration_context(hist_query, DurationExprContext::instant(600_000))
            .unwrap();
    let operator_result = hist_engine
        .plan_instant_expr("t", &hist_expr, 600_000)
        .await;
    assert2::assert!(matches!(operator_result, Err(PromqlError::Exec(_))));
}
