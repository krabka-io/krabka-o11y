use super::*;

/// A series matching more than one `or` branch stays one series.
///
/// `label_matcher_sets` resolves `{a or b}` to one matcher set per branch, and
/// the operator leaf is filled branch by branch, so a series matching both
/// branches arrives twice. The leaf must still present it as a single
/// contiguous, time-ordered run, because `SeriesDivide` splits its input into
/// per-series batches by scanning for a change in the label columns and every
/// downstream operator assumes one batch per series.
#[tokio::test]
pub(crate) async fn a_range_query_over_or_matchers_counts_a_doubly_matched_series_once() {
    let mut store = InMemoryMetricStore::new();
    for ts_ms in [0_i64, 60_000, 120_000] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            ts_ms,
            2.0,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    // The one stored series matches both branches: its job is `api` and its
    // instance is `a`.
    let result = engine
        .query_range(
            &tenant_id("tenant-a"),
            r#"sum by (job) (up{job="api" or instance="a"})"#,
            0,
            120_000,
            millis(60_000),
        )
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected a range matrix");
    };
    check!(series.len() == 1);
    check!(series[0].labels.get("job") == Some("api"));
    check!(
        series[0]
            .samples
            .iter()
            .map(|(ts_ms, value)| (*ts_ms, float_value(value)))
            .collect::<Vec<_>>()
            == vec![(0, 2.0), (60_000, 2.0), (120_000, 2.0)]
    );
}
