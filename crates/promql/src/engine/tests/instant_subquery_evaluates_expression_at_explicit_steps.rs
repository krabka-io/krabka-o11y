use super::*;

#[tokio::test]
pub(crate) async fn instant_subquery_evaluates_expression_at_explicit_steps() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 2.0), (120_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "(queue_depth * 2)[2m:1m]", 120_000)
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    check!(series[0].labels.get("__name__").is_none());
    check!(series[0].labels.get("job") == Some("api"));
    check!(series[0].samples.len() == 3);
    for (sample, (want_ts, want)) in
        series[0]
            .samples
            .iter()
            .zip([(0, 2.0), (60_000, 4.0), (120_000, 6.0)])
    {
        check!(sample.0 == want_ts);
        check!(approx_eq(float_value(&sample.1), want), "at ts {want_ts}");
    }
}
