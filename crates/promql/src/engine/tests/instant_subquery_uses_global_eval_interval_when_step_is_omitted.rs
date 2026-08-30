use super::*;

#[tokio::test]
pub(crate) async fn instant_subquery_uses_global_eval_interval_when_step_is_omitted() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (30_000, 2.0), (60_000, 3.0), (90_000, 4.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            eval_interval: millis(30_000),
            ..EngineOpts::default()
        },
    );
    let result = engine
        .query_instant("tenant-a", "queue_depth[90s:]", 90_000)
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    let timestamps = series[0]
        .samples
        .iter()
        .map(|(ts_ms, _)| *ts_ms)
        .collect::<Vec<_>>();
    check!(timestamps == [0, 30_000, 60_000, 90_000]);
}
