use super::*;

#[tokio::test]
pub(crate) async fn instant_subquery_uses_global_eval_interval_when_step_is_omitted() {
    let store = SeriesFixture::new(labels(&[("__name__", "queue_depth"), ("job", "api")]))
        .at(0_i64, 1.0)
        .at(30_000, 2.0)
        .at(60_000, 3.0)
        .at(90_000, 4.0)
        .store();

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            eval_interval: millis(30_000),
            ..EngineOpts::default()
        },
    );
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "queue_depth[90s:]", 90_000)
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
