use super::*;

#[tokio::test]
pub(crate) async fn range_rate_uses_each_step_as_window_end() {
    let store = SeriesFixture::new(labels(&[
        ("__name__", "http_requests_total"),
        ("job", "api"),
    ]))
    .at(0_i64, 0.0)
    .at(60_000, 1.0)
    .at(120_000, 2.0)
    .at(180_000, 3.0)
    .at(240_000, 4.0)
    .at(300_000, 5.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_range(
            &tenant_id("tenant-a"),
            "rate(http_requests_total[5m])",
            240_000,
            300_000,
            millis(60_000),
        )
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    check!(series[0].samples.len() == 2);
    for (sample, (want_ts, want)) in series[0]
        .samples
        .iter()
        .zip([(240_000, 4.0 / 300.0), (300_000, 5.0 / 300.0)])
    {
        check!(sample.0 == want_ts);
        check!(approx_eq(float_value(&sample.1), want), "at ts {want_ts}");
    }
}
