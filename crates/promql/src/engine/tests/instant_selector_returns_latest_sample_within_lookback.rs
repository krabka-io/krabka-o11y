use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_returns_latest_sample_within_lookback() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        20_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        40_000,
        4.0,
    );

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            lookback_delta: millis(15_000),
            max_samples: 100,
            ..EngineOpts::default()
        },
    );

    let result = engine
        .query_instant("tenant-a", "up", 30_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(
        (
            samples.len(),
            &samples[0].labels,
            samples[0].ts_ms,
            approx_eq(float_value(&samples[0].value), 2.0),
        ) == (
            1,
            &labels(&[("__name__", "up"), ("job", "api")]),
            20_000,
            true,
        )
    );
}
