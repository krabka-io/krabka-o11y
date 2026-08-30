use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_stale_marker_terminates_series_before_lookback_expiry() {
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
        stale_nan(),
    );

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            lookback_delta: millis(60_000),
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
    assert2::assert!(samples.is_empty());
}
