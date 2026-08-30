use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_at_and_offset_combine_order_independently() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        120_000,
        2.0,
    );

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            lookback_delta: millis(30_000),
            max_samples: 100,
            ..EngineOpts::default()
        },
    );
    for query in ["up @ 120 offset 1m", "up offset 1m @ 120"] {
        let result = engine
            .query_instant("tenant-a", query, 999_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1);
        check!(samples[0].ts_ms == 60_000);
        check!(approx_eq(float_value(&samples[0].value), 1.0));
    }
}
