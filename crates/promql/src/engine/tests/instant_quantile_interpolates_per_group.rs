use super::*;

#[tokio::test]
pub(crate) async fn instant_quantile_interpolates_per_group() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [1.0, 2.0, 4.0, 8.0].into_iter().enumerate() {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "latency_seconds"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "quantile by (job) (0.5, latency_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 3.0));
}
