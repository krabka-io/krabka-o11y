use super::*;

#[tokio::test]
pub(crate) async fn instant_stddev_and_stdvar_aggregate_population_variance() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        .into_iter()
        .enumerate()
    {
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
    let stdvar = engine
        .query_instant("tenant-a", "stdvar(latency_seconds)", 10_000)
        .await
        .unwrap();
    let stddev = engine
        .query_instant("tenant-a", "stddev(latency_seconds)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(stdvar_samples) = stdvar else {
        panic!("expected vector");
    };
    let QueryResult::InstantVector(stddev_samples) = stddev else {
        panic!("expected vector");
    };
    check!(stdvar_samples.len() == 1);
    check!(stddev_samples.len() == 1);
    check!(approx_eq(float_value(&stdvar_samples[0].value), 4.0));
    check!(approx_eq(float_value(&stddev_samples[0].value), 2.0));
}
