use super::*;

#[tokio::test]
pub(crate) async fn instant_group_returns_one_for_each_group() {
    let mut store = InMemoryMetricStore::new();
    for (job, value) in [("api", 10.0), ("api", 30.0), ("web", 99.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", job)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "group by (job) (up)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 2);
    for sample in samples {
        assert2::assert!(sample.labels.get("__name__") == None);
        assert2::assert!(approx_eq(float_value(&sample.value), 1.0));
    }
}
