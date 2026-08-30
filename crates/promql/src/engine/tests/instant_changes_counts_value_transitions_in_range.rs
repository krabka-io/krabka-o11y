use super::*;

#[tokio::test]
pub(crate) async fn instant_changes_counts_value_transitions_in_range() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 1.0),
        (60_000, 1.0),
        (120_000, 2.0),
        (180_000, 2.0),
        (240_000, 5.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "changes(queue_depth[4m])", 240_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 2.0));
}
