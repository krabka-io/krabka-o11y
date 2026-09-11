use super::*;

/// Prometheus ranks a `NaN` below every number under both operators. `topk`
/// and `bottomk` therefore select the `NaN` series only after they select
/// every number. `None` stands for the `NaN` sample in the table below,
/// because no float compares equal to a `NaN`.
#[tokio::test]
pub(crate) async fn instant_topk_and_bottomk_rank_nan_last() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("c", 2.0), ("n", f64::NAN)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("topk(1, memory_bytes)", vec![("c", Some(2.0))]),
        ("bottomk(1, memory_bytes)", vec![("a", Some(1.0))]),
        (
            "topk(2, memory_bytes)",
            vec![("c", Some(2.0)), ("a", Some(1.0))],
        ),
        (
            "bottomk(2, memory_bytes)",
            vec![("a", Some(1.0)), ("c", Some(2.0))],
        ),
        (
            "topk(3, memory_bytes)",
            vec![("c", Some(2.0)), ("a", Some(1.0)), ("n", None)],
        ),
        (
            "bottomk(3, memory_bytes)",
            vec![("a", Some(1.0)), ("c", Some(2.0)), ("n", None)],
        ),
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let selected = samples
            .iter()
            .map(|sample| {
                let value = float_value(&sample.value);
                (
                    sample.labels.get("instance").unwrap(),
                    (!value.is_nan()).then_some(value),
                )
            })
            .collect::<Vec<_>>();
        assert2::assert!(selected == expected);
    }
}
