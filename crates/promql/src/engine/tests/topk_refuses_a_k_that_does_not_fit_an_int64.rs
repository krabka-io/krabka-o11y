use super::*;

/// Prometheus bounds `k` to what an `int64` holds: `topk` refuses any parameter
/// greater than or equal to `maxInt64`, which is `2^63 - 1024` and the last
/// `f64` below `2^63`. A `usize` does not impose that bound on a 64-bit target
/// -- it reaches almost twice as far -- so the bound has to be spelled out.
#[tokio::test]
pub(crate) async fn topk_refuses_a_k_that_does_not_fit_an_int64() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 3.0), ("c", 2.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
            10_000,
            value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // `2^63` and the bound itself are both refused; `1e19` is the value that a
    // `u64` would have swallowed whole.
    for query in [
        "topk(1e19, memory_bytes)",
        "topk(9223372036854774784, memory_bytes)",
    ] {
        let error = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap_err();

        check!(matches!(error, PromqlError::Plan(_)), "{query}");
        check!(format!("{error}").contains("overflows int64"), "{query}");
    }

    // The next `f64` below the bound is `2^63 - 2048`, which is accepted and
    // selects everything.
    let result = engine
        .query_instant(
            "tenant-a",
            "topk(9223372036854773760, memory_bytes)",
            10_000,
        )
        .await
        .expect("a k just inside the int64 bound");
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected a vector");
    };
    let mut projection = samples
        .iter()
        .map(|sample| (sample.labels.get("instance"), float_value(&sample.value)))
        .collect::<Vec<_>>();
    projection.sort_by_key(|(instance, _)| *instance);
    check!(projection == vec![(Some("a"), 1.0), (Some("b"), 3.0), (Some("c"), 2.0)]);
}
