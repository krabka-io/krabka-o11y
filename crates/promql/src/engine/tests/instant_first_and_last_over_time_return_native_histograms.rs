use super::*;

#[tokio::test]
pub(crate) async fn instant_first_and_last_over_time_return_native_histograms() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, sum) in [(60_000, 10.0), (120_000, 20.0)] {
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            ts_ms,
            native_histogram(4.0, sum),
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "histogram_sum(first_over_time(request_duration_seconds[2m]))",
            10.0,
        ),
        (
            "histogram_sum(last_over_time(request_duration_seconds[2m]))",
            20.0,
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 120_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1, "{query}");
        check!(samples[0].labels.get("__name__").is_none(), "{query}");
        check!(samples[0].labels.get("job") == Some("api"), "{query}");
        check!(
            approx_eq(float_value(&samples[0].value), expected),
            "{query}"
        );
    }
}
