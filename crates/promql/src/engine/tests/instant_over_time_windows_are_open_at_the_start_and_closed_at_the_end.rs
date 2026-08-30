use super::*;

#[tokio::test]
pub(crate) async fn instant_over_time_windows_are_open_at_the_start_and_closed_at_the_end() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, sum) in [(0_i64, 1.0), (10_000, 2.0), (60_000, 3.0)] {
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            native_histogram(4.0, sum),
        );
    }

    // A `[50s]` range ending at 60s starts at exactly 10s. Prometheus' window
    // is half-open, so the sample sitting on that start is out and only the
    // one at 60s is in. This pins that boundary as engine behaviour rather
    // than as a property of whichever layer happens to apply the trim.
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("histogram_sum(first_over_time(queue_depth[50s]))", 3.0),
        ("histogram_sum(last_over_time(queue_depth[50s]))", 3.0),
        ("count_over_time(queue_depth[50s])", 1.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 60_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1, "{query}");
        check!(
            approx_eq(float_value(&samples[0].value), expected),
            "{query}: {}",
            float_value(&samples[0].value)
        );
    }
}
