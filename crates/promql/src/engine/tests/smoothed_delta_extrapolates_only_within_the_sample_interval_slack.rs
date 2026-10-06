use super::*;

/// Smoothed ranges interpolate between observed samples and carry the last
/// observed value at the right boundary. They do not extend the last slope.
#[tokio::test]
pub(crate) async fn smoothed_delta_uses_the_observed_right_boundary() {
    let mut store = InMemoryMetricStore::new();
    for (ts, value) in [(0, 0.0), (60_000, 60.0)] {
        store.push_float("tenant-a", labels(&[("__name__", "m")]), ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (case, eval_ms, want) in [
        ("start between samples at 3s", 123_000, 57.0),
        ("start between samples at 6s", 126_000, 54.0),
        ("start between samples at 10s", 130_000, 50.0),
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), "delta(smoothed(m[2m]))", eval_ms)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {case}");
        };
        assert2::assert!(samples.len() == 1, "{case}");
        assert2::assert!(approx_eq(float_value(&samples[0].value), want), "{case}");
    }

    // `delta` stops at the difference; only `rate` divides by the range.
    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), "rate(smoothed(m[3m]))", 123_000)
        .await
        .expect("a smoothed rate")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0 / 3.0));
}
