use super::*;

/// Past the last sample, `smoothed` extrapolates only while the gap stays
/// within 1.1x the sample interval, and clamps to the last value beyond that.
/// Both sides of that threshold have to be pinned: a test on either one alone
/// leaves the slack factor free to move.
#[tokio::test]
pub(crate) async fn smoothed_delta_extrapolates_only_within_the_sample_interval_slack() {
    let mut store = InMemoryMetricStore::new();
    for (ts, value) in [(0, 0.0), (60_000, 60.0)] {
        store.push_float("tenant-a", labels(&[("__name__", "m")]), ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // The interval is 60s, so the slack runs to 66s past the last sample.
    for (case, eval_ms, want) in [
        ("63s past the last sample extrapolates", 123_000, 120.0),
        // Exactly 66s -- 1.1 times the 60s interval -- is not past it. This
        // pair is what separates `>` from `>=`, and the two cases either side
        // of it above cannot.
        (
            "66s past the last sample is exactly the slack",
            126_000,
            120.0,
        ),
        ("70s past it clamps to the last value", 130_000, 50.0),
    ] {
        let result = engine
            .query_instant("tenant-a", "delta(smoothed(m[2m]))", eval_ms)
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
        .query_instant("tenant-a", "rate(smoothed(m[3m]))", 123_000)
        .await
        .expect("a smoothed rate")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(approx_eq(
        float_value(&samples[0].value),
        0.683_333_333_333_333_3
    ));
}
