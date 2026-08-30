use super::*;

/// Prometheus extrapolates a counter out to the edges of the range, but only
/// as far as 1.1x the average sample spacing; past that it settles for half a
/// spacing. Each geometry below trips a different part of that: the first
/// clamps at the end and is cut short at the start by the counter's own zero
/// crossing, the second clamps at the start with a counter that never crosses
/// zero, and the third clamps at neither edge. A series that simply spans its
/// range never enters any of them.
#[tokio::test]
pub(crate) async fn increase_extrapolates_to_the_range_edges_by_the_sample_spacing() {
    let mut store = InMemoryMetricStore::new();
    for (name, points) in [
        (
            "ends_short",
            vec![
                (50_000_i64, 0.0),
                (60_000, 10.0),
                (70_000, 20.0),
                (80_000, 30.0),
            ],
        ),
        (
            "starts_late",
            vec![(70_000_i64, 5.0), (80_000, 15.0), (90_000, 25.0)],
        ),
        (
            "spans_range",
            vec![
                (45_000_i64, 2.0),
                (60_000, 4.0),
                (75_000, 6.0),
                (95_000, 10.0),
            ],
        ),
    ] {
        for (ts_ms, value) in points {
            store.push_float("tenant-a", labels(&[("__name__", name)]), ts_ms, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (query, want) in [
        // 20s to the range end is over 1.1x the 10s spacing, so it clamps to
        // half a spacing; the counter starts at zero, so the extrapolation
        // back to the range start is cut to the zero crossing instead.
        ("increase(ends_short[1m])", 35.0),
        // The mirror: 30s to the range start clamps, 10s to the end does not,
        // and a counter starting above zero leaves the start alone.
        ("increase(starts_late[1m])", 35.0),
        // Neither gap reaches the threshold, so both extrapolate in full.
        ("increase(spans_range[1m])", 9.6),
        // `rate` is that same extrapolation over the range in seconds.
        ("rate(spans_range[1m])", 0.16),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 100_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {query}");
        };
        assert2::assert!(samples.len() == 1, "{query}");
        assert2::assert!(approx_eq(float_value(&samples[0].value), want), "{query}");
    }
}
