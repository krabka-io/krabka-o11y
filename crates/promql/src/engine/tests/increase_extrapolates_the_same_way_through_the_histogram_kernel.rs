use super::*;

/// The same three geometries through the *other* extrapolation implementation.
///
/// `rate`/`increase`/`delta` lower onto a float-only operator leaf, which
/// cannot carry native histograms, so a selector that matches even one
/// histogram series routes the whole call through the interpreter kernel
/// instead -- a separate, byte-for-byte port of the same arithmetic. A float
/// series with a histogram sibling under its own name is the only shape that
/// reaches it, and without one the kernel's copy is never executed at all.
#[tokio::test]
pub(crate) async fn increase_extrapolates_the_same_way_through_the_histogram_kernel() {
    let mut store = InMemoryMetricStore::new();
    for (name, points) in [
        (
            "k_ends_short",
            vec![
                (50_000_i64, 0.0),
                (60_000, 10.0),
                (70_000, 20.0),
                (80_000, 30.0),
            ],
        ),
        (
            "k_starts_late",
            vec![(70_000_i64, 5.0), (80_000, 15.0), (90_000, 25.0)],
        ),
        (
            "k_spans_range",
            vec![
                (45_000_i64, 2.0),
                (60_000, 4.0),
                (75_000, 6.0),
                (95_000, 10.0),
            ],
        ),
        // 11.05s to the range end sits between 1.1x the 10s spacing (11.0)
        // and that spacing plus 1.1 (11.1), so the threshold has to be a
        // product rather than a sum for the end to clamp at all.
        (
            "k_thr_edge",
            vec![(68_950_i64, 0.0), (78_950, 10.0), (88_950, 20.0)],
        ),
    ] {
        for (ts_ms, value) in points {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", name), ("series", "f")]),
                ts_ms,
                value,
            );
        }
        // One histogram sibling is enough to route the call to the kernel.
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", name), ("series", "h")]),
            60_000,
            native_histogram(4.0, 10.0),
        );
    }
    // A float series whose only sample falls before the window contributes
    // nothing to the result.
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "k_no_points"), ("series", "f")]),
        10_000,
        1.0,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "k_no_points"), ("series", "h")]),
        60_000,
        native_histogram(4.0, 10.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (query, want) in [
        ("increase(k_ends_short[1m])", 35.0),
        ("increase(k_starts_late[1m])", 35.0),
        ("increase(k_spans_range[1m])", 9.6),
        ("rate(k_spans_range[1m])", 0.16),
        // `delta` is not a counter, so the zero-crossing cut never applies and
        // the start clamp stands on its own -- and the whole guard has to be
        // an `&&` chain, since under `||` a non-counter would take the cut.
        ("delta(k_thr_edge[1m])", 30.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 100_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {query}");
        };
        let float = samples
            .iter()
            .find(|sample| sample.labels.get("series") == Some("f"))
            .unwrap_or_else(|| panic!("{query}: no float series in the result"));
        assert2::assert!(approx_eq(float_value(&float.value), want), "{query}");
    }

    let QueryResult::InstantVector(samples) = engine
        .query_instant("tenant-a", "increase(k_no_points[1m])", 100_000)
        .await
        .expect("a window with no float points is not an error")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(
        !samples
            .iter()
            .any(|sample| sample.labels.get("series") == Some("f")),
        "a float series with no points in the window yields no sample"
    );
}
