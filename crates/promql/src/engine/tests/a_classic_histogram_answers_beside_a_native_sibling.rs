use super::*;

/// A classic histogram answers correctly alongside a native sibling under the
/// same name -- the mixed case, which every other classic test avoids by
/// keeping the two apart. The window bounds are the infinite ones, so the
/// buckets open at each end are the ones being read.
#[tokio::test]
pub(crate) async fn a_classic_histogram_answers_beside_a_native_sibling() {
    let mut store = InMemoryMetricStore::new();
    // A classic histogram closed at both ends by an infinity.
    for (le, count) in [("-1", 2.0), ("0", 3.0), ("1", 6.0), ("+Inf", 10.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "hk"), ("kind", "classic"), ("le", le)]),
            10_000,
            count,
        );
    }
    // The native sibling that sends the call to the kernel.
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "hk"), ("kind", "native")]),
        10_000,
        native_histogram(4.0, 8.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (query, want) in [
        // The bucket running from -Inf, and the one running to +Inf.
        ("histogram_fraction(-Inf, -1, hk)", 0.2),
        ("histogram_fraction(1, Inf, hk)", 0.4),
        ("histogram_fraction(0, 1, hk)", 0.3),
    ] {
        let QueryResult::InstantVector(samples) = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"))
        else {
            panic!("expected a vector for {query}");
        };
        let classic = samples
            .iter()
            .find(|sample| sample.labels.get("kind") == Some("classic"))
            .unwrap_or_else(|| panic!("{query}: no classic series in the result"));
        assert2::assert!(approx_eq(float_value(&classic.value), want), "{query}");
    }
}
