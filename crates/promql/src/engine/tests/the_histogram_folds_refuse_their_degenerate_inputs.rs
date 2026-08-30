use super::*;

/// The degenerate inputs to the histogram folds. A classic series without a
/// `+Inf` bucket has no total to divide by and yields NaN; two buckets are
/// enough to interpolate between; and a NaN bound, or a histogram holding
/// nothing, yields NaN rather than a number. Each guard below is a chain of
/// `||`, and no test had ever made exactly one of its clauses true -- which is
/// what tells the chain apart from the same clauses joined by `&&`.
#[tokio::test]
pub(crate) async fn the_histogram_folds_refuse_their_degenerate_inputs() {
    let mut store = InMemoryMetricStore::new();
    for (name, points) in [
        // Two buckets, closed with +Inf: the smallest series that interpolates.
        ("c2", vec![("1", 1.0), ("+Inf", 2.0)]),
        // No +Inf bucket at all, so there is no total.
        ("cf", vec![("1", 1.0), ("2", 2.0)]),
    ] {
        for (le, count) in points {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", name), ("le", le)]),
                10_000,
                count,
            );
        }
    }
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "nh")]),
        10_000,
        native_histogram(4.0, 8.0),
    );
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "nh0")]),
        10_000,
        native_histogram(0.0, 0.0),
    );
    // A negative count is the only way to tell the `count <= 0.0` clause from
    // the NaN one beside it: at exactly zero the fold divides zero by zero and
    // reaches NaN by itself.
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "nhneg")]),
        10_000,
        native_histogram(-1.0, 0.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let value = |query: &'static str| {
        let engine = &engine;
        async move {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected a vector for {query}");
            };
            assert2::assert!(samples.len() == 1, "{query}");
            float_value(&samples[0].value)
        }
    };

    // Two buckets are enough to interpolate: the rank lands on the first one's
    // upper bound.
    assert2::assert!(approx_eq(value("histogram_quantile(0.5, c2)").await, 1.0));

    for query in [
        // Without a +Inf bucket there is no total to divide by.
        "histogram_quantile(0.5, cf)",
        "histogram_fraction(0, 1, cf)",
        // A NaN bound poisons the fold, from either side, on either kind.
        "histogram_fraction(NaN, 1, c2)",
        "histogram_fraction(NaN, 1, nh)",
        "histogram_fraction(0, NaN, nh)",
        // A histogram holding nothing, or less than nothing, has no fraction.
        "histogram_fraction(0, 1, nh0)",
        "histogram_fraction(0, 1, nhneg)",
    ] {
        assert2::assert!(value(query).await.is_nan(), "{query}");
    }
}
