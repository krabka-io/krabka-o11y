
/// The same smoothing with *distinct* factors. Every other test here passes
/// `0.5, 0.5`, where `factor` and `1.0 - factor` are the same number, so
/// swapping either one is invisible -- as is reversing which of the two the
/// value and the trend are weighted by. Four samples also run the trend update
/// twice, which a shorter series never reaches, and the pair case pins the
/// two-sample minimum.
#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_double_exponential_smoothing_weights_each_factor_distinctly() {
    let mut store = InMemoryMetricStore::new();
    // The initial trend is 2.0, not 1.0: at 1.0 the trend carry `(1 - tf) *
    // trend` equals `(1 - tf) / trend`, so that multiply would be free to be a
    // divide.
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 3.0), (120_000, 4.0), (180_000, 8.0)] {
        store.push_float("tenant-a", labels(&[("__name__", "g")]), ts_ms, value);
    }
    for (ts_ms, value) in [(120_000_i64, 1.0), (180_000, 2.0)] {
        store.push_float("tenant-a", labels(&[("__name__", "pair")]), ts_ms, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (query, want) in [
        ("double_exponential_smoothing(g[4m], 0.3, 0.4)", 7.006),
        // Two samples are the minimum the fold accepts.
        ("double_exponential_smoothing(pair[2m], 0.3, 0.4)", 2.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 180_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {query}");
        };
        assert2::assert!(samples.len() == 1, "{query}");
        assert2::assert!(approx_eq(float_value(&samples[0].value), want), "{query}");
    }
}
