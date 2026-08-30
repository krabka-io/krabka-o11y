use super::*;

/// `stdvar_over_time` runs a compensated Welford fold, and each of its three
/// compensations is added back rather than subtracted. The difference is in
/// the final bits, so this is compared bit-for-bit -- a relative tolerance
/// accepts all three of them running the wrong way.
#[tokio::test]
pub(crate) async fn stdvar_over_time_adds_back_each_of_its_compensations() {
    let mut store = InMemoryMetricStore::new();
    for (index, value) in [1.0_f64, 0.1, 1.0, 1.0].into_iter().enumerate() {
        let ts_ms = 10_000 + 10_000 * i64::try_from(index).expect("a small index");
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "v"), ("series", "f")]),
            ts_ms,
            value,
        );
    }
    // The histogram sibling routes the fold through the interpreter kernel's
    // own copy; without it the operator leaf's copy answers instead.
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "v"), ("series", "h")]),
        20_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(samples) = engine
        .query_instant("tenant-a", "stdvar_over_time(v[5m])", 40_000)
        .await
        .expect("a variance")
    else {
        panic!("expected a vector");
    };
    let float = samples
        .iter()
        .find(|sample| sample.labels.get("series") == Some("f"))
        .expect("a float series in the result");
    assert2::assert!(
        float_value(&float.value).to_bits() == 0.151_875_000_000_000_04_f64.to_bits(),
        "got {}",
        float_value(&float.value)
    );
}
