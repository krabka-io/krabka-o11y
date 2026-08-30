use super::*;

/// M17: `avg` of large-magnitude samples must not overflow the running sum.
///
/// The running sum must not reach +Inf or -Inf. The incremental Kahan mean
/// stays finite and equals the common value for two equal maxima.
#[tokio::test]
pub(crate) async fn instant_avg_aggregate_does_not_overflow() {
    let mut store = InMemoryMetricStore::new();
    for instance in 0..2 {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "huge"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            f64::MAX,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(avg) = engine
        .query_instant("tenant-a", "avg(huge)", 10_000)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(avg.len() == 1);
    let value = float_value(&avg[0].value);
    assert2::assert!(value.is_finite());
    assert2::assert!(approx_eq(value, f64::MAX));
}
