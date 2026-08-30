use super::*;

/// M16: `stdvar` and `stddev` must not cancel into a negative variance.
///
/// The group has a large offset and close values. A negative variance gives
/// NaN from `sqrt`. Welford returns the small positive population variance for
/// `{0,1,2}` -> 2/3.
#[tokio::test]
pub(crate) async fn instant_stdvar_aggregate_is_stable_for_large_offset_group() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [1e8, 1e8 + 1.0, 1e8 + 2.0].into_iter().enumerate() {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "big"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(stdvar) = engine
        .query_instant("tenant-a", "stdvar(big)", 10_000)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(stdvar.len() == 1);
    let value = float_value(&stdvar[0].value);
    check!(!value.is_nan(), "stdvar must be finite, got NaN");
    check!(value > 0.0, "stdvar must be positive, got {value}");
    check!(approx_eq(value, 2.0 / 3.0), "stdvar == 2/3, got {value}");
}
