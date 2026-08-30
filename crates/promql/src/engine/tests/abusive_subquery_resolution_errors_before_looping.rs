use super::*;

/// C2 engine backstop: an abusive subquery resolution returns an error.
///
/// The range driver's `check_resolution_points` guard returns the error. The
/// driver does not loop about 1e11 times.
#[tokio::test]
pub(crate) async fn abusive_subquery_resolution_errors_before_looping() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", labels(&[("__name__", "up")]), 0, 1.0);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // `last_over_time(up[1000d:1ms])` would walk ~8.6e10 sub-steps; the
    // backstop rejects it with the resolution error instead.
    let err = engine
        .query_instant("t", "last_over_time(up[1000d:1ms])", 0)
        .await
        .expect_err("abusive subquery resolution must error");
    assert2::assert!(err.to_string().contains("exceeded maximum resolution"));
}
