use super::*;

/// Runs an instant query for `tenant-a` and returns its vector samples.
pub(crate) async fn instant_vector<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    time_ms: i64,
) -> Vec<crate::InstantSample> {
    let query_result = engine
        .query_instant(&tenant_id("tenant-a"), query, time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = query_result else {
        panic!("expected vector");
    };
    samples
}

/// Checks `query` at 10s for `tenant-a`: exactly one sample, approximately
/// `want`.
pub(crate) async fn assert_lone_value<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    query: &str,
    want: f64,
) {
    let query_result = engine
        .query_instant(&tenant_id("tenant-a"), query, 10_000)
        .await
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let QueryResult::InstantVector(samples) = query_result else {
        panic!("expected a vector for {query}");
    };
    assert2::assert!(samples.len() == 1, "{query}");
    assert2::assert!(approx_eq(float_value(&samples[0].value), want), "{query}");
}
