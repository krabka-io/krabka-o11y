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
    assert_lone_value_at(
        engine,
        LoneValueQuery {
            query,
            time_ms: 10_000,
            want,
        },
    )
    .await;
}

/// An instant query for `tenant-a` and the one float it should answer.
pub(crate) struct LoneValueQuery<'a> {
    pub(crate) query: &'a str,
    pub(crate) time_ms: i64,
    pub(crate) want: f64,
}

/// Checks `lone.query` at `lone.time_ms` for `tenant-a`: exactly one sample,
/// approximately `lone.want`.
pub(crate) async fn assert_lone_value_at<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    lone: LoneValueQuery<'_>,
) {
    lone_sample_at(engine, lone).await;
}

/// Checks `lone.query` at `lone.time_ms` for `tenant-a`: exactly one sample,
/// without any labels, approximately `lone.want`.
pub(crate) async fn assert_lone_unlabeled_value_at<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    lone: LoneValueQuery<'_>,
) {
    let query = lone.query;
    let sample = lone_sample_at(engine, lone).await;
    assert2::assert!(sample.labels.is_empty(), "{query}");
}

async fn lone_sample_at<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    lone: LoneValueQuery<'_>,
) -> crate::InstantSample {
    let LoneValueQuery {
        query,
        time_ms,
        want,
    } = lone;
    let query_result = engine
        .query_instant(&tenant_id("tenant-a"), query, time_ms)
        .await
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let QueryResult::InstantVector(mut samples) = query_result else {
        panic!("expected a vector for {query}");
    };
    assert2::assert!(samples.len() == 1, "{query}");
    let sample = samples.remove(0);
    assert2::assert!(approx_eq(float_value(&sample.value), want), "{query}");
    sample
}
