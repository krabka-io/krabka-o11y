use super::*;


mod instant_query_without_time_defaults_to_current_time {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn instant_query_without_time_defaults_to_current_time() {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", "up");
    labels.insert("job", "api");
    store.push_float("tenant-a", labels, unix_now_ms().unwrap(), 1.0);

    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));

    let response = annotated_query_body(state, "/api/v1/query?query=up").await;
    let status = response.status;
    let body = response.body;

    assert2::assert!(status == StatusCode::OK);
    assert2::assert!(body["status"].as_str() == Some("success"));
    first_vector_sample::check_first_vector_sample(
        &body,
        &first_vector_sample::ExpectedVectorSample {
            job: "api",
            value: "1",
        },
    );
}
}


mod query_handlers_respect_configured_concurrency_limit {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn query_handlers_respect_configured_concurrency_limit() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(
        PrometheusApiState::new(
            Arc::new(SlowEmptyStore::new(
                Arc::clone(&active),
                Arc::clone(&max_active),
            )),
            EngineOpts::default(),
        )
        .with_max_concurrent_queries(1),
    );
    let router = prometheus_router(state);

    let one = router.clone().oneshot(
        Request::builder()
            .uri("/api/v1/query?query=up")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::empty())
            .unwrap(),
    );
    let two = router.oneshot(
        Request::builder()
            .uri("/api/v1/query?query=up")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::empty())
            .unwrap(),
    );

    let joined = tokio::join!(one, two);
    let one = joined.0;
    let two = joined.1;
    assert2::assert!(one.unwrap().status() == StatusCode::OK);
    assert2::assert!(two.unwrap().status() == StatusCode::OK);
    assert2::assert!(max_active.load(Ordering::SeqCst) == 1);
}
}


mod query_range_rejects_resolution_over_point_cap_without_limits {
use super::*;
use super::*;

#[tokio::test]
pub(crate) async fn query_range_rejects_resolution_over_point_cap_without_limits() {
    // No per-tenant query_limits configured: Prometheus enforces the
    // 11000-point resolution cap unconditionally. start=0 end=20000 step=1s
    // => 20000 intervals > 11000.
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=0&end=20000&step=1")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(
        body["error"].as_str()
            == Some(
                "exceeded maximum resolution of 11,000 points per timeseries. \
                 Try decreasing the query resolution (?step=XX)"
            )
    );
}
}
