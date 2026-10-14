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
