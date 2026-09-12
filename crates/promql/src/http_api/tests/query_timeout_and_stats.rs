use super::*;

async fn json(response: Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn timeout_covers_admission_wait_and_uses_prometheus_error_shape() {
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_max_concurrent_queries(1)
            .with_query_timeout(millis(20)),
    );
    let held = Arc::clone(state.query_gate.as_ref().unwrap())
        .acquire_owned()
        .await
        .unwrap();
    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&timeout=1h")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    drop(held);

    assert2::assert!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    let body = json(response).await;
    assert2::assert!(body["status"] == "error");
    assert2::assert!(body["errorType"] == "timeout");
}

#[tokio::test]
async fn request_timeout_applies_to_evaluation_for_get_and_post() {
    for post in [false, true] {
        let active = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(
            PrometheusApiState::new(
                Arc::new(SlowEmptyStore::new(
                    Arc::clone(&active),
                    Arc::new(AtomicUsize::new(0)),
                )),
                EngineOpts::default(),
            )
            .with_query_timeout(minutes(2)),
        );
        let request = if post {
            Request::post("/api/v1/query")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::from("query=up&timeout=10ms"))
                .unwrap()
        } else {
            Request::get("/api/v1/query?query=up&timeout=10ms")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap()
        };
        let response = prometheus_router(state).oneshot(request).await.unwrap();
        assert2::assert!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
        assert2::assert!(json(response).await["errorType"] == "timeout");
    }
}

#[tokio::test]
async fn stats_report_measured_samples_timings_and_per_step_counts() {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", "up");
    store.push_float("tenant-a", labels.clone(), 0, 1.0);
    store.push_float("tenant-a", labels, 1_000, 2.0);
    let router = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    )));

    let instant = router
        .clone()
        .oneshot(
            Request::get("/api/v1/query?query=up&time=1&stats=1")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(instant.status() == StatusCode::OK);
    let instant = json(instant).await;
    assert2::assert!(instant["data"]["stats"]["samples"]["totalQueryableSamples"] == 2);
    assert2::assert!(instant["data"]["stats"]["samples"]["peakSamples"] == 2);
    assert2::assert!(
        instant["data"]["stats"]["samples"]
            .get("totalQueryableSamplesPerStep")
            .is_none()
    );
    assert2::assert!(
        instant["data"]["stats"]["timings"]["execTotalTime"]
            .as_f64()
            .unwrap()
            > 0.0
    );

    let range = router
        .oneshot(
            Request::post("/api/v1/query_range")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::from("query=up&start=0&end=1&step=1&stats=all"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(range.status() == StatusCode::OK);
    let range = json(range).await;
    assert2::assert!(range["data"]["stats"]["samples"]["totalQueryableSamples"] == 3);
    assert2::assert!(range["data"]["stats"]["samples"]["peakSamples"] == 2);
    assert2::assert!(
        range["data"]["stats"]["samples"]["totalQueryableSamplesPerStep"]
            == serde_json::json!([[0.0, 1], [1.0, 2]])
    );
}

#[tokio::test]
async fn empty_stats_parameter_does_not_add_stats() {
    let response = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )))
    .oneshot(
        Request::get("/api/v1/query?query=1&time=0&stats=")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    let body = json(response).await;
    assert2::assert!(body["data"].get("stats").is_none());
}

#[tokio::test]
async fn stats_response_keeps_query_annotations() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(annotation_store()),
        EngineOpts::default(),
    ));
    let (status, body) = annotated_query_body(
        state,
        &format!(
            "{}&stats=1",
            annotation_query_uri("/api/v1/query", "histogram_quantile(0.5, up)")
        ),
    )
    .await;

    assert2::assert!(status == StatusCode::OK);
    assert2::assert!(body["data"]["stats"].is_object());
    assert2::assert!(body["warnings"].as_array().map(Vec::len) == Some(1));
}
