use super::*;

fn search_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", "request_duration_seconds");
    labels.insert("job", "api");
    store.push_histogram(
        "tenant-a",
        labels,
        1_000,
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 4.0,
            sum: 10.0,
            positive_spans: vec![krabka_metrics::BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![1.0, 3.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        },
    );
    store.push_metadata(
        "tenant-a",
        "request_duration_seconds",
        "histogram",
        "Request latency",
        "seconds",
    );
    store
}

#[tokio::test]
async fn search_routes_stream_batched_ndjson_with_metadata() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(search_store()),
        EngineOpts::default(),
    ));
    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/search/metric_names?start=0&end=2&search[]=request&include_score=true&include_metadata=true&batch_size=1")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::OK);
    assert2::assert!(response.headers()["content-type"] == "application/x-ndjson; charset=utf-8");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let lines = std::str::from_utf8(&body)
        .unwrap()
        .lines()
        .collect::<Vec<_>>();
    assert2::assert!(lines.len() == 2);
    let results: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert2::check!(results["results"][0]["name"] == "request_duration_seconds");
    assert2::check!(results["results"][0]["type"] == "histogram");
    assert2::check!(results["results"][0]["unit"] == "seconds");
    assert2::check!(results["results"][0]["score"] == 1.0);
    let trailer: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert2::check!(trailer == serde_json::json!({"status":"success","has_more":false}));
}

#[tokio::test]
async fn native_histogram_cardinality_reports_latest_bucket_counts() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(search_store()),
        EngineOpts::default(),
    ));
    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/active_native_histogram_metrics")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert2::check!(body["data"][0]["metric"] == "request_duration_seconds");
    assert2::check!(body["data"][0]["series_count"] == 1);
    assert2::check!(body["data"][0]["bucket_count"] == 2);
    assert2::check!(body["data"][0]["avg_bucket_count"] == 2.0);
    assert2::check!(body["data"][0]["min_bucket_count"] == 2);
    assert2::check!(body["data"][0]["max_bucket_count"] == 2);
}

#[tokio::test]
async fn native_histogram_cardinality_uses_the_latest_sample_type() {
    let mut store = search_store();
    let mut labels = Labels::new();
    labels.insert("__name__", "request_duration_seconds");
    labels.insert("job", "api");
    store.push_float("tenant-a", labels, 2_000, 1.0);
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/active_native_histogram_metrics")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert2::check!(body["data"] == serde_json::json!([]));
}
