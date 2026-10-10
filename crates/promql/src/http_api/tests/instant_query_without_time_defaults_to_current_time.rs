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

    let (status, body) = annotated_query_body(state, "/api/v1/query?query=up").await;

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
