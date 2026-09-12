use super::*;

// Runs one query against `state` and returns the parsed response envelope with
// its status code.
pub(crate) async fn annotated_query_body<S: MetricStore + 'static>(
    state: Arc<PrometheusApiState<S>>,
    uri: &str,
) -> (StatusCode, serde_json::Value) {
    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
