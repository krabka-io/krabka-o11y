#[allow(dead_code)]
pub(crate) async fn body(response: axum::response::Response, context: &str) -> serde_json::Value {
    assert!(
        response.status().is_success(),
        "{context}: {}",
        response.status()
    );
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap_or_else(|error| panic!("{context} body: {error}"));
    serde_json::from_slice(&bytes).unwrap_or_else(|error| panic!("{context} json: {error}"))
}
