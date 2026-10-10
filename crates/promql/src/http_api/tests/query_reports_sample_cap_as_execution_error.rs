use super::*;

#[tokio::test]
pub(crate) async fn query_reports_sample_cap_as_execution_error() {
    let limits = Limits {
        max_samples_per_query: 1,
        ..Limits::default()
    };
    let response = limited_get(two_series_store(), limits, "/api/v1/query?query=up&time=0").await;

    assert_execution_error(
        &response,
        |error| error == "samples per query exceeded: observed 2 above limit 1",
        "query",
    );
}

#[tokio::test]
async fn byte_label_names_fail_as_execution_errors_and_malformed_escapes_fail_as_syntax() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(two_series_store()),
        EngineOpts::default(),
    ));
    for (query, status, error_type) in [
        (
            r#"count_values("\xff", up)"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
        ),
        (
            r#"label_replace(up, "\xff", "", "src", "(.*)")"#,
            StatusCode::UNPROCESSABLE_ENTITY,
            "execution",
        ),
        (
            r#"label_replace(up, "\q", "", "src", "(.*)")"#,
            StatusCode::BAD_REQUEST,
            "bad_data",
        ),
    ] {
        let encoded = url::form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>();
        let response = prometheus_router(Arc::clone(&state))
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/query?query={encoded}&time=0"))
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status() == status, "{query}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(
            body["status"] == "error" && body["errorType"] == error_type,
            "{query}: {body}"
        );
    }
}
