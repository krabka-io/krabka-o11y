use super::*;

#[derive(bon::Builder)]
struct QueryErrorCase {
    query: &'static str,
    status: StatusCode,
    error_type: &'static str,
}

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
    for case in [
        QueryErrorCase::builder()
            .query(r#"count_values("\xff", up)"#)
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .error_type("execution")
            .build(),
        QueryErrorCase::builder()
            .query(r#"label_replace(up, "\xff", "", "src", "(.*)")"#)
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .error_type("execution")
            .build(),
        QueryErrorCase::builder()
            .query(r#"label_replace(up, "\q", "", "src", "(.*)")"#)
            .status(StatusCode::BAD_REQUEST)
            .error_type("bad_data")
            .build(),
    ] {
        let encoded =
            url::form_urlencoded::byte_serialize(case.query.as_bytes()).collect::<String>();
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
        assert2::assert!(response.status() == case.status, "{}", case.query);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(
            body["status"] == "error" && body["errorType"] == case.error_type,
            "{}: {body}",
            case.query
        );
    }
}
