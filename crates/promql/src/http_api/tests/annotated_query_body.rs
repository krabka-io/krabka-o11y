use super::*;

pub(crate) struct ApiResponseBody {
    pub(crate) status: StatusCode,
    pub(crate) body: serde_json::Value,
}

// Runs one query against `state` and returns the parsed response envelope with
// its status code.
pub(crate) async fn annotated_query_body<S: MetricStore + 'static>(
    state: Arc<PrometheusApiState<S>>,
    uri: &str,
) -> ApiResponseBody {
    org_query_body(
        state,
        OrgRequest {
            uri,
            org_id: "tenant-a",
        },
    )
    .await
}

/// A GET of `uri` sent with `org_id` as its `X-Scope-OrgID` header.
pub(crate) struct OrgRequest<'a> {
    pub(crate) uri: &'a str,
    pub(crate) org_id: &'a str,
}

// Runs one request against `state` and returns the parsed response envelope
// with its status code.
pub(crate) async fn org_query_body<S: MetricStore + 'static>(
    state: Arc<PrometheusApiState<S>>,
    request: OrgRequest<'_>,
) -> ApiResponseBody {
    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri(request.uri)
                .header("x-scope-orgid", request.org_id)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    ApiResponseBody {
        status,
        body: serde_json::from_slice(&body).unwrap(),
    }
}
