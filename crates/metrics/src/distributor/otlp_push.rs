use prost::Message as _;

use super::*;

pub(crate) async fn otlp_push(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let started = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    let tenant = authorized_tenant_from_headers(&headers, &principal);
    // ONE ingest span per OTLP HTTP push request; series recorded post-decode.
    let span = ingest_span(tenant.as_ref().ok(), body_size);
    let result = async { otlp_push_inner(&state, &tenant?, &headers, &body).await }
        .instrument(span)
        .await;
    if let Some(metrics) = &state.metrics {
        match &result {
            Ok((_, items, _)) => {
                metrics.record_ingest(true, body_size, *items, started.elapsed().as_time());
            }
            Err(_) => metrics.record_ingest(false, body_size, 0, started.elapsed().as_time()),
        }
    }
    match result {
        Ok((success, _items, partial_success)) => {
            let mut response = success.into_response();
            let body = ExportMetricsServiceResponse {
                partial_success: partial_success.map(|partial| ExportMetricsPartialSuccess {
                    rejected_data_points: i64::try_from(partial.rejected_data_points)
                        .unwrap_or(i64::MAX),
                    error_message: partial.error_message,
                }),
            }
            .encode_to_vec();
            *response.body_mut() = axum::body::Body::from(body.clone());
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            );
            response.headers_mut().insert(
                axum::http::header::CONTENT_LENGTH,
                HeaderValue::from_str(&body.len().to_string())
                    .expect("a response body length is an HTTP header value"),
            );
            response.headers_mut().insert(
                "x-content-type-options",
                HeaderValue::from_static("nosniff"),
            );
            response
        }
        Err(error) => error.into_response(),
    }
}
