use super::{
    ByteSizeExt, Bytes, CONTENT_ENCODING, CONTENT_TYPE, DistributorState, HeaderMap,
    RequestSecurity, Response, State, TenantErrorSurface, normalize_otlp_http_logs,
    otlp_http_error_response, push_logs::push_logs_with_normalizer,
};

pub(crate) async fn push_otlp_logs(
    State(state): State<DistributorState>,
    security: RequestSecurity,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    push_logs_with_normalizer(
        state,
        security,
        headers,
        body,
        TenantErrorSurface::OtlpPush,
        normalize_otlp_http_logs,
        |error, headers, body_size| {
            tracing::debug!(
                error = %error,
                content_type = ?headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok()),
                content_encoding = ?headers.get(CONTENT_ENCODING).and_then(|value| value.to_str().ok()),
                bytes = body_size.bytes_u64(),
                "OTLP logs: decode/normalize rejected the request"
            );
            otlp_http_error_response(error)
        },
        |error| tracing::debug!(error = %error, "OTLP logs: WAL append failed"),
    )
    .await
}
