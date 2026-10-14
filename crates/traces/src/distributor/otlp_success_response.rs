use super::*;

pub(crate) fn otlp_success_response() -> Response {
    let body = ExportTraceServiceResponse {
        partial_success: None,
    }
    .encode_to_vec();
    Response::builder()
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(axum::body::Body::from(body))
        .expect("the HTTP response builds")
}
