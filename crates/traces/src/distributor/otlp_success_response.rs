use super::*;

pub(crate) fn otlp_success_response() -> Response {
    let body = ExportTraceServiceResponse {
        partial_success: None,
    }
    .encode_to_vec();
    ([(header::CONTENT_TYPE, "application/x-protobuf")], body).into_response()
}
