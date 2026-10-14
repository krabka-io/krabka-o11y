use super::{DistributorError, IntoResponse, Response, StatusCode, encode_otlp_status_message};

pub(crate) fn otlp_http_error_response(error: DistributorError) -> Response {
    if matches!(
        error,
        DistributorError::TimestampTooOld { .. } | DistributorError::TimestampTooNew { .. }
    ) {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("content-type", "application/x-protobuf")
            .body(encode_otlp_status_message(&error.to_string()).into())
            .expect("OTLP response headers are valid");
    }

    error.into_response()
}
