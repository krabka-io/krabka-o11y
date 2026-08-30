use super::{DistributorError, IntoResponse, Response, StatusCode, encode_otlp_status_message};

pub(crate) fn otlp_http_error_response(error: DistributorError) -> Response {
    if matches!(
        error,
        DistributorError::TimestampTooOld { .. } | DistributorError::TimestampTooNew { .. }
    ) {
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/x-protobuf")],
            encode_otlp_status_message(&error.to_string()),
        )
            .into_response();
    }

    error.into_response()
}
