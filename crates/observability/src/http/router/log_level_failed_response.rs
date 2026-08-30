use super::{Response, StatusCode, json, json_response};

pub(crate) fn log_level_failed_response(message: &str) -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        &json!({
            "status": "failed",
            "message": message,
        }),
    )
}
