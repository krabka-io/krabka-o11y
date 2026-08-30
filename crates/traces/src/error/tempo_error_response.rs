use super::*;

#[must_use]
pub fn tempo_error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({
            "status": "error",
            "error": message.into(),
        })),
    )
        .into_response()
}
