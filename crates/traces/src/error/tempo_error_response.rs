use super::{IntoResponse, Json, Response, StatusCode, json};

#[must_use]
pub fn tempo_error_response(status: StatusCode, message: impl Into<String>) -> Response {
    let mut response = Json(json!({
        "status": "error",
        "error": message.into(),
    }))
    .into_response();
    *response.status_mut() = status;
    response
}
