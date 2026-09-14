use super::{IntoResponse, Json, Response, StatusCode, json};

pub(crate) fn accepted_response() -> Response {
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "status": "success",
            "data": null,
            "errorType": "",
            "error": "",
        })),
    )
        .into_response()
}
