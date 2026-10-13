use super::{Json, Response, StatusCode, json, response_with_status};

pub(crate) fn accepted_response() -> Response {
    response_with_status(
        StatusCode::ACCEPTED,
        Json(json!({
            "status": "success",
            "data": null,
            "errorType": "",
            "error": "",
        })),
    )
}
