use super::*;

pub(crate) async fn log_level() -> Response {
    json_response(
        StatusCode::OK,
        &json!({ "message": "Current log level is info" }),
    )
}
