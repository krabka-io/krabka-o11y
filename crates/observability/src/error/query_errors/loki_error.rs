use super::*;

pub(crate) fn loki_error(status: StatusCode, error_type: &'static str, error: &str) -> Response {
    let value = json!({
        "status": "error",
        "errorType": error_type,
        "error": error,
        "data": null,
    });
    json_response(status, &value)
}
