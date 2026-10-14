use super::{Response, StatusCode};

pub(crate) fn loki_format_query_invalid_response(status: StatusCode, error: &str) -> Response {
    let error = serde_json::to_string(error).expect("string serialization cannot fail");
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(format!("{{\"status\":\"invalid-query\",\"error\":{error}}}\n").into())
        .expect("JSON response headers are valid")
}
