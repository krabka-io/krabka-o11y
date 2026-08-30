use super::*;

pub(crate) fn loki_format_query_invalid_response(status: StatusCode, error: &str) -> Response {
    let error = serde_json::to_string(error).expect("string serialization cannot fail");
    (
        status,
        [("content-type", "application/json")],
        format!("{{\"status\":\"invalid-query\",\"error\":{error}}}\n"),
    )
        .into_response()
}
