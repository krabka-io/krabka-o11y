use super::*;

pub(crate) fn api_prom_streams_only_response(value: &Value) -> Response {
    if value.pointer("/data/resultType").and_then(Value::as_str) == Some("streams") {
        json_response(StatusCode::OK, value)
    } else {
        text_response(
            StatusCode::BAD_REQUEST,
            "rpc error: code = Code(400) desc = legacy endpoints only support streams result type",
        )
    }
}
