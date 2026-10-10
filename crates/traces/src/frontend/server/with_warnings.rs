use super::{IntoResponse, Json, Response, json};

/// Answer with `body`, carrying `warnings` only when there are any, so a
/// complete answer stays the body Tempo returns.
pub(crate) fn with_warnings(mut body: serde_json::Value, warnings: &[String]) -> Response {
    if !warnings.is_empty() {
        body["warnings"] = json!(warnings);
    }
    Json(body).into_response()
}
