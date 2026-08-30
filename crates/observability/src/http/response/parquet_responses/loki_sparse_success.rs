use super::*;

pub(crate) fn loki_sparse_success() -> Response {
    json_response(StatusCode::OK, &json!({ "status": "success" }))
}
