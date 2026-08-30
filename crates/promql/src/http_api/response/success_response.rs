use super::{IntoResponse, Json, QueryResult, Response, json, result_json};

pub(crate) fn success_response(result: QueryResult) -> Response {
    Json(json!({
        "status": "success",
        "data": result_json(result),
    }))
    .into_response()
}
