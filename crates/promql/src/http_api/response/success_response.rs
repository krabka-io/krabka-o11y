use super::{json, QueryResult, Response, IntoResponse, Json, result_json};

pub(crate) fn success_response(result: QueryResult) -> Response {
    Json(json!({
        "status": "success",
        "data": result_json(result),
    }))
    .into_response()
}
