use super::*;

pub(crate) fn success_response(result: QueryResult) -> Response {
    Json(json!({
        "status": "success",
        "data": result_json(result),
    }))
    .into_response()
}
