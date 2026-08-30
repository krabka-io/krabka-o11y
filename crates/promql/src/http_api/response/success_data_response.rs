use super::{IntoResponse, Json, Response, json};

pub(crate) fn success_data_response(data: impl serde::Serialize) -> Response {
    Json(json!({
        "status": "success",
        "data": data,
    }))
    .into_response()
}
