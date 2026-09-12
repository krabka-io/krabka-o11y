use super::{IntoResponse, Json, Response, json};

pub(crate) async fn buildinfo() -> Response {
    Json(json!({
        "status": "success",
        "data": {
            "version": "2.6.0",
            "revision": "krabka",
            "branch": "main",
            "buildUser": "krabka",
            "buildDate": "",
            "goVersion": "",
        },
    }))
    .into_response()
}
