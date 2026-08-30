use super::{Response, StatusCode, json, json_response};

pub(crate) async fn build_info() -> Response {
    let value = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": "unknown",
        "branch": "unknown",
        "buildDate": "",
        "buildUser": "krabka",
        "goVersion": "not-go",
    });
    json_response(StatusCode::OK, &value)
}
