use super::{Response, json, success_data_response};

pub(crate) async fn build_info() -> Response {
    success_data_response(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": "",
        "branch": "",
        "buildUser": "",
        "buildDate": "",
        "goVersion": "",
    }))
}
