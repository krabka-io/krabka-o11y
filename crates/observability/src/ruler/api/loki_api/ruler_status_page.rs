use super::{Response, StatusCode};

pub(crate) fn ruler_status_page() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/html; charset=utf-8")
        .body(
            "<!doctype html><html><head><title>Cortex Ruler Status</title></head>\
             <body><h1>Cortex Ruler Status</h1></body></html>"
                .into(),
        )
        .expect("ruler status page headers are valid")
}
