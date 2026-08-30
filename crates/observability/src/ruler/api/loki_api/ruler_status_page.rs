use super::*;

pub(crate) fn ruler_status_page() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        "<!doctype html><html><head><title>Cortex Ruler Status</title></head>\
         <body><h1>Cortex Ruler Status</h1></body></html>",
    )
        .into_response()
}
