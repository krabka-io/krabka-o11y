use super::*;

pub(crate) fn ring_status_page(instance: &'static str) -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        format!(
            "<!doctype html><html><head><title>Ring Status</title></head>\
         <body><h1>Ring Status</h1>\
         <table><thead><tr><th>Instance</th><th>State</th></tr></thead>\
         <tbody><tr><td>{instance}</td><td>ACTIVE</td></tr></tbody>\
         </table></body></html>"
        ),
    )
        .into_response()
}
