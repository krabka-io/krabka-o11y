use super::{IntoResponse, Response, StatusCode};

/// The ring status page, describing the one process that serves this role.
///
/// Krabka runs no hash ring: there is no gossip, no token ownership and no
/// second instance to hand a range to. The page stays because `Loki`'s is at
/// this path and an operator's runbook sends them here, but it says what is
/// true rather than rendering a one-member ring as though membership had been
/// checked. `state` comes from the caller's readiness, so a process that is
/// still starting reads `JOINING` here and 503 at `/ready`.
pub(crate) fn ring_status_page(instance: &str, state: &str) -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        format!(
            "<!doctype html><html><head><title>Ring Status</title></head>\
         <body><h1>Ring Status</h1>\
         <p>Krabka runs no hash ring. This process serves the role by itself, \
         so the row below is the process answering this request and not a \
         membership that was gossiped, tokenised or health-checked. There are \
         no peers to list, and none to have been forgotten.</p>\
         <table><thead><tr><th>Instance</th><th>State</th></tr></thead>\
         <tbody><tr><td>{instance}</td><td>{state}</td></tr></tbody>\
         </table></body></html>"
        ),
    )
        .into_response()
}
