//! What `/log_level` answers in a process whose filter cannot be moved.
//!
//! `krabka-telemetry` installs the subscriber itself and hands back no reload
//! handle, so a role exporting over OTLP has a filter that is fixed at
//! start-up. The route has to say so. A `200 {"status":"success"}` there is
//! worse than a 404: an operator who sets `debug`, sees no new lines, and
//! concludes the fault is elsewhere has been sent the wrong way by the thing
//! they were debugging with.
//!
//! It is its own test binary, and it has to be. The control under test is the
//! *process's*, a process has one, and `tests/log_level.rs` is the process
//! that installs a reloadable one.

use assert2::check;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_observability::{InMemoryWalSink, LogLevelControl, distributor_router};
use tower::ServiceExt as _;

async fn page(app: &axum::Router, method: &str, uri: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("collect body");
    (
        status,
        String::from_utf8(bytes.to_vec()).expect("utf-8 body"),
    )
}

#[tokio::test]
async fn a_process_with_a_fixed_filter_refuses_the_change_rather_than_reporting_one() {
    let installed = LogLevelControl::install_process(LogLevelControl::fixed("info"));
    check!(
        !installed.is_reloadable(),
        "this binary installs no subscriber, so nothing else can have claimed the control"
    );

    let app = distributor_router(InMemoryWalSink::default());

    // The level is still reported, and reported truthfully.
    let (status, body) = page(&app, "GET", "/log_level").await;
    check!(status == StatusCode::OK);
    check!(body.contains("Current log level is info"));

    // The change is refused, with a status a client can branch on and a
    // message that names what does set this process's level.
    let (status, body) = page(&app, "POST", "/log_level?log_level=debug").await;
    check!(status == StatusCode::NOT_IMPLEMENTED);
    check!(body.contains(r#""status":"failed""#));
    check!(
        body.contains("RUST_LOG"),
        "the refusal names the variable that does set the level: {body}"
    );

    // And nothing moved: the report after the refusal is the report before it.
    let (status, body) = page(&app, "GET", "/log_level").await;
    check!(status == StatusCode::OK);
    check!(body.contains("Current log level is info"));
}
