//! `POST /log_level` moves the filter, and the emitted lines say so.
//!
//! The endpoint used to validate the level and reply `success` without
//! touching anything, so the one page that could have told an operator their
//! `debug` had not taken agreed with them that it had. This suite drives the
//! route over HTTP and then reads what the subscriber actually wrote, because
//! the response body is exactly the part that was never the problem.
//!
//! It is its own test binary: the control it installs is the process's, and a
//! process has one.

use std::{
    io::Write,
    sync::{Arc, Mutex},
};

use assert2::check;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_observability::{
    InMemoryWalSink, LogLevelControl, distributor_router, json_logging_layer,
};
use tower::ServiceExt as _;
use tracing_subscriber::{
    Registry, fmt::MakeWriter, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

/// A writer that keeps every line the subscriber emitted.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("captured lines").clone()).expect("utf-8 log lines")
    }
}

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("captured lines")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for Captured {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

async fn body_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("collect body");
    String::from_utf8(bytes.to_vec()).expect("utf-8 body")
}

struct PageResponse {
    status: StatusCode,
    body: String,
}

async fn page(
    app: &axum::Router,
    method: &str,
    uri: &str,
    form_body: Option<&str>,
) -> PageResponse {
    let mut request = Request::builder().method(method).uri(uri);
    if form_body.is_some() {
        request = request.header("content-type", "application/x-www-form-urlencoded");
    }
    let body = form_body.map_or_else(Body::empty, |form| Body::from(form.to_owned()));
    let response = app
        .clone()
        .oneshot(request.body(body).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let body = body_text(response).await;
    PageResponse { status, body }
}

#[tokio::test]
async fn the_log_level_endpoint_reports_and_moves_the_process_filter() {
    let captured = Captured::default();
    // `RUST_LOG` would outrank the default filter, and this suite is about
    // what the endpoint does to that filter, not what the shell did before.
    temp_env::with_var("RUST_LOG", None::<&str>, || {
        let logging = json_logging_layer("info", captured.clone());
        Registry::default().with(logging.layer).init();
        LogLevelControl::install_process(logging.control);
    });

    let app = distributor_router(InMemoryWalSink::default());

    // The level the process started at, reported rather than assumed.
    let response = page(&app, "GET", "/log_level", None).await;
    check!(response.status == StatusCode::OK);
    check!(response.body.contains("Current log level is info"));

    tracing::debug!("before-the-change");
    check!(!captured.text().contains("before-the-change"));

    // A level in the query string.
    let response = page(&app, "POST", "/log_level?log_level=debug", None).await;
    check!(response.status == StatusCode::OK);
    check!(response.body == r#"{"status":"success","message":"Log level set to debug"}"#);

    tracing::debug!("after-the-change");
    check!(captured.text().contains("after-the-change"));

    let response = page(&app, "GET", "/log_level", None).await;
    check!(response.body.contains("Current log level is debug"));

    // A level in a form body.
    let response = page(&app, "POST", "/log_level", Some("log_level=warn")).await;
    check!(response.status == StatusCode::OK);
    check!(response.body == r#"{"status":"success","message":"Log level set to warn"}"#);

    tracing::debug!("after-the-form-body");
    check!(!captured.text().contains("after-the-form-body"));

    // A form body outranks the query string, as Loki's does -- and the level
    // that wins is the one the filter ends up at, not only the one the reply
    // names.
    let response = page(
        &app,
        "POST",
        "/log_level?log_level=debug",
        Some("log_level=error"),
    )
    .await;
    check!(response.status == StatusCode::OK);
    check!(response.body == r#"{"status":"success","message":"Log level set to error"}"#);

    tracing::warn!("after-the-body-won");
    check!(!captured.text().contains("after-the-body-won"));
    tracing::error!("errors-still-pass");
    check!(captured.text().contains("errors-still-pass"));
}
