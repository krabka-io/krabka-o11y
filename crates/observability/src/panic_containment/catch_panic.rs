use std::panic::AssertUnwindSafe;

use axum::{extract::Request, middleware::Next};
use futures_util::FutureExt as _;

use super::{IntoResponse, Response, StatusCode, panic_message};

/// Runs the rest of the stack and answers 500 if it panics.
///
/// The panic hook has already run by the time the unwind reaches here, so the
/// backtrace is on stderr as usual; what this adds is a reply. `AssertUnwindSafe`
/// is the honest annotation: the request is consumed by the inner future and
/// the router is cloned per request, so nothing this frame owns is observed
/// after the unwind. What the *handler* touched is another matter, and
/// [`the module docs`](super) say what that costs.
pub(crate) async fn catch_panic(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    match AssertUnwindSafe(next.run(request)).catch_unwind().await {
        Ok(response) => response,
        Err(payload) => {
            tracing::error!(
                %method,
                %path,
                panic = panic_message(payload.as_ref()),
                "request handler panicked; answering 500"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, "internal server error\n").into_response()
        }
    }
}
