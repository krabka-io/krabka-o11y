//! Where a panic is one request's problem, and where it is the role's.
//!
//! The two halves of this milestone pull in opposite directions, and the
//! boundary between them is a listener.
//!
//! **Inside a request, contain it.** A handler that panics on one malformed
//! body has found a bug in that code path, not a reason to drop the
//! connection and leave the client guessing. Without containment the panic
//! unwinds through hyper's per-connection task and the socket closes with no
//! status at all, which a client reports as a transport error and a dashboard
//! reports as nothing. [`contain_handler_panics`] turns it into a 500, so the
//! request fails the way every other failure does and the connection survives
//! for the next one.
//!
//! **Inside a background loop, do not.** A WAL consumer or an index refresher
//! that panics leaves a role that still listens, still answers, and answers
//! from a tier that stopped advancing. There is nothing to return a 500 to.
//! That case belongs to
//! [`SupervisedTasks`](crate::SupervisedTasks), which cancels the role and
//! lets the process exit.
//!
//! Containment is not repair. A caught panic leaves whatever the handler was
//! part-way through exactly as it was, so shared state on the request path has
//! to be written such that a panic cannot leave it half-updated -- build the
//! new value privately and publish it in one move -- or else stay visibly
//! broken. A `std::sync` lock poisons on an unwind while it is held
//! exclusively, and that poison is the signal; clearing it without the
//! publish-on-success discipline behind it converts a loud fault into a silent
//! one, which is the failure mode this milestone exists to remove.

use std::{any::Any, panic::AssertUnwindSafe};

use axum::{extract::Request, middleware::Next};
use futures_util::FutureExt as _;

use crate::{Response, Router, StatusCode};

mod panic_safe_shared;
#[cfg(test)]
mod tests;

pub use panic_safe_shared::PanicSafeShared;

/// Wraps `router` so a panic in any of its handlers becomes a 500 for that
/// request instead of a dropped connection.
///
/// Apply it once per listener, at the point the router is served, rather than
/// inside each router builder. A role composes several routers into one app --
/// a data router merged with `/ready`, a Prometheus router built in another
/// crate -- and one layer at the serving boundary covers all of them without
/// stacking one wrapper per merge.
pub fn contain_handler_panics(router: Router) -> Router {
    router.layer(axum::middleware::from_fn(catch_panic))
}

/// The message a panic carried, for a log line.
///
/// `panic!` stores a `&'static str` when it has no arguments and a `String`
/// when it has. A panic raised by any other means carries a payload this
/// cannot read, which is rare enough to name rather than to decode.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else {
        "panic payload of an unknown type"
    }
}

/// Runs the rest of the stack and answers 500 if it panics.
///
/// The panic hook has already run by the time the unwind reaches here, so the
/// backtrace is on stderr as usual; what this adds is a reply. `AssertUnwindSafe`
/// is the honest annotation: the request is consumed by the inner future and
/// the router is cloned per request, so nothing this frame owns is observed
/// after the unwind. What the *handler* touched is another matter, and
/// [`the module documentation`](crate::panic_containment) say what that costs.
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
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("internal server error\n".into())
                .expect("internal server error response status is valid")
        }
    }
}
