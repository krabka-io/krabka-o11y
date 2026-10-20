//! What a role must have done before it can serve correct answers.
//!
//! A readiness probe that passes as soon as the listener binds tells the
//! orchestrator to route queries into a process that has not loaded its index
//! and has not caught up on the WAL. Such a process answers with silent gaps
//! rather than with an error, so the rollout looks healthy while the data is
//! wrong. [`RoleReadiness`] is the shared seam that lets each role say what it
//! is still waiting for.

use crate::{
    Arc, AtomicBool, AtomicOrdering, Extension, IntoResponse, PanicSafeShared, Response, RoleKind,
    Router, StatusCode, get,
};

mod draining_gate;
mod readiness_gate;
mod recovery_status;
use recovery_status::recovery_status;
mod role_readiness;
#[cfg(test)]
mod tests;

pub use draining_gate::DRAINING_GATE;
pub use readiness_gate::ReadinessGate;
pub use role_readiness::RoleReadiness;

/// A router serving `GET /ready` for `readiness`.
///
/// Merge it into a role's admin router so a probe can ask the admin port and
/// need no route on the data port, and into the data-port router so a client
/// can ask the port it queries.
pub fn readiness_router(readiness: RoleReadiness) -> Router {
    Router::new()
        .route("/ready", get(ready))
        .route("/status/recovery", get(recovery_status))
        .layer(Extension(readiness))
}

/// The `GET /ready` handler every Krabka role serves.
///
/// It answers `200` with `ready\n` when every gate of the [`RoleReadiness`] in
/// the request extensions is met, and `503` with `not ready: <name>, <name>\n`
/// when some are not.
///
/// [`readiness_router`](readiness_router) mounts it at `/ready`. Mount the
/// handler directly when a role also needs it at a second path, as Tempo does
/// with `/status`.
pub async fn ready(Extension(readiness): Extension<RoleReadiness>) -> Response {
    let pending = readiness.pending();
    if pending.is_empty() {
        crate::text_response(StatusCode::OK, "ready\n")
    } else {
        crate::text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &format!("not ready: {}\n", pending.join(", ")),
        )
    }
}
