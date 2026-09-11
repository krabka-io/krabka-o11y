use super::{Extension, RoleReadiness, Router, get, ready};

/// A router serving `GET /ready` for `readiness`.
///
/// Merge it into a role's admin router so a probe can ask the admin port and
/// need no route on the data port, and into the data-port router so a client
/// can ask the port it queries.
pub fn readiness_router(readiness: RoleReadiness) -> Router {
    Router::new()
        .route("/ready", get(ready))
        .layer(Extension(readiness))
}
