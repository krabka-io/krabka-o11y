use axum::Router;

use super::{ServerSecurity, authenticate_request::authenticate_request};

/// Wraps `router` so every request is authenticated as `security` says.
///
/// Every handler of the wrapped router finds a [`Principal`](super::Principal)
/// in its request extensions, except on an
/// [`UnauthenticatedRoutes`](super::UnauthenticatedRoutes) route. Without a
/// credentials file that principal is always
/// [`Principal::Unauthenticated`](super::Principal::Unauthenticated), and a
/// credential in the request is ignored. The layer also covers the router's
/// fallback, so a request for an unknown path gets a 401 before it gets a 404.
///
/// The layer reads a client certificate from `ConnectInfo<PeerAddr>`. Serve the
/// router with [`serve_router`](super::serve_router), which adds that.
pub fn authenticate_requests(router: Router, security: &ServerSecurity) -> Router {
    router.layer(axum::middleware::from_fn_with_state(
        security.authenticator(),
        authenticate_request,
    ))
}
