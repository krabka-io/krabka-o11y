use std::sync::Arc;

use axum::{
    Router,
    extract::{ConnectInfo, Request, State},
    middleware::Next,
    response::Response,
};

use super::{
    PeerAddr, Principal, ServerSecurity, authenticator::Authenticator,
    unauthorized_response::unauthorized_response,
};

/// Authenticates one request, puts its [`Principal`] into the extensions, and runs the rest of the stack.
///
/// Without a credentials file every request gets
/// [`Principal::Unauthenticated`], and nothing else changes. With one, a route
/// in the unauthenticated list runs with no principal, a request whose
/// credential matches runs with its principal, and every other request gets a
/// 401.
pub async fn authenticate_request(
    State(authenticator): State<Option<Arc<Authenticator>>>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(authenticator) = authenticator else {
        request.extensions_mut().insert(Principal::Unauthenticated);
        return next.run(request).await;
    };
    if authenticator
        .unauthenticated_routes
        .contains(request.method(), request.uri().path())
    {
        return next.run(request).await;
    }
    let peer = request
        .extensions()
        .get::<ConnectInfo<PeerAddr>>()
        .map(|ConnectInfo(peer)| peer);
    let Some(principal) = authenticator.authenticate(request.headers(), peer) else {
        return unauthorized_response();
    };
    request.extensions_mut().insert(principal);
    next.run(request).await
}

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
