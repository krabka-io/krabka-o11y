use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, Request, State},
    middleware::Next,
    response::Response,
};

use super::{
    PeerAddr, Principal, authenticator::Authenticator, unauthorized_response::unauthorized_response,
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
