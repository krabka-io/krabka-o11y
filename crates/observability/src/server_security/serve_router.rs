use axum::{
    Router,
    extract::{ConnectInfo, connect_info::IntoMakeServiceWithConnectInfo},
    middleware::AddExtension,
    serve::Serve,
};

use super::{PeerAddr, ServerListener, ServerSecurity, authenticate_requests};
use crate::contain_handler_panics;

/// Serves `router` on `listener` with authentication and panic containment.
///
/// The layers go on in this order, from the inside out:
///
/// 1. [`authenticate_requests`], which puts the [`Principal`](super::Principal)
///    into each request;
/// 2. [`contain_handler_panics`], which wraps the authentication layer too, so
///    a panic there is a 500 and not a dropped connection;
/// 3. `into_make_service_with_connect_info::<PeerAddr>()`, which gives every
///    request its `ConnectInfo<PeerAddr>`.
///
/// The return value is `axum::serve`'s own future, so a caller still adds
/// `with_graceful_shutdown`.
pub fn serve_router(
    listener: ServerListener,
    router: Router,
    security: &ServerSecurity,
) -> Serve<
    ServerListener,
    IntoMakeServiceWithConnectInfo<Router, PeerAddr>,
    AddExtension<Router, ConnectInfo<PeerAddr>>,
> {
    let app = contain_handler_panics(authenticate_requests(router, security));
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<PeerAddr>(),
    )
}
