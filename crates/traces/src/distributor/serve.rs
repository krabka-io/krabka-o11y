use std::future::IntoFuture as _;

use super::{ReceiverEndpoint, SocketAddr, bind_listener, router, serve_router, spawn_server};

/// Serve the distributor until cancelled, returning the bound address and the
/// accept loop's handle.
///
/// The listener serves as `security` says: plain HTTP with every request
/// unauthenticated when no security flag is set, and TLS, a credential, or
/// both, when the flags ask for them. Each push door then checks that the
/// request's principal may use the tenant it names.
///
/// The handle is half the return value because the caller has to keep it. A
/// listener that stops accepting -- because the loop errored, or because it
/// panicked and unwound past the `if let Err` below -- leaves a distributor
/// process that is still alive, still passing a liveness probe, and no longer
/// taking spans. Supervising the handle is what turns that into an exit.
///
/// A panic inside a handler is the other case and is contained: the router
/// answers 500 for that request and the accept loop carries on.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve(
    endpoint: ReceiverEndpoint<'_>,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let ReceiverEndpoint {
        addr,
        state,
        security,
        shutdown,
    } = endpoint;
    let listener = bind_listener(addr, security).await?;
    let bound = listener.local_addr();
    let server = serve_router(listener, router(state), security)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .into_future();
    Ok((bound, spawn_server(server, "traces distributor server")))
}
