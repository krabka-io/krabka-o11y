use std::{convert::Infallible, future::Future, task::Poll};

use axum::serve::Listener;
use futures_util::Stream;

use super::{GrpcConnection, ServerListener};

/// The connections of `listener`, as the stream that tonic's `serve_with_incoming_shutdown` takes.
///
/// A standalone gRPC server gets the same TLS, the same concurrent
/// handshakes, and the same verified client identity as an HTTP listener:
///
/// ```no_run
/// # use krabka_observability::server_security::{GrpcAuthenticationLayer, ServerListener, ServerSecurity, grpc_incoming};
/// # async fn f<S>(security: ServerSecurity, service: S) -> Result<(), Box<dyn std::error::Error>>
/// # where
/// #     S: tower::Service<axum::http::Request<tonic::body::Body>, Response = axum::http::Response<tonic::body::Body>, Error = std::convert::Infallible>
/// #         + tonic::server::NamedService + Clone + Send + Sync + 'static,
/// #     S::Future: Send + 'static,
/// # {
/// let tcp = tokio::net::TcpListener::bind("127.0.0.1:4317").await?;
/// let listener = ServerListener::bind(tcp, &security)?;
/// tonic::transport::Server::builder()
///     .layer(GrpcAuthenticationLayer::new(&security))
///     .add_service(service)
///     .serve_with_incoming_shutdown(grpc_incoming(listener), async {})
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Use this and not `tonic::transport::ServerTlsConfig`. tonic builds its TLS
/// configuration with `rustls::ServerConfig::builder()`, which panics in a
/// process that compiles in both the `ring` and the `aws-lc-rs` providers,
/// as every Krabka service does.
///
/// Every connection gets `TCP_NODELAY`, which is the default of
/// `tonic::transport::Server` when it binds its own socket.
pub fn grpc_incoming(
    listener: ServerListener,
) -> impl Stream<Item = Result<GrpcConnection, Infallible>> + Send + 'static {
    let mut listener = listener;
    futures_util::stream::poll_fn(move |context| {
        let mut accept = Box::pin(Listener::accept(&mut listener));
        match accept.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(connection) => {
                let stream = connection.0;
                let peer = connection.1;
                if let Err(error) = stream.tcp().set_nodelay(true) {
                    tracing::debug!(peer = %peer.socket, %error, "cannot set TCP_NODELAY on a gRPC connection");
                }
                Poll::Ready(Some(Ok(GrpcConnection::new(stream, peer))))
            }
        }
    })
}
