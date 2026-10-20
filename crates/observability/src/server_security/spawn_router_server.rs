use std::{future::Future, net::SocketAddr};

use axum::Router;
use tokio::net::TcpListener;

use super::{ServerListener, ServerSecurity, serve_router};

pub struct JoinableRouterServer {
    pub address: SocketAddr,
    pub task: tokio::task::JoinHandle<()>,
}

/// What [`spawn_router_server`] binds and serves.
pub struct RouterServer<'a, Shutdown> {
    /// The address to bind; port `0` picks a free port.
    pub addr: SocketAddr,
    /// The routes to serve, before authentication and panic containment.
    pub router: Router,
    /// Decides whether the listener serves TLS and whether a request needs a
    /// credential.
    pub security: &'a ServerSecurity,
    /// Resolves when the server should stop accepting connections and drain.
    pub shutdown: Shutdown,
    /// Names the server in the warning logged when it stops with an error,
    /// such as `"metrics distributor"`.
    pub server_name: &'static str,
}

impl<'a, Shutdown> RouterServer<'a, Shutdown> {
    pub fn new(
        addr: SocketAddr,
        router: Router,
        security: &'a ServerSecurity,
        shutdown: Shutdown,
        server_name: &'static str,
    ) -> Self {
        Self {
            addr,
            router,
            security,
            shutdown,
            server_name,
        }
    }
}

/// Binds `server.addr`, serves `server.router` through [`serve_router`] on a
/// spawned task until `server.shutdown` resolves, and returns the bound
/// address.
///
/// # Errors
///
/// Returns an error when the address cannot be bound, or when the bound
/// socket cannot report its local address.
pub async fn spawn_router_server<Shutdown>(
    server: RouterServer<'_, Shutdown>,
) -> std::io::Result<SocketAddr>
where
    Shutdown: Future<Output = ()> + Send + 'static,
{
    let started = spawn_router_server_joinable(server).await?;
    Ok(started.address)
}

/// Binds and serves a router until shutdown, naming the supervised listener
/// in any stop warning.
///
/// # Errors
/// Returns an error when the address cannot be bound or the listener address
/// cannot be read.
pub async fn spawn_router_server_until_shutdown<Shutdown>(
    addr: SocketAddr,
    router: Router,
    security: &ServerSecurity,
    shutdown: Shutdown,
    server_name: &'static str,
) -> std::io::Result<SocketAddr>
where
    Shutdown: Future<Output = ()> + Send + 'static,
{
    spawn_router_server(RouterServer::new(
        addr,
        router,
        security,
        shutdown,
        server_name,
    ))
    .await
}

/// Binds a router and returns its serving task so the caller can join it.
///
/// # Errors
/// Returns an error when the address cannot be bound or the listener address
/// cannot be read.
pub async fn spawn_router_server_joinable<Shutdown>(
    server: RouterServer<'_, Shutdown>,
) -> std::io::Result<JoinableRouterServer>
where
    Shutdown: Future<Output = ()> + Send + 'static,
{
    let RouterServer {
        addr,
        router,
        security,
        shutdown,
        server_name,
    } = server;
    let listener = ServerListener::bind(TcpListener::bind(addr).await?, security)
        .map_err(std::io::Error::other)?;
    let bound = listener.local_addr();
    let serving = serve_router(listener, router, security)
        .with_graceful_shutdown(shutdown)
        .into_future();
    let task = tokio::spawn(async move {
        if let Err(error) = serving.await {
            tracing::warn!(%error, "{server_name} server stopped with error");
        }
    });
    Ok(JoinableRouterServer {
        address: bound,
        task,
    })
}
