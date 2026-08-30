#[cfg(test)]
use super::*;

#[cfg(test)]
pub(crate) async fn serve_query_frontend(
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    serve_role_http(
        addr,
        query_frontend_router(),
        "metrics query-frontend",
        shutdown,
    )
    .await
}
