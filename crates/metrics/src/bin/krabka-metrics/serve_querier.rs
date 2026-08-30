#[cfg(test)]
use super::*;

#[cfg(test)]
pub(crate) async fn serve_querier(
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    serve_role_http(addr, querier_router(), "metrics querier", shutdown).await
}
