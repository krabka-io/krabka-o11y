use super::*;

#[cfg(test)]
pub(crate) async fn serve_ruler(
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    serve_role_http(addr, ruler_router(), "metrics ruler", shutdown).await
}
