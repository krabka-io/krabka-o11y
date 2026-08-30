#[cfg(test)]
use super::*;

#[cfg(test)]
pub(crate) async fn serve_role_http(
    addr: SocketAddr,
    router: Router,
    role_name: &'static str,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%error, %role_name, "metrics role server stopped with error");
        }
    });
    Ok(bound)
}
