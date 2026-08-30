use super::{Cli, TcpListener, ruler_router};

pub(crate) async fn run_ruler(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(cli.listen).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "metrics ruler listening");
    axum::serve(listener, ruler_router())
        .with_graceful_shutdown(async {
            krabka_observability::shutdown_signal().await;
        })
        .await?;
    Ok(())
}
