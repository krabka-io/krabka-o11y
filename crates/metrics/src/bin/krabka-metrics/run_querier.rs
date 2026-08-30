use super::*;

pub(crate) async fn run_querier(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(cli.listen).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "metrics querier listening");
    axum::serve(listener, querier_router())
        .with_graceful_shutdown(async {
            krabka_observability::shutdown_signal().await;
        })
        .await?;
    Ok(())
}
