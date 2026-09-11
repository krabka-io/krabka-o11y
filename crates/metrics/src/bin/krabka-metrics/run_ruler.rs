use krabka_observability::contain_handler_panics;

use super::{Cli, RoleReadiness, TcpListener, readiness_router, ruler_router};

pub(crate) async fn run_ruler(
    cli: Cli,
    readiness: RoleReadiness,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(cli.listen).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "metrics ruler listening");
    axum::serve(
        listener,
        contain_handler_panics(ruler_router().merge(readiness_router(readiness))),
    )
    .with_graceful_shutdown(async {
        krabka_observability::shutdown_signal().await;
    })
    .await?;
    Ok(())
}
