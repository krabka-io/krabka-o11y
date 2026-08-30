use super::*;

pub(crate) async fn serve_compactor_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let delete_requests =
        compactor_delete_requests_for_config(&config, dependencies.delete_requests.clone())?;
    let app = compactor_router_with_delete_requests(delete_requests.clone());
    let dependencies = dependencies.with_delete_requests(delete_requests);
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = http_shutdown_rx.await;
        })
        .into_future();
    let compactor = run_compactor_until_shutdown(&config, dependencies, object_store, pending());
    tokio::pin!(server);
    tokio::pin!(compactor);

    tokio::select! {
        result = &mut server => {
            result?;
            Ok(())
        }
        result = &mut compactor => {
            let _ = http_shutdown_tx.send(());
            result?;
            Ok(())
        }
    }
}
