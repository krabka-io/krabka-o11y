use super::{
    ObjectStore, ServiceConfig, ServiceDependencies, ServiceRuntimeError, TcpListener,
    compactor_delete_requests_for_config, compactor_router_with_delete_requests,
    contain_handler_panics, run_compactor_until_shutdown, shutdown_signal,
};

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
    let server = axum::serve(listener, contain_handler_panics(app))
        .with_graceful_shutdown(async {
            let _ = http_shutdown_rx.await;
        })
        .into_future();
    // SIGTERM is how Kubernetes, systemd and `docker stop` ask a process to
    // stop. Waiting on a future that never resolves left the compactor to be
    // SIGKILLed at the end of the grace period, mid-batch: the block it was
    // writing was abandoned and its WAL offset went uncommitted, so every
    // ordinary restart replayed that window.
    let compactor =
        run_compactor_until_shutdown(&config, dependencies, object_store, shutdown_signal());
    tokio::pin!(server);
    tokio::pin!(compactor);

    tokio::select! {
        result = &mut server => {
            result?;
            Ok(())
        }
        result = &mut compactor => {
            // The compactor has finished its batch and committed. Stop
            // accepting, let the requests already in flight finish, and only
            // then report how the compactor ended.
            let _ = http_shutdown_tx.send(());
            if let Err(error) = server.await {
                tracing::warn!(%error, "compactor HTTP server stopped with an error while draining");
            }
            result?;
            Ok(())
        }
    }
}
