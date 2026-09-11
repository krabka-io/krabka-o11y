use super::{
    CancellationToken, CriticalTaskError, ObjectStore, ServerListener, ServiceConfig,
    ServiceDependencies, ServiceRuntimeError, SupervisedTasks, TcpListener,
    compactor_delete_requests_for_config, compactor_router_with_delete_requests,
    query_authorizer_for_role, run_compactor_until_shutdown, serve_router,
    service_audit_for_config, shutdown_signal, start_runtime_security, with_service_audit,
};

pub(crate) async fn serve_compactor_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let security = Box::pin(start_runtime_security(&config, &dependencies)).await?;
    // Cancelled on every way out of this function, so an early error does not
    // leave the audit writer waiting for a stop that never comes.
    let _audit_stop = security.audit_stop.clone().drop_guard();
    let dependencies = dependencies.with_audit(security.audit.clone());
    let delete_requests =
        compactor_delete_requests_for_config(&config, dependencies.delete_requests.clone())?;
    let readiness = dependencies.readiness.clone().unwrap_or_default();
    // The delete-request API checks every tenant against this authorizer. Its
    // connect task also keeps the ACL snapshot fresh, so it is supervised: a
    // block builder whose authorizer stopped would refuse every delete call
    // and say nothing. The audit writer is supervised with it, because a
    // block builder whose audit writer stopped would record no delete request.
    let mut tasks = SupervisedTasks::new(CancellationToken::new());
    let role_authorizer =
        query_authorizer_for_role(&config, &dependencies, tasks.token(), &readiness);
    if let Some((name, handle)) = role_authorizer.task {
        tasks.adopt(name, handle);
    }
    if let Some(writer) = security.audit_writer {
        tasks.adopt("audit writer", writer);
    }
    let app = with_service_audit(
        compactor_router_with_delete_requests(
            delete_requests.clone(),
            role_authorizer.authorizer,
            readiness,
        ),
        service_audit_for_config(&config, &dependencies),
    );
    let dependencies = dependencies.with_delete_requests(delete_requests);
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
    let listener = ServerListener::bind(listener, &security.server)?;
    let server = serve_router(listener, app, &security.server)
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

    let outcome = tokio::select! {
        result = &mut server => result.map_err(ServiceRuntimeError::from),
        result = &mut compactor => {
            // The compactor has finished its batch and committed. Stop
            // accepting, let the requests already in flight finish, and only
            // then report how the compactor ended.
            let _ = http_shutdown_tx.send(());
            if let Err(error) = server.await {
                tracing::warn!(%error, "compactor HTTP server stopped with an error while draining");
            }
            result.map(|_| ())
        }
        name = tasks.first_unexpected_exit() => {
            let _ = http_shutdown_tx.send(());
            if let Err(error) = server.await {
                tracing::warn!(%error, "compactor HTTP server stopped with an error while draining");
            }
            Err(CriticalTaskError(name).into())
        }
    };
    // The listener has stopped, so no request records an event after this.
    security.audit_stop.cancel();
    tasks.shutdown().await;
    outcome
}
