use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::{
    Arc, MimirTenantAdminState, PrometheusApiState, RoleLaunch, RoleObjectStore, ServerSecurity,
    Shutdown, WalHead, WalHeadConsumerRecovery, WalHeadFeed, load_runtime_overrides,
    mimir_tenant_admin_router, prometheus_router, query_engine_opts, readiness_router,
    serve_prometheus_router_joinable, spawn_role_wal_head_consumer,
};

#[tracing::instrument(
    level = "info",
    name = "metrics.run_querier",
    skip_all,
    fields(listen = %launch.cli.listen, object_store = %launch.cli.object_store_url, manifest_prefix = %launch.cli.manifest_prefix, wal_topic = %launch.cli.wal_topic),
)]
pub(crate) fn run_querier(
    launch: RoleLaunch,
    security: ServerSecurity,
    shutdown: Shutdown,
) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static {
    let RoleLaunch {
        cli,
        metrics,
        readiness,
        wal_security,
        audit,
    } = launch;
    let startup = readiness.gate("startup");
    Box::pin(tracing::Instrument::instrument(
        async move {
            let role_store = RoleObjectStore::open(&cli, &metrics, &readiness)?;
            let store = Arc::clone(&role_store.store);
            let head = WalHead::with_retention(cli.wal_head_retention);
            let recovery_metrics = metrics.wal_consumer.clone();
            readiness.track_wal_consumer(recovery_metrics.clone());
            let status_wal = cli
                .wal_bootstrap
                .as_ref()
                .map(|_| (head.clone(), readiness.gate("wal-head")));
            let query_limits = load_runtime_overrides(cli.runtime_overrides.as_deref())?;
            // The WAL head consumer is the querier's recent window. If it stops, the
            // role keeps its listener and answers from the last compacted block with
            // nothing in the response to say the rest is missing, so its exit -- panic
            // included -- has to end the role.
            let mut tasks = SupervisedTasks::new(shutdown.token().clone());
            if let Some(bootstrap) = cli.wal_bootstrap.clone() {
                // Configured to read the WAL, so the querier is not ready until it
                // does: until then its answers stop at the last compacted block and
                // silently omit everything since.
                let wal_head_gate = status_wal
                    .as_ref()
                    .expect("configured WAL bootstrap registers a readiness gate")
                    .1
                    .clone();
                let group_id = cli.wal_group_id.clone();
                tasks.adopt(
                    "metrics querier WAL head",
                    spawn_role_wal_head_consumer(
                        &cli,
                        WalHeadFeed {
                            bootstrap,
                            security: wal_security,
                            group_id,
                            head: head.clone(),
                            gate: wal_head_gate,
                            recovery: WalHeadConsumerRecovery::for_serving_role(
                                recovery_metrics,
                                &readiness,
                            ),
                        },
                        shutdown.clone(),
                    ),
                );
            }
            let metric_store = Arc::new(role_store.refreshing_metric_store(&cli, head.clone()));
            let state = PrometheusApiState::new(Arc::clone(&metric_store), query_engine_opts(&cli))
                .with_erasure_store(Arc::clone(&store))
                .with_max_concurrent_queries(cli.max_concurrent_queries)
                .with_query_timeout(cli.query_timeout)
                .with_remote_read_max_body(cli.remote_read_max_body)
                .with_runtime_status(
                    krabka_observability::LogLevelControl::process().level(),
                    None,
                )
                .with_metrics(metrics)
                .with_audit(audit);
            let state = if let Some((head, readiness)) = status_wal {
                state.with_wal_head_status(head, readiness)
            } else {
                state
            };
            let state = state.with_query_limits(query_limits);
            let router = prometheus_router(Arc::new(state))
                .merge(mimir_tenant_admin_router(MimirTenantAdminState::new(
                    store,
                    metric_store,
                    head,
                )))
                .merge(readiness_router(readiness));
            let (bound, mut server) = match serve_prometheus_router_joinable(
                cli.listen,
                router,
                &security,
                shutdown.signalled(),
            )
            .await
            {
                Ok(server) => server,
                Err(error) => {
                    tasks.shutdown().await;
                    return Err(Box::<dyn std::error::Error + Send + Sync>::from(error));
                }
            };
            startup.mark_ready();
            tracing::info!(
                %bound,
                tls = security.tls_enabled(),
                authentication = security.authentication_enabled(),
                "metrics-service querier listening"
            );
            // Join the server task so in-flight requests drain (graceful shutdown)
            // before the process exits -- unless the WAL head consumer stops first, in
            // which case the role fails by name and the drain happens on the way out.
            let outcome = tokio::select! {
                result = &mut server => result.map_err(Into::into),
                name = tasks.first_unexpected_exit() => {
                    shutdown.trigger();
                    let _ = server.await;
                    Err(Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)))
                },
            };
            shutdown.trigger();
            tasks.shutdown().await;
            outcome
        },
        tracing::Span::current(),
    ))
}
