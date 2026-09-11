use super::{
    Arc, CancellationToken, JoinHandle, ObjectStore, QUERIER_OPS, Role, Router, ServiceConfig,
    ServiceConfigError, ServiceDependencies, all_in_one_router,
    compactor_delete_requests_for_config, compactor_router_with_delete_requests,
    distributor_router_with_sink, distributor_state_for_config, ingest_limiter_refresh_task,
    limits_provider_for_config, querier_routes_with_shutdown, query_authorizer_for_role,
    service_audit_for_config, with_role_ops_routes, with_service_audit,
};
use crate::{DRAINING_GATE, RoleKind};

pub(crate) async fn build_service_router_with_shutdown(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    token: CancellationToken,
) -> Result<(Router, Vec<(&'static str, JoinHandle<()>)>), ServiceConfigError> {
    let metrics = dependencies.metrics.clone().unwrap_or_default();
    // The binary may have handed in the same readiness its admin port serves,
    // so a probe on `:9404` and a probe on the data port agree.
    let readiness = dependencies.readiness.clone().unwrap_or_default();
    // Every route of the role records through the audit handle in
    // `dependencies`, or through a disabled one.
    let audit = service_audit_for_config(config, &dependencies);
    // One provider for the process. Both the write path and the read path
    // resolve every tenant through this one, so the two gates cannot disagree.
    let overrides = limits_provider_for_config(config)?;
    let (router, background_tasks) = match config.target {
        Role::Distributor => {
            let state = distributor_state_for_config(
                config,
                &dependencies,
                metrics,
                readiness
                    .for_role(RoleKind::Distributor)
                    .gate(DRAINING_GATE),
                overrides,
            )?;
            let refresh = ingest_limiter_refresh_task(&state.ingest_limiter, &token);
            (
                distributor_router_with_sink(
                    state.sink,
                    state.ingest_limiter,
                    state.overrides,
                    config.wal_append_timeout,
                    state.metrics,
                ),
                vec![refresh],
            )
        }
        Role::Querier => {
            let role_authorizer =
                query_authorizer_for_role(config, &dependencies, &token, &readiness);
            let dependencies = ServiceDependencies {
                query_authorizer: Some(role_authorizer.authorizer),
                ..dependencies
            };
            let (routes, mut background_tasks) = Box::pin(querier_routes_with_shutdown(
                config,
                dependencies,
                object_store,
                token,
                metrics,
                readiness.clone(),
                overrides,
            ))
            .await?;
            background_tasks.extend(role_authorizer.task);
            (
                with_role_ops_routes(Router::new(), QUERIER_OPS, readiness).merge(routes),
                background_tasks,
            )
        }
        Role::BlockBuilder => {
            let role_authorizer =
                query_authorizer_for_role(config, &dependencies, &token, &readiness);
            let delete_requests =
                compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
            (
                compactor_router_with_delete_requests(
                    delete_requests,
                    role_authorizer.authorizer,
                    readiness,
                ),
                role_authorizer.task.into_iter().collect(),
            )
        }
        Role::All => {
            let distributor_state = distributor_state_for_config(
                config,
                &dependencies,
                metrics.clone(),
                readiness
                    .for_role(RoleKind::Distributor)
                    .gate(DRAINING_GATE),
                Arc::clone(&overrides),
            )?;
            Box::pin(all_in_one_router(
                config,
                dependencies,
                object_store,
                token,
                metrics,
                readiness,
                distributor_state,
            ))
            .await?
        }
    };
    Ok((with_service_audit(router, audit), background_tasks))
}
