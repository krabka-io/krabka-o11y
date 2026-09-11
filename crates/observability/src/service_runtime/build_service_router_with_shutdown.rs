use super::{
    CancellationToken, JoinHandle, ObjectStore, QUERIER_OPS, Role, Router, ServiceConfig,
    ServiceConfigError, ServiceDependencies, all_in_one_router,
    compactor_delete_requests_for_config, compactor_router_with_delete_requests,
    distributor_router_with_sink, distributor_state_for_config, querier_routes_with_shutdown,
    with_role_ops_routes,
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
    match config.target {
        Role::Distributor => {
            let state = distributor_state_for_config(
                config,
                &dependencies,
                metrics,
                readiness
                    .for_role(RoleKind::Distributor)
                    .gate(DRAINING_GATE),
            )?;
            Ok((
                distributor_router_with_sink(
                    state.sink,
                    state.ingest_limiter,
                    config.max_ingest_body,
                    config.wal_append_timeout,
                    Some(config.reject_old_samples_max_age),
                    Some(config.creation_grace_period),
                    state.metrics,
                ),
                Vec::new(),
            ))
        }
        Role::Querier => {
            let (routes, background_tasks) = querier_routes_with_shutdown(
                config,
                dependencies,
                object_store,
                token,
                metrics,
                readiness.clone(),
            )
            .await?;
            Ok((
                with_role_ops_routes(Router::new(), QUERIER_OPS, readiness).merge(routes),
                background_tasks,
            ))
        }
        Role::BlockBuilder => {
            let delete_requests =
                compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
            Ok((
                compactor_router_with_delete_requests(delete_requests),
                Vec::new(),
            ))
        }
        Role::All => {
            let distributor_state = distributor_state_for_config(
                config,
                &dependencies,
                metrics.clone(),
                readiness
                    .for_role(RoleKind::Distributor)
                    .gate(DRAINING_GATE),
            )?;
            all_in_one_router(
                config,
                dependencies,
                object_store,
                token,
                metrics,
                readiness,
                distributor_state,
            )
            .await
        }
    }
}
