use super::{
    ALL_OPS, Arc, CancellationToken, DistributorState, JoinHandle, ObjectStore, Router,
    ServiceConfig, ServiceConfigError, ServiceDependencies, ServiceMetrics,
    compactor_delete_requests_for_config, delete_request_routes, distributor_push_routes,
    ingest_limiter_refresh_task, querier_routes_with_shutdown, query_authorizer_for_role,
    with_role_ops_routes,
};
use crate::{RoleKind, RoleReadiness};

/// The one data port of a `--target all` process, and the querier tasks behind
/// it.
///
/// `Loki`'s single binary serves push and query on one port, and so does this.
/// The three roles' surfaces are merged rather than served on three listeners,
/// because three listeners would mean three ports to publish, three probes to
/// wire, and a `Loki` datasource that could not simply be pointed at the
/// process. The routes do not overlap: each role's push, read and
/// delete-request routes are disjoint, and the ops routes -- which every role
/// would otherwise contribute -- are added once here, under
/// [`ALL_OPS`](crate::ALL_OPS).
///
/// # Errors
/// Returns an error when a dependency any of the three roles needs is absent,
/// or when the querier's object store or index cannot be built.
pub(crate) async fn all_in_one_router(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    token: CancellationToken,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    distributor_state: DistributorState,
) -> Result<(Router, Vec<(&'static str, JoinHandle<()>)>), ServiceConfigError> {
    // The querier resolves tenants through the very provider the distributor
    // holds, so the two gates cannot answer one tenant with two limit sets.
    let overrides = Arc::clone(&distributor_state.overrides);
    let delete_requests =
        compactor_delete_requests_for_config(config, dependencies.delete_requests.clone())?;
    // Without the consumer: the one in `dependencies` is the block builder's,
    // and a querier that took it as its hot tail would poll records out of the
    // block builder's group. See
    // [`without_wal_consumer`](ServiceDependencies::without_wal_consumer).
    let querier_readiness = readiness.for_role(RoleKind::Querier);
    // One authorizer for the reads, the ruler and the delete-request API, so
    // the three surfaces cannot answer one tenant in two ways.
    let role_authorizer =
        query_authorizer_for_role(config, &dependencies, &token, &querier_readiness);
    let query_authorizer = Arc::clone(&role_authorizer.authorizer);
    let limiter_task = ingest_limiter_refresh_task(&distributor_state.ingest_limiter, &token);
    let (querier_routes, mut background_tasks) = querier_routes_with_shutdown(
        config,
        ServiceDependencies {
            query_authorizer: Some(role_authorizer.authorizer),
            ..dependencies
                .without_wal_consumer()
                .with_delete_requests(delete_requests.clone())
        },
        object_store,
        token,
        metrics,
        // Scoped, so a querier precondition that is unmet reads as
        // `querier/wal-tail` rather than as a bare `wal-tail` that any of the
        // three roles could have registered.
        querier_readiness,
        overrides,
    )
    .await?;
    background_tasks.extend(role_authorizer.task);
    background_tasks.push(limiter_task);
    let router = with_role_ops_routes(Router::new(), ALL_OPS, readiness)
        .merge(distributor_push_routes(distributor_state))
        .merge(querier_routes)
        .merge(delete_request_routes(delete_requests, query_authorizer));
    Ok((router, background_tasks))
}
