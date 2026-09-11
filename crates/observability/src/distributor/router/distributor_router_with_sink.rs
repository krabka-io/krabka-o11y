use super::{
    Arc, ByteSize, DISTRIBUTOR_OPS, DRAINING_GATE, DistributorState, LogIngestLimiter, LogWalSink,
    RoleReadiness, Router, ServiceMetrics, Time, distributor_push_routes, format_query,
    format_query_post, get, with_role_ops_routes,
};

pub(crate) fn distributor_router_with_sink(
    sink: Arc<dyn LogWalSink>,
    ingest_limiter: Arc<dyn LogIngestLimiter>,
    max_ingest_body: Option<ByteSize>,
    wal_append_timeout: Option<Time>,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
    metrics: ServiceMetrics,
) -> Router {
    // The one gate this role owns. It starts met -- a distributor that has
    // bound its listener can take writes -- and an operator's drain request
    // drops it, which is the only thing `/ready` on this role reports.
    let readiness = RoleReadiness::new();
    let accepting_writes = readiness.gate(DRAINING_GATE);
    accepting_writes.mark_ready();

    with_role_ops_routes(Router::new(), DISTRIBUTOR_OPS, readiness)
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .merge(distributor_push_routes(DistributorState {
            sink,
            ingest_limiter,
            prepare_shutdown: accepting_writes,
            max_ingest_body,
            wal_append_timeout,
            reject_old_samples_max_age,
            creation_grace_period,
            metrics,
        }))
}
