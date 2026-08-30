use super::{
    Extension, RoleOps, Router, ServiceReadiness, build_info, get, log_level, log_level_post,
    memberlist_status, ready, role_config, role_metrics, role_ring, role_services,
};

pub(crate) fn with_role_ops_routes<S>(
    mut router: Router<S>,
    ops: RoleOps,
    readiness: ServiceReadiness,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router = router
        .route("/ready", get(ready))
        .route("/log_level", get(log_level).post(log_level_post))
        .route("/metrics", get(role_metrics))
        .route("/config", get(role_config))
        .route("/services", get(role_services))
        .route("/memberlist", get(memberlist_status))
        .route("/ring", get(role_ring))
        .route("/loki/api/v1/status/buildinfo", get(build_info));
    if let Some(path) = ops.role_ring_path {
        router = router.route(path, get(role_ring));
    }
    router.layer(Extension(ops)).layer(Extension(readiness))
}
