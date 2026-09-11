use super::{Extension, Response, RoleOps, RoleReadiness, ring_status_page};

pub(crate) async fn role_ring(
    Extension(ops): Extension<RoleOps>,
    Extension(readiness): Extension<RoleReadiness>,
) -> Response {
    // The same readiness `/ready` and `/services` report. A process that is
    // still loading its index is joining, not serving.
    let state = if readiness.is_ready() {
        "ACTIVE"
    } else {
        "JOINING"
    };
    ring_status_page(ops.ring_component, state)
}
