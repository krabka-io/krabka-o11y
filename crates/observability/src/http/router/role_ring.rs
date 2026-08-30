use super::*;

pub(crate) async fn role_ring(Extension(ops): Extension<RoleOps>) -> Response {
    ring_status_page(ops.ring_component)
}
