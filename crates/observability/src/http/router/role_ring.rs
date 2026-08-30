use super::{Extension, Response, RoleOps, ring_status_page};

pub(crate) async fn role_ring(Extension(ops): Extension<RoleOps>) -> Response {
    ring_status_page(ops.ring_component)
}
