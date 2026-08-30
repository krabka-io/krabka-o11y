use super::{Extension, Response, RoleOps, status_services};

pub(crate) async fn role_services(Extension(ops): Extension<RoleOps>) -> Response {
    status_services(ops.target)
}
