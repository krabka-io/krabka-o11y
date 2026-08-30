use super::*;

pub(crate) async fn role_metrics(Extension(ops): Extension<RoleOps>) -> Response {
    status_metrics(ops.target)
}
