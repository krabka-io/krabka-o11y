use super::{Extension, Response, RoleReadiness, status_services};

pub(crate) async fn role_services(Extension(readiness): Extension<RoleReadiness>) -> Response {
    status_services(&readiness)
}
