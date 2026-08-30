use super::{Router, role_status_router};

pub(crate) fn ruler_router() -> Router {
    role_status_router("ruler")
}
