use super::{Router, role_status_router};

pub(crate) fn query_frontend_router() -> Router {
    role_status_router("query-frontend")
}
