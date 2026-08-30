use super::*;

pub(crate) fn role_status_router(role: &'static str) -> Router {
    Router::new()
        .route(
            "/api/v1/status/buildinfo",
            get(move || async move { role_build_info(role) }),
        )
        .route(
            "/prometheus/api/v1/status/buildinfo",
            get(move || async move { role_build_info(role) }),
        )
}
