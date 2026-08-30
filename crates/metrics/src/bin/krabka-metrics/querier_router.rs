use super::{Router, get, querier_build_info};

pub(crate) fn querier_router() -> Router {
    Router::new()
        .route("/api/v1/status/buildinfo", get(querier_build_info))
        .route(
            "/prometheus/api/v1/status/buildinfo",
            get(querier_build_info),
        )
}
