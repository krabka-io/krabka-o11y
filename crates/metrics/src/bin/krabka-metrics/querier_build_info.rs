use super::{IntoResponse, role_build_info};

pub(crate) async fn querier_build_info() -> impl IntoResponse {
    role_build_info("querier")
}
