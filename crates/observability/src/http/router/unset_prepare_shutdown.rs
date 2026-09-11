use super::{DistributorState, IntoResponse, Response, State, StatusCode};

/// `DELETE /ingester/prepare_shutdown`: cancel a drain, as `Loki` does.
pub(crate) async fn unset_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    state.prepare_shutdown.mark_ready();
    StatusCode::NO_CONTENT.into_response()
}
