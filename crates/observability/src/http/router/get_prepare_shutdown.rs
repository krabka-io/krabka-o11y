use super::{DistributorState, Response, State, StatusCode, text_response};

/// `GET /ingester/prepare_shutdown`: whether a drain is in force.
pub(crate) async fn get_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    let status = if state.prepare_shutdown.is_ready() {
        "unset"
    } else {
        "set"
    };
    text_response(StatusCode::OK, status)
}
