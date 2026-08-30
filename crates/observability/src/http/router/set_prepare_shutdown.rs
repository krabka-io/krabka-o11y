use super::*;

pub(crate) async fn set_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    state.prepare_shutdown.store(true, AtomicOrdering::SeqCst);
    StatusCode::NO_CONTENT.into_response()
}
