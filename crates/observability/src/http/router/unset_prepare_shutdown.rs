use super::*;

pub(crate) async fn unset_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    state.prepare_shutdown.store(false, AtomicOrdering::SeqCst);
    StatusCode::NO_CONTENT.into_response()
}
