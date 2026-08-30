use super::*;

pub(crate) async fn get_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    let status = if state.prepare_shutdown.load(AtomicOrdering::SeqCst) {
        "set"
    } else {
        "unset"
    };
    text_response(StatusCode::OK, status)
}
