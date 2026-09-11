use super::{DistributorState, IntoResponse, Response, State, StatusCode};

/// `/ingester/shutdown`: leave rotation for good.
///
/// `Loki` flushes, unregisters and terminates. Krabka has nothing to flush --
/// a push is acknowledged only after its WAL append is -- and no ring to leave,
/// so what is left of the operation is to stop being ready, which this does.
/// The process does not exit itself: an orchestrator that sees the probe fail
/// stops it, and a process that killed itself out from under an in-flight
/// query would be the opposite of a drain.
pub(crate) async fn shutdown_ingester(State(state): State<DistributorState>) -> Response {
    state.prepare_shutdown.mark_unready();
    StatusCode::NO_CONTENT.into_response()
}
