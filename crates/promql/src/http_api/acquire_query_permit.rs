use super::{MetricStore, PrometheusApiState, OwnedSemaphorePermit, Arc};

pub(crate) async fn acquire_query_permit<S: MetricStore>(
    state: &PrometheusApiState<S>,
) -> Option<OwnedSemaphorePermit> {
    match &state.query_gate {
        Some(gate) => Some(
            Arc::clone(gate)
                .acquire_owned()
                .await
                .expect("query semaphore is never closed"),
        ),
        None => None,
    }
}
