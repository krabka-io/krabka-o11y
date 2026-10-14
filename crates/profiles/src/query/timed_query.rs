use krabka_observability::service_metrics::{QueryRequest, RequestOutcome};

use super::*;

/// Times `fut`, a Connect handler body, and records the outcome on `route`.
///
/// `Ok` gives `status="ok"` and any `ConnectError` gives `status="error"`. The
/// function observes the latency for every outcome.
pub(crate) async fn timed_query<T>(
    metrics: &ServiceMetrics,
    route: &str,
    fut: impl Future<Output = Result<T, ConnectError>>,
) -> Result<T, ConnectError> {
    let start = std::time::Instant::now();
    let result = fut.await;
    metrics.record_query(QueryRequest {
        route,
        outcome: RequestOutcome::from_result(&result),
        elapsed: start.elapsed().as_time(),
    });
    result
}

pub(crate) async fn timed_connect_query<S, Request, Response, Handler, HandlerFuture>(
    state: Extension<Arc<QuerierState<S>>>,
    principal: Extension<Principal>,
    headers: HeaderMap,
    request: ConnectRequest<Request>,
    route: &str,
    handler: Handler,
) -> Result<ConnectResponse<Response>, ConnectError>
where
    S: ProfileStore,
    Handler: FnOnce(QuerierRequestParts<S>, ConnectRequest<Request>) -> HandlerFuture,
    HandlerFuture: Future<Output = Result<ConnectResponse<Response>, ConnectError>>,
{
    let metrics = state.0.metrics.clone();
    timed_query(
        &metrics,
        route,
        handler(
            QuerierRequestParts::from_extensions(state, principal, headers),
            request,
        ),
    )
    .await
}
