use super::{
    Arc, DistributorState, LogsServiceServer, OtlpGrpcLogsService, Router, flush_ingester_chunks,
    get, get_prepare_shutdown, post, push_logs, push_otlp_logs, set_prepare_shutdown,
    shutdown_ingester, unset_prepare_shutdown,
};

/// Every write route the distributor serves, with no ops routes on it.
///
/// The ops routes -- `/ready`, `/metrics`, `/config`, `/services` and the ring
/// pages -- say which role the process is, and a process is one role even when
/// it runs several of them. Keeping them out of here is what lets an
/// all-in-one merge this surface with
/// [`loki_query_routes`](crate::http::loki_query_routes) and still answer
/// `/ready` once, for the whole process, rather than twice with two different
/// answers.
pub(crate) fn distributor_push_routes(state: DistributorState) -> Router {
    let grpc_logs_service = OtlpGrpcLogsService {
        sink: Arc::clone(&state.sink),
        ingest_limiter: Arc::clone(&state.ingest_limiter),
        wal_append_timeout: state.wal_append_timeout,
        metrics: state.metrics.clone(),
    };

    Router::new()
        .route("/flush", post(flush_ingester_chunks))
        .route(
            "/ingester/prepare_shutdown",
            get(get_prepare_shutdown)
                .post(set_prepare_shutdown)
                .delete(unset_prepare_shutdown),
        )
        .route(
            "/ingester/shutdown",
            get(shutdown_ingester).post(shutdown_ingester),
        )
        .route("/loki/api/v1/push", post(push_logs))
        .route("/api/prom/push", post(push_logs))
        .route("/v1/logs", post(push_otlp_logs))
        .route("/otlp/v1/logs", post(push_otlp_logs))
        .route_service(
            "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            LogsServiceServer::new(grpc_logs_service),
        )
        .with_state(state)
}
