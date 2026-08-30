use super::*;

pub(crate) fn distributor_router_with_sink(
    sink: Arc<dyn LogWalSink>,
    ingest_limiter: Arc<dyn LogIngestLimiter>,
    max_ingest_body: Option<ByteSize>,
    wal_append_timeout: Option<Time>,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
    metrics: ServiceMetrics,
) -> Router {
    let grpc_logs_service = OtlpGrpcLogsService {
        sink: Arc::clone(&sink),
        ingest_limiter: Arc::clone(&ingest_limiter),
        wal_append_timeout,
        metrics: metrics.clone(),
    };

    with_role_ops_routes(Router::new(), DISTRIBUTOR_OPS, ServiceReadiness::ready())
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
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route("/loki/api/v1/push", post(push_logs))
        .route("/api/prom/push", post(push_logs))
        .route("/v1/logs", post(push_otlp_logs))
        .route("/otlp/v1/logs", post(push_otlp_logs))
        .route_service(
            "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            LogsServiceServer::new(grpc_logs_service),
        )
        .with_state(DistributorState {
            sink,
            ingest_limiter,
            prepare_shutdown: Arc::new(AtomicBool::new(false)),
            max_ingest_body,
            wal_append_timeout,
            reject_old_samples_max_age,
            creation_grace_period,
            metrics,
        })
}
