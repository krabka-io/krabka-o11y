#[derive(Clone)]
pub struct DistributorState {
    sink: Arc<dyn LogWalSink>,
    ingest_limiter: Arc<dyn LogIngestLimiter>,
    prepare_shutdown: Arc<AtomicBool>,
    max_ingest_body: Option<ByteSize>,
    wal_append_timeout: Option<Time>,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
    metrics: ServiceMetrics,
}

pub fn distributor_router(sink: impl LogWalSink) -> Router {
    distributor_router_with_sink(
        Arc::new(sink),
        Arc::new(AllowAllIngestLimiter),
        None,
        None,
        None,
        None,
        ServiceMetrics::new(),
    )
}

#[derive(Clone, Copy)]
struct RoleOps {
    target: &'static str,
    ring_component: &'static str,
    role_ring_path: Option<&'static str>,
}

#[derive(Clone)]
struct ServiceReadiness {
    wal_connected: Arc<AtomicBool>,
    authorization_connected: Arc<AtomicBool>,
}

impl ServiceReadiness {
    fn ready() -> Self {
        Self {
            wal_connected: Arc::new(AtomicBool::new(true)),
            authorization_connected: Arc::new(AtomicBool::new(true)),
        }
    }

    fn deferred_querier() -> Self {
        Self {
            wal_connected: Arc::new(AtomicBool::new(false)),
            authorization_connected: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_ready(&self) -> bool {
        self.wal_connected.load(AtomicOrdering::SeqCst)
            && self.authorization_connected.load(AtomicOrdering::SeqCst)
    }
}

const DISTRIBUTOR_OPS: RoleOps = RoleOps {
    target: "distributor",
    ring_component: "krabka-distributor",
    role_ring_path: Some("/distributor/ring"),
};

const QUERIER_OPS: RoleOps = RoleOps {
    target: "querier",
    ring_component: "krabka-querier",
    role_ring_path: None,
};

const COMPACTOR_OPS: RoleOps = RoleOps {
    target: "compactor",
    ring_component: "krabka-compactor",
    role_ring_path: Some("/compactor/ring"),
};

fn with_role_ops_routes<S>(
    mut router: Router<S>,
    ops: RoleOps,
    readiness: ServiceReadiness,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router = router
        .route("/ready", get(ready))
        .route("/log_level", get(log_level).post(log_level_post))
        .route("/metrics", get(role_metrics))
        .route("/config", get(role_config))
        .route("/services", get(role_services))
        .route("/memberlist", get(memberlist_status))
        .route("/ring", get(role_ring))
        .route("/loki/api/v1/status/buildinfo", get(build_info));
    if let Some(path) = ops.role_ring_path {
        router = router.route(path, get(role_ring));
    }
    router.layer(Extension(ops)).layer(Extension(readiness))
}

fn distributor_router_with_sink(
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

#[derive(Clone)]
pub struct OtlpGrpcLogsService {
    sink: Arc<dyn LogWalSink>,
    ingest_limiter: Arc<dyn LogIngestLimiter>,
    wal_append_timeout: Option<Time>,
    metrics: ServiceMetrics,
}

pub fn otlp_grpc_logs_service(sink: impl LogWalSink) -> OtlpGrpcLogsService {
    otlp_grpc_logs_service_with_limiter(sink, AllowAllIngestLimiter)
}

pub fn otlp_grpc_logs_service_with_limiter(
    sink: impl LogWalSink,
    ingest_limiter: impl LogIngestLimiter,
) -> OtlpGrpcLogsService {
    OtlpGrpcLogsService {
        sink: Arc::new(sink),
        ingest_limiter: Arc::new(ingest_limiter),
        wal_append_timeout: None,
        metrics: ServiceMetrics::new(),
    }
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcLogsService {
    async fn export(
        &self,
        request: tonic::Request<ProtoExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ProtoExportLogsServiceResponse>, tonic::Status> {
        let (metadata, _, payload) = request.into_parts();
        let tenant = grpc_tenant(&metadata)?;
        let records = normalize_otlp_proto_logs_for_tenant(tenant, payload, None, None)
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        let state = DistributorState {
            sink: Arc::clone(&self.sink),
            ingest_limiter: Arc::clone(&self.ingest_limiter),
            prepare_shutdown: Arc::new(AtomicBool::new(false)),
            max_ingest_body: None,
            wal_append_timeout: self.wal_append_timeout,
            reject_old_samples_max_age: None,
            creation_grace_period: None,
            metrics: self.metrics.clone(),
        };
        append_distributor_wal_records(&state, records)
            .await
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        Ok(tonic::Response::new(ProtoExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[derive(Debug, Deserialize)]
struct LokiPushRequest {
    #[serde(default)]
    streams: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct LokiTypedPushRequest {
    streams: Vec<LokiPushStream>,
}

#[derive(Debug, Deserialize)]
struct LokiPushStream {
    #[serde(default)]
    stream: Option<Labels>,
    #[serde(default)]
    values: Option<Vec<Value>>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoPushRequest {
    #[prost(message, repeated, tag = "1")]
    streams: Vec<LokiProtoStream>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoStream {
    #[prost(string, tag = "1")]
    labels: String,
    #[prost(message, repeated, tag = "2")]
    entries: Vec<LokiProtoEntry>,
    #[prost(uint64, tag = "3")]
    hash: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoEntry {
    #[prost(message, optional, tag = "1")]
    timestamp: Option<LokiProtoTimestamp>,
    #[prost(string, tag = "2")]
    line: String,
    #[prost(message, repeated, tag = "3")]
    structured_metadata: Vec<LokiProtoLabelPair>,
    #[prost(message, repeated, tag = "4")]
    parsed: Vec<LokiProtoLabelPair>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoTimestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoLabelPair {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpLogsRequest {
    resource_logs: Vec<OtlpResourceLogs>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpResourceLogs {
    resource: Option<OtlpResource>,
    scope_logs: Vec<OtlpScopeLogs>,
}

#[derive(Debug, Deserialize)]
struct OtlpResource {
    attributes: Option<Vec<OtlpKeyValue>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpScopeLogs {
    scope: Option<OtlpScope>,
    log_records: Vec<OtlpLogRecord>,
}

#[derive(Debug, Deserialize)]
struct OtlpScope {
    attributes: Option<Vec<OtlpKeyValue>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtlpLogRecord {
    time_unix_nano: Value,
    #[serde(default)]
    severity_number: Option<Value>,
    #[serde(default)]
    severity_text: Option<String>,
    body: Option<OtlpAnyValue>,
    attributes: Option<Vec<OtlpKeyValue>>,
}

#[derive(Clone, Debug, Deserialize)]
struct OtlpKeyValue {
    key: String,
    value: OtlpAnyValue,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum OtlpAnyValue {
    #[serde(rename = "stringValue")]
    String(String),
    #[serde(rename = "boolValue")]
    Bool(bool),
    #[serde(rename = "intValue")]
    Int(Value),
    #[serde(rename = "doubleValue")]
    Double(Value),
    #[serde(rename = "bytesValue")]
    Bytes(String),
    #[serde(rename = "arrayValue")]
    Array(OtlpArrayValue),
    #[serde(rename = "kvlistValue")]
    Kvlist(OtlpKeyValueList),
}

#[derive(Clone, Debug, Deserialize)]
struct OtlpArrayValue {
    values: Option<Vec<OtlpAnyValue>>,
}

#[derive(Clone, Debug, Deserialize)]
struct OtlpKeyValueList {
    values: Option<Vec<OtlpKeyValue>>,
}

/// Tenant for an ingest request, from `X-Scope-OrgID`. It falls back to
/// `"unknown"` when the header is missing, non-UTF-8, or empty.
///
/// The value only labels the ingest span and the per-tenant metric. The WAL
/// records carry their own per-record tenant, so a permissive fallback here
/// never affects storage.
fn ingest_tenant(headers: &HeaderMap) -> String {
    headers
        .get("X-Scope-OrgID")
        .and_then(|v| v.to_str().ok())
        .filter(|t| !t.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

async fn push_logs(
    State(state): State<DistributorState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_size = measured_size(body.len());
    let tenant = ingest_tenant(&headers);
    // ONE server span per push request (not per log line): wraps the whole
    // ingest body so the produce-side WAL append (which injects `traceparent`)
    // and downstream compaction stitch onto this trace. `krabka.ingest.lines`
    // is unknown until normalization, so it is recorded on the span below.
    let span = tracing::info_span!(
        "logs_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = "__krabka_observability_logs_wal",
        krabka.tenant = %tenant,
        krabka.ingest.lines = tracing::field::Empty,
        krabka.ingest.bytes = body_size.bytes_u64(),
    );
    async move {
        let start = Instant::now();
        if let Err(error) = validate_ingest_body_limit(&state, body_size) {
            return record_ingest_response(&state, error.into_response(), body_size, 0, start);
        }
        let resp = match normalize_loki_http_push(
            &headers,
            &body,
            state.reject_old_samples_max_age,
            state.creation_grace_period,
        ) {
            Ok(records) => {
                let items = records.len() as u64;
                tracing::Span::current().record("krabka.ingest.lines", items);
                state.metrics.record_ingest_lines(&tenant, items);
                let resp = match append_distributor_wal_records(&state, records).await {
                    Ok(()) => StatusCode::NO_CONTENT.into_response(),
                    Err(error) => error.into_response(),
                };
                return record_ingest_response(&state, resp, body_size, items, start);
            }
            Err(error) => error.into_response(),
        };
        record_ingest_response(&state, resp, body_size, 0, start)
    }
    .instrument(span)
    .await
}

async fn push_otlp_logs(
    State(state): State<DistributorState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_size = measured_size(body.len());
    let tenant = ingest_tenant(&headers);
    // ONE server span per OTLP push request, mirroring the Loki push handler:
    // the OTLP emit path feeds the same log WAL, so instrumenting it keeps the
    // produce→compaction trace intact for OTLP-emitted logs too.
    let span = tracing::info_span!(
        "logs_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = "__krabka_observability_logs_wal",
        krabka.tenant = %tenant,
        krabka.ingest.lines = tracing::field::Empty,
        krabka.ingest.bytes = body_size.bytes_u64(),
    );
    async move {
        let start = Instant::now();
        if let Err(error) = validate_ingest_body_limit(&state, body_size) {
            return record_ingest_response(&state, error.into_response(), body_size, 0, start);
        }
        let resp = match normalize_otlp_http_logs(
            &headers,
            &body,
            state.reject_old_samples_max_age,
            state.creation_grace_period,
        ) {
            Ok(records) => {
                let items = records.len() as u64;
                tracing::Span::current().record("krabka.ingest.lines", items);
                state.metrics.record_ingest_lines(&tenant, items);
                let resp = match append_distributor_wal_records(&state, records).await {
                    Ok(()) => StatusCode::NO_CONTENT.into_response(),
                    Err(error) => {
                        // Surface why an accepted OTLP log batch failed to persist
                        // (WAL append errors are otherwise opaque to the client).
                        tracing::debug!(error = %error, "OTLP logs: WAL append failed");
                        error.into_response()
                    }
                };
                return record_ingest_response(&state, resp, body_size, items, start);
            }
            Err(error) => {
                // Surface why an OTLP log push was rejected at decode/normalize
                // (content-type, encoding, and size pinpoint client misconfig).
                tracing::debug!(
                    error = %error,
                    content_type = ?headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()),
                    content_encoding = ?headers.get(CONTENT_ENCODING).and_then(|v| v.to_str().ok()),
                    bytes = body_size.bytes_u64(),
                    "OTLP logs: decode/normalize rejected the request"
                );
                otlp_http_error_response(error)
            }
        };
        record_ingest_response(&state, resp, body_size, 0, start)
    }
    .instrument(span)
    .await
}

