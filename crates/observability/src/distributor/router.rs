use axum::response::IntoResponse;
use krabka_units::convert::ByteSizeExt;
use prost::Message as _;
use tracing::Instrument;

use crate::{
    AllowAllIngestLimiter, Arc, ByteSize, Bytes, CONTENT_ENCODING, CONTENT_TYPE, DRAINING_GATE,
    Deserialize, DistributorError, Extension, HeaderMap, Instant, Labels, Limits, LogIngestLimiter,
    LogWalSink, LogsService, LogsServiceServer, OverridesProvider, ProtoExportLogsServiceRequest,
    ProtoExportLogsServiceResponse, ReadinessGate, RequestSecurity, Response, RoleReadiness,
    Router, ServiceMetrics, State, StatusCode, TenantErrorSurface, TenantId, Time, Value,
    WalLogRecord, append_distributor_wal_records, build_info, distributor_error_to_grpc_status,
    flush_ingester_chunks, format_query, format_query_post, get, get_prepare_shutdown, grpc_tenant,
    log_level, log_level_post, measured_size, memberlist_status, normalize_loki_http_push,
    normalize_otlp_http_logs, normalize_otlp_proto_logs_for_tenant, otlp_http_error_response, post,
    ready, record_ingest_response, require_org_id, resolve_single_tenant, role_config,
    role_metrics, role_ring, role_services, service_metrics::IngestPushMeasurement,
    set_prepare_shutdown, shutdown_ingester, tenant_error_response, tenant_header_value,
    unset_prepare_shutdown, validate_ingest_body_limit,
};

fn logs_ingest_span(tenant: &TenantId, body_size: ByteSize) -> tracing::Span {
    tracing::info_span!(
        "logs_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = "__krabka_observability_logs_wal",
        krabka.tenant = %tenant,
        krabka.ingest.lines = tracing::field::Empty,
        krabka.ingest.bytes = body_size.bytes_u64(),
    )
}

/// An export service with every limit turned off.
///
/// It takes a sink and a limiter and nothing else, so there is no operator
/// configuration for it to read a limit from. The service path builds its
/// export service from the distributor's push routes instead, and that one
/// carries the process's provider.
pub fn otlp_grpc_logs_service_with_limiter(
    sink: impl LogWalSink,
    ingest_limiter: impl LogIngestLimiter,
) -> OtlpGrpcLogsService {
    OtlpGrpcLogsService {
        sink: Arc::new(sink),
        ingest_limiter: Arc::new(ingest_limiter),
        overrides: Arc::new(OverridesProvider::new(Limits::unenforced())),
        wal_append_timeout: None,
        metrics: ServiceMetrics::new(),
    }
}

pub(crate) fn with_role_ops_routes<S>(
    mut router: Router<S>,
    ops: RoleOps,
    readiness: RoleReadiness,
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

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoEntry {
    #[prost(message, optional, tag = "1")]
    pub(crate) timestamp: Option<LokiProtoTimestamp>,
    #[prost(string, tag = "2")]
    pub(crate) line: String,
    #[prost(message, repeated, tag = "3")]
    pub(crate) structured_metadata: Vec<LokiProtoLabelPair>,
    #[prost(message, repeated, tag = "4")]
    pub(crate) parsed: Vec<LokiProtoLabelPair>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoLabelPair {
    #[prost(string, tag = "1")]
    pub(crate) name: String,
    #[prost(string, tag = "2")]
    pub(crate) value: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoPushRequest {
    #[prost(message, repeated, tag = "1")]
    pub(crate) streams: Vec<LokiProtoStream>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoStream {
    #[prost(string, tag = "1")]
    pub(crate) labels: String,
    #[prost(message, repeated, tag = "2")]
    pub(crate) entries: Vec<LokiProtoEntry>,
    #[prost(uint64, tag = "3")]
    pub(crate) hash: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct LokiProtoTimestamp {
    #[prost(int64, tag = "1")]
    pub(crate) seconds: i64,
    #[prost(int32, tag = "2")]
    pub(crate) nanos: i32,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LokiPushRequest {
    #[serde(default)]
    pub(crate) streams: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LokiPushStream {
    #[serde(default)]
    pub(crate) stream: Option<Labels>,
    #[serde(default)]
    pub(crate) values: Option<Vec<Value>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LokiTypedPushRequest {
    pub(crate) streams: Vec<LokiPushStream>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum OtlpAnyValue {
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
pub(crate) struct OtlpArrayValue {
    pub(crate) values: Option<Vec<OtlpAnyValue>>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct OtlpKeyValue {
    pub(crate) key: String,
    pub(crate) value: OtlpAnyValue,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct OtlpKeyValueList {
    pub(crate) values: Option<Vec<OtlpKeyValue>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtlpLogRecord {
    #[serde(default)]
    pub(crate) time_unix_nano: Value,
    #[serde(default)]
    pub(crate) observed_time_unix_nano: Option<Value>,
    #[serde(default)]
    pub(crate) severity_number: Option<Value>,
    #[serde(default)]
    pub(crate) severity_text: Option<String>,
    #[serde(default)]
    pub(crate) trace_id: Option<String>,
    #[serde(default)]
    pub(crate) span_id: Option<String>,
    pub(crate) body: Option<OtlpAnyValue>,
    pub(crate) attributes: Option<Vec<OtlpKeyValue>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtlpLogsRequest {
    pub(crate) resource_logs: Vec<OtlpResourceLogs>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OtlpResource {
    pub(crate) attributes: Option<Vec<OtlpKeyValue>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtlpResourceLogs {
    pub(crate) resource: Option<OtlpResource>,
    pub(crate) scope_logs: Vec<OtlpScopeLogs>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OtlpScope {
    #[serde(default)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) version: String,
    pub(crate) attributes: Option<Vec<OtlpKeyValue>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OtlpScopeLogs {
    pub(crate) scope: Option<OtlpScope>,
    pub(crate) log_records: Vec<OtlpLogRecord>,
}

/// The ops identity a `--target all` process presents.
///
/// `Loki`'s single binary reports `target: all` and runs every module, and a
/// runbook pointed at this process should read the same. The ring page is the
/// distributor's, because that is the ring a `Loki` operator looks for on a
/// process that accepts pushes.
pub(crate) const ALL_OPS: RoleOps = RoleOps {
    target: "all",
    ring_component: "krabka-all",
    role_ring_path: Some("/distributor/ring"),
};

/// One authorized push request whose body the distributor has normalized.
pub(crate) struct NormalizedPush<'a> {
    pub(crate) state: &'a DistributorState,
    pub(crate) security: &'a RequestSecurity,
    pub(crate) tenant: &'a TenantId,
    /// The request's body size and start, before the body decoded.
    pub(crate) measurement: IngestPushMeasurement,
}

/// Appends a normalized push's `records` to the WAL and records the response,
/// with the push's line count, on the ingest metrics and the request span.
///
/// `on_append_error` sees a WAL append failure before it becomes the response.
pub(crate) async fn append_and_record_push(
    push: NormalizedPush<'_>,
    records: Vec<WalLogRecord>,
    on_append_error: impl FnOnce(&DistributorError),
) -> Response {
    let NormalizedPush {
        state,
        security,
        tenant,
        measurement,
    } = push;
    let items = records.len() as u64;
    tracing::Span::current().record("krabka.ingest.lines", items);
    state.metrics.record_ingest_lines(tenant.as_str(), items);
    let resp = match append_distributor_wal_records(state, security, tenant, records).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => {
            on_append_error(&error);
            error.into_response()
        }
    };
    record_ingest_response(state, resp, measurement.with_items(items))
}

/// The ops identity the block builder presents on the `Loki` compat surface.
///
/// The role is `block-builder`; the strings here say `compactor`, and that is
/// deliberate. `Loki` has no block builder. The stage that writes durable
/// storage there is its compactor, and a `Loki` runbook, dashboard or
/// datasource reads `loki_boltdb_shipper_compactor_running` and the
/// `/compactor/ring` path to find it. Renaming these would rename a wire
/// contract to match an internal one, which is the wrong way round: the role
/// vocabulary is Krabka's, and these three strings are `Loki`'s.
pub(crate) const BLOCK_BUILDER_OPS: RoleOps = RoleOps {
    target: "compactor",
    ring_component: "krabka-compactor",
    role_ring_path: Some("/compactor/ring"),
};

pub(crate) const DISTRIBUTOR_OPS: RoleOps = RoleOps {
    target: "distributor",
    ring_component: "krabka-distributor",
    role_ring_path: Some("/distributor/ring"),
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
        overrides: Arc::clone(&state.overrides),
        wal_append_timeout: state.wal_append_timeout,
        metrics: state.metrics.clone(),
    };

    // `dskit`'s auth middleware guards the HTTP push routes, so a push without
    // `X-Scope-OrgID` gets 401 before its body is read. The gRPC export
    // resolves its tenant from metadata instead.
    let push_routes = Router::new()
        .route("/loki/api/v1/push", post(push_logs))
        .route("/api/prom/push", post(push_logs))
        .route("/v1/logs", post(push_otlp_logs))
        .route("/otlp/v1/logs", post(push_otlp_logs))
        .route_layer(axum::middleware::from_fn(require_org_id));

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
        .route_service(
            "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            LogsServiceServer::new(grpc_logs_service),
        )
        .merge(push_routes)
        .with_state(state)
}

/// A push router with every limit turned off.
///
/// It takes a sink and nothing else, so there is no operator configuration for
/// it to read a limit from. Use
/// [`distributor_router_with_overrides`](super::distributor_router_with_overrides)
/// to give it a tenant's limits.
pub fn distributor_router(sink: impl LogWalSink) -> Router {
    distributor_router_with_overrides(sink, OverridesProvider::new(Limits::unenforced()))
}

/// A push router that resolves each tenant's limits through `overrides`.
pub fn distributor_router_with_overrides(
    sink: impl LogWalSink,
    overrides: OverridesProvider,
) -> Router {
    distributor_router_with_sink(
        Arc::new(sink),
        Arc::new(AllowAllIngestLimiter),
        Arc::new(overrides),
        None,
        ServiceMetrics::new(),
    )
}

pub(crate) fn distributor_router_with_sink(
    sink: Arc<dyn LogWalSink>,
    ingest_limiter: Arc<dyn LogIngestLimiter>,
    overrides: Arc<OverridesProvider>,
    wal_append_timeout: Option<Time>,
    metrics: ServiceMetrics,
) -> Router {
    // The one gate this role owns. It starts met -- a distributor that has
    // bound its listener can take writes -- and an operator's drain request
    // drops it, which is the only thing `/ready` on this role reports.
    let readiness = RoleReadiness::new();
    let accepting_writes = readiness.gate(DRAINING_GATE);
    accepting_writes.mark_ready();

    with_role_ops_routes(Router::new(), DISTRIBUTOR_OPS, readiness)
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .merge(distributor_push_routes(DistributorState {
            sink,
            ingest_limiter,
            prepare_shutdown: accepting_writes,
            overrides,
            wal_append_timeout,
            metrics,
        }))
}

#[derive(Clone)]
pub struct DistributorState {
    pub(crate) sink: Arc<dyn LogWalSink>,
    pub(crate) ingest_limiter: Arc<dyn LogIngestLimiter>,
    /// The readiness gate an operator's drain request drops.
    ///
    /// See [`DRAINING_GATE`](crate::DRAINING_GATE): marking it unmet is what
    /// `/ingester/prepare_shutdown` does instead of leaving a ring, and it is
    /// what makes `/ready` answer 503 while a pod drains.
    pub(crate) prepare_shutdown: ReadinessGate,
    /// The one provider this service resolves every tenant's limits through.
    ///
    /// It is shared with the querier, so an ingest gate and a read gate answer
    /// the same tenant with the same numbers.
    pub(crate) overrides: Arc<OverridesProvider>,
    pub(crate) wal_append_timeout: Option<Time>,
    pub(crate) metrics: ServiceMetrics,
}

impl DistributorState {
    /// The limits that apply to one push.
    ///
    /// The ingest byte rate is not among them. The broker owns that quota, and
    /// `BrokerBackedIngestLimiter` enforces it. See [`crate::Limits`].
    pub(crate) fn limits_for(&self, tenant: &TenantId) -> &Limits {
        self.overrides.for_tenant(tenant)
    }
}

#[derive(Clone)]
pub struct OtlpGrpcLogsService {
    pub(crate) sink: Arc<dyn LogWalSink>,
    pub(crate) ingest_limiter: Arc<dyn LogIngestLimiter>,
    /// The same provider the HTTP push routes carry.
    ///
    /// The gRPC export path is an ingest door like the others, so it answers a
    /// tenant with that tenant's limits and not with none.
    pub(crate) overrides: Arc<OverridesProvider>,
    pub(crate) wal_append_timeout: Option<Time>,
    pub(crate) metrics: ServiceMetrics,
}

pub fn otlp_grpc_logs_service(sink: impl LogWalSink) -> OtlpGrpcLogsService {
    otlp_grpc_logs_service_with_limiter(sink, AllowAllIngestLimiter)
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcLogsService {
    async fn export(
        &self,
        request: tonic::Request<ProtoExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ProtoExportLogsServiceResponse>, tonic::Status> {
        let parts = request.into_parts();
        let metadata = parts.0;
        let extensions = parts.1;
        let payload = parts.2;
        // The router serves this service, so the authentication layer has put
        // the principal into the same extensions as for an HTTP request.
        let security = RequestSecurity::from_extensions(&extensions)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let tenant = grpc_tenant(&metadata)?;
        security
            .authorize_tenant(&tenant)
            .map_err(|denied| tonic::Status::permission_denied(denied.to_string()))?;
        // One resolution for the whole export, as the HTTP push handlers do.
        let limits = self.overrides.for_tenant(&tenant).clone();
        // The export is this door's request body, so the body cap applies to
        // it. `encoded_len` is what the same message occupied on the wire.
        validate_ingest_body_limit(&limits, measured_size(payload.encoded_len()))
            .map_err(|error| distributor_error_to_grpc_status(&error))?;
        let records = normalize_otlp_proto_logs_for_tenant(&tenant, payload, &limits)
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        // A state for this one append. The gRPC export path has no drain
        // route of its own, so the gate it carries is met and nothing drops it.
        let accepting_writes = ReadinessGate::unmet(DRAINING_GATE);
        accepting_writes.mark_ready();
        let state = DistributorState {
            sink: Arc::clone(&self.sink),
            ingest_limiter: Arc::clone(&self.ingest_limiter),
            prepare_shutdown: accepting_writes,
            overrides: Arc::clone(&self.overrides),
            wal_append_timeout: self.wal_append_timeout,
            metrics: self.metrics.clone(),
        };
        append_distributor_wal_records(&state, &security, &tenant, records)
            .await
            .map_err(|error| distributor_error_to_grpc_status(&error))?;

        Ok(tonic::Response::new(ProtoExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

pub(crate) async fn push_logs(
    State(state): State<DistributorState>,
    security: RequestSecurity,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    push_logs_with_normalizer(
        state,
        security,
        headers,
        body,
        TenantErrorSurface::Push,
        normalize_loki_http_push,
        |error, _, _| error.into_response(),
        |_| {},
    )
    .await
}

// Keep the normalizer and response callbacks injectable for focused handler tests.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn push_logs_with_normalizer<E>(
    state: DistributorState,
    security: RequestSecurity,
    headers: HeaderMap,
    body: Bytes,
    error_surface: TenantErrorSurface,
    normalize: impl FnOnce(
        &TenantId,
        &HeaderMap,
        &[u8],
        &Limits,
    ) -> Result<Vec<crate::compactor::WalLogRecord>, E>,
    decode_error: impl FnOnce(E, &HeaderMap, ByteSize) -> Response,
    append_error: impl FnOnce(&crate::DistributorError),
) -> Response {
    let start = Instant::now();
    let body_size = measured_size(body.len());
    let measurement = IngestPushMeasurement::before_decode(body_size, start);
    // The tenant is resolved before anything reads the body, so a malformed
    // tenant never picks limits and never reaches the WAL.
    let tenant = match resolve_single_tenant(tenant_header_value(&headers)) {
        Ok(tenant) => tenant,
        Err(error) => {
            let response = tenant_error_response(&error, error_surface);
            return record_ingest_response(&state, response, measurement);
        }
    };
    if let Err(denied) = security.authorize_tenant(&tenant) {
        return record_ingest_response(&state, denied.into_response(), measurement);
    }
    // ONE server span per push request (not per log line): wraps the whole
    // ingest body so the produce-side WAL append (which injects `traceparent`)
    // and downstream compaction stitch onto this trace. `krabka.ingest.lines`
    // is unknown until normalization, so it is recorded on the span below.
    let span = logs_ingest_span(&tenant, body_size);
    async move {
        // One resolution for the whole push: the body cap, the line cap, the
        // label caps and the two timestamp windows all come from this set.
        let limits = state.limits_for(&tenant).clone();
        if let Err(error) = validate_ingest_body_limit(&limits, body_size) {
            return record_ingest_response(&state, error.into_response(), measurement);
        }
        let resp = match normalize(&tenant, &headers, body.as_ref(), &limits) {
            Ok(records) => {
                return append_and_record_push(
                    NormalizedPush {
                        state: &state,
                        security: &security,
                        tenant: &tenant,
                        measurement,
                    },
                    records,
                    append_error,
                )
                .await;
            }
            Err(error) => decode_error(error, &headers, body_size),
        };
        record_ingest_response(&state, resp, measurement)
    }
    .instrument(span)
    .await
}

pub(crate) async fn push_otlp_logs(
    State(state): State<DistributorState>,
    security: RequestSecurity,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    push_logs_with_normalizer(
        state,
        security,
        headers,
        body,
        TenantErrorSurface::OtlpPush,
        normalize_otlp_http_logs,
        |error, headers, body_size| {
            tracing::debug!(
                error = %error,
                content_type = ?headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok()),
                content_encoding = ?headers.get(CONTENT_ENCODING).and_then(|value| value.to_str().ok()),
                bytes = body_size.bytes_u64(),
                "OTLP logs: decode/normalize rejected the request"
            );
            otlp_http_error_response(error)
        },
        |error| tracing::debug!(error = %error, "OTLP logs: WAL append failed"),
    )
    .await
}

pub(crate) const QUERIER_OPS: RoleOps = RoleOps {
    target: "querier",
    ring_component: "krabka-querier",
    role_ring_path: None,
};

#[derive(Clone, Copy)]
pub(crate) struct RoleOps {
    pub(crate) target: &'static str,
    pub(crate) ring_component: &'static str,
    pub(crate) role_ring_path: Option<&'static str>,
}
