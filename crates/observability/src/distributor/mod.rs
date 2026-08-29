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

/// Records one push-handler ingest outcome from the response status and returns
/// the response unchanged.
///
/// `ok` is true for any 2xx. The WAL/produce failure counter is bumped
/// separately at the [`append_distributor_wal_records`] error site, so a 4xx
/// validation or quota reject here does not inflate it.
fn record_ingest_response(
    state: &DistributorState,
    resp: Response,
    body: ByteSize,
    items: u64,
    start: Instant,
) -> Response {
    let ok = resp.status().is_success();
    state
        .metrics
        .record_ingest(ok, body, items, start.elapsed().as_time());
    resp
}

/// A measured length, as a byte quantity.
fn measured_size(len: usize) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(len).unwrap_or(u64::MAX))
}

fn otlp_http_error_response(error: DistributorError) -> Response {
    if matches!(
        error,
        DistributorError::TimestampTooOld { .. } | DistributorError::TimestampTooNew { .. }
    ) {
        return (
            StatusCode::BAD_REQUEST,
            [("content-type", "application/x-protobuf")],
            encode_otlp_status_message(&error.to_string()),
        )
            .into_response();
    }

    error.into_response()
}

fn encode_otlp_status_message(message: &str) -> Vec<u8> {
    let message = message.trim_end_matches('\n').as_bytes();
    let mut body = vec![0x12];
    encode_varint(message.len() as u64, &mut body);
    body.extend_from_slice(message);
    body
}

fn encode_varint(mut value: u64, body: &mut Vec<u8>) {
    while value >= 0x80 {
        // `|` against `^` is a permanent mutation survivor here: the masked
        // byte has its top bit clear, so setting it and flipping it agree.
        body.push(u8::try_from(value & 0x7f).expect("masked varint byte fits in u8") | 0x80);
        value >>= 7;
    }
    body.push(u8::try_from(value).expect("final varint byte fits in u8"));
}

fn validate_ingest_body_limit(
    state: &DistributorState,
    body: ByteSize,
) -> Result<(), DistributorError> {
    let Some(max) = state.max_ingest_body else {
        return Ok(());
    };
    if body > max {
        // The error carries plain integers so its rendered message is fixed by
        // the `#[error]` format string alone.
        return Err(DistributorError::IngestBodyTooLarge {
            body_bytes: body.bytes_usize(),
            max_bytes: max.bytes_usize(),
        });
    }
    Ok(())
}

async fn append_wal_records(
    sink: &dyn LogWalSink,
    records: Vec<WalLogRecord>,
) -> Result<(), WalSinkError> {
    for record in records {
        sink.append(record).await?;
    }
    Ok(())
}

async fn append_distributor_wal_records(
    state: &DistributorState,
    records: Vec<WalLogRecord>,
) -> Result<(), DistributorError> {
    // A quota/rate-limit reject is a 4xx client error, NOT a WAL-append
    // failure, so it must not bump the WAL failure counter.
    check_ingest_quota(state.ingest_limiter.as_ref(), &records).await?;
    let result = if let Some(timeout) = state.wal_append_timeout {
        match tokio::time::timeout(
            timeout.to_std(),
            append_wal_records(state.sink.as_ref(), records),
        )
        .await
        {
            Ok(inner) => inner.map_err(DistributorError::from),
            Err(_) => Err(DistributorError::WalAppendTimeout),
        }
    } else {
        append_wal_records(state.sink.as_ref(), records)
            .await
            .map_err(DistributorError::from)
    };
    // Bump the WAL/produce append-failure counter only at the actual append
    // error site (timeout or sink error), never on a 4xx validation/quota
    // reject handled above or upstream.
    if result.is_err() {
        state.metrics.record_wal_append_failure();
    }
    result
}

async fn check_ingest_quota(
    limiter: &dyn LogIngestLimiter,
    records: &[WalLogRecord],
) -> Result<(), DistributorError> {
    let Some(first) = records.first() else {
        return Ok(());
    };
    limiter
        .check(&first.tenant, records)
        .await
        .map_err(DistributorError::IngestQuota)
}

fn normalize_loki_http_push(
    headers: &HeaderMap,
    body: &[u8],
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let body = decode_loki_http_body(headers, body)?;
    if is_loki_json_content_type(headers)? {
        let raw_payload: Value =
            serde_json::from_slice(&body).map_err(|_| DistributorError::InvalidPushPayload)?;
        if raw_payload.is_null() {
            return Err(DistributorError::NoValidStreams);
        }
        if !raw_payload.is_object() {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_payload_parse_error(&body),
            ));
        }
        let payload =
            serde_json::from_slice(&body).map_err(|_| DistributorError::InvalidPushPayload)?;
        let payload = validate_loki_json_push_stream_objects(payload, &body)?;
        validate_loki_json_push_value_arrays(&payload, &body)?;
        validate_loki_json_push_timestamp_types(&payload, &body)?;
        validate_loki_json_structured_metadata_value_types(&payload, &body)?;
        normalize_loki_push(
            headers,
            payload,
            reject_old_samples_max_age,
            creation_grace_period,
        )
    } else {
        let decompressed = SnappyDecoder::new()
            .decompress_vec(&body)
            .map_err(DistributorError::LokiSnappyDecode)?;
        let payload = LokiProtoPushRequest::decode(decompressed.as_slice())
            .map_err(DistributorError::LokiDecode)?;
        normalize_loki_proto_push(
            headers,
            payload,
            reject_old_samples_max_age,
            creation_grace_period,
        )
    }
}

fn validate_loki_json_push_stream_objects(
    payload: LokiPushRequest,
    body: &[u8],
) -> Result<LokiTypedPushRequest, DistributorError> {
    let Some(streams) = payload.streams else {
        return Err(DistributorError::NoValidStreams);
    };
    let Some(raw_streams) = streams.as_array() else {
        return Err(DistributorError::InvalidJsonPushValueSyntax(
            loki_json_push_streams_parse_error(body, &streams),
        ));
    };
    if raw_streams.is_empty() {
        return Err(DistributorError::NoValidStreams);
    }
    let mut streams = Vec::with_capacity(raw_streams.len());
    for stream in raw_streams {
        if !stream.is_object() {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_stream_parse_error(body, stream),
            ));
        }
        if let Some(labels) = stream.get("stream")
            && !labels.is_object()
        {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_labels_field_parse_error(body),
            ));
        }
        if let Some(values) = stream.get("values")
            && !values.is_array()
            && !values.is_null()
        {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_values_field_parse_error(body, values),
            ));
        }
        let stream = serde_json::from_value(stream.clone())
            .map_err(|_| DistributorError::InvalidPushPayload)?;
        streams.push(stream);
    }

    Ok(LokiTypedPushRequest { streams })
}

fn validate_loki_json_push_value_arrays(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    for stream in &payload.streams {
        let Some(values) = &stream.values else {
            continue;
        };
        for value in values {
            if !value.is_array() {
                return Err(DistributorError::InvalidJsonPushValueSyntax(
                    loki_json_push_value_parse_error(body, value),
                ));
            }
        }
    }

    Ok(())
}

fn validate_loki_json_push_timestamp_types(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    for stream in &payload.streams {
        let Some(values) = &stream.values else {
            continue;
        };
        for value in values {
            let Some(timestamp) = value.get(0) else {
                continue;
            };
            if !timestamp.is_string() {
                return Err(DistributorError::InvalidJsonTimestampSyntax(
                    loki_json_timestamp_value_parse_error(body, timestamp, value.get(1)),
                ));
            }
        }
    }

    Ok(())
}

fn loki_json_push_value_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(10));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(30));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_payload_parse_error(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let value_start = body
        .char_indices()
        .find_map(|(index, char)| (!char.is_whitespace()).then_some(index))
        .unwrap_or(body.len());
    let found = body[value_start..].chars().next().unwrap_or('\0');
    let context_start = previous_char_boundary(&body, value_start);
    let context_end = previous_char_boundary(&body, body.len().min(context_start + 11));
    let context = &body[context_start..context_end];
    let bigger_context = loki_decode_error_context(&body, value_start);

    format!(
        "readObjectStart: expect {{ or n, but found {found}, error found in #1 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_values_field_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(37));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_stream_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_add(4));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(12));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_labels_field_parse_error(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let context = loki_decode_error_context(&body, body.len().saturating_sub(12));
    let bigger_context = loki_decode_error_context(&body, body.len().saturating_sub(52));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_push_streams_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context_start = previous_char_boundary(&body, value_start.saturating_sub(9));
    let context_end = previous_char_boundary(&body, body.len().min(context_start + 20));
    let context = &body[context_start..context_end];
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(11));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: decode slice: expect [ or n, but found \", error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn validate_loki_json_structured_metadata_value_types(
    payload: &LokiTypedPushRequest,
    body: &[u8],
) -> Result<(), DistributorError> {
    for stream in &payload.streams {
        let Some(values) = &stream.values else {
            continue;
        };
        for value in values {
            let Some(metadata_value) = value.get(2) else {
                continue;
            };
            let Value::Object(metadata) = metadata_value else {
                return Err(DistributorError::InvalidStructuredMetadataSyntax(
                    loki_structured_metadata_object_parse_error(body, metadata_value),
                ));
            };
            if let Some((name, value)) = metadata.iter().find(|(_, value)| !value.is_string()) {
                return Err(DistributorError::InvalidStructuredMetadataSyntax(
                    loki_structured_metadata_value_parse_error(body, name, value),
                ));
            }
        }
    }

    Ok(())
}

fn loki_structured_metadata_object_parse_error(body: &[u8], value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let value_text = value.to_string();
    let value_start = body.find(&value_text).unwrap_or(body.len());
    let context = loki_decode_error_context(&body, value_start.saturating_sub(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(43));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object, but can't find closing '}}' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_structured_metadata_value_parse_error(body: &[u8], name: &str, value: &Value) -> String {
    let body = String::from_utf8_lossy(body);
    let key = quote_logql_string(name);
    let needle = format!("{key}:{value}");
    let value_start = body.find(&needle).map_or_else(
        || body.find(&value.to_string()).unwrap_or(body.len()),
        |offset| offset + key.len() + 1,
    );
    let context = loki_decode_error_context(&body, value_start.saturating_sub(3));
    let bigger_context = loki_decode_error_context(&body, value_start.saturating_sub(43));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|{context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_decode_error_context(body: &str, start: usize) -> &str {
    let start = previous_char_boundary(body, start.min(body.len()));
    let end = previous_char_boundary(body, body.len().min(start + 80));
    &body[start..end]
}

fn previous_char_boundary(value: &str, mut offset: usize) -> usize {
    while !value.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

fn decode_loki_http_body(headers: &HeaderMap, body: &[u8]) -> Result<Vec<u8>, DistributorError> {
    let Some(encoding) = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(body.to_vec());
    };
    let encoding = encoding.trim();

    if encoding.is_empty() || encoding.eq_ignore_ascii_case("snappy") {
        return Ok(body.to_vec());
    } else if encoding.eq_ignore_ascii_case("gzip") {
        let mut decoder = GzDecoder::new(body);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(DistributorError::LokiGzipDecode)?;
        return Ok(decompressed);
    } else if encoding.eq_ignore_ascii_case("deflate") {
        let mut decoder = DeflateDecoder::new(body);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(DistributorError::LokiDeflateDecode)?;
        return Ok(decompressed);
    }

    Err(DistributorError::UnsupportedLokiContentEncoding(
        encoding.to_string(),
    ))
}

fn normalize_otlp_http_logs(
    headers: &HeaderMap,
    body: &[u8],
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    // OTLP/HTTP clients (e.g. the OpenTelemetry SDK's otlphttp exporter, which
    // defaults to gzip) honour Content-Encoding just like the Loki push path, so
    // decompress before decode. Without this, a gzip body reaches the protobuf
    // decoder as raw deflate stream bytes and fails to parse.
    let body = decode_loki_http_body(headers, body)?;
    let body = body.as_slice();

    if is_protobuf_content_type(headers) {
        let payload =
            ProtoExportLogsServiceRequest::decode(body).map_err(DistributorError::OtlpDecode)?;
        return normalize_otlp_proto_logs(
            headers,
            payload,
            reject_old_samples_max_age,
            creation_grace_period,
        );
    }

    let payload = serde_json::from_slice(body).map_err(|_| DistributorError::InvalidOtlpPayload)?;
    normalize_otlp_logs(
        headers,
        payload,
        reject_old_samples_max_age,
        creation_grace_period,
    )
}

fn is_protobuf_content_type(headers: &HeaderMap) -> bool {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    content_type.split(';').next().is_some_and(|content_type| {
        matches!(
            content_type.trim(),
            "application/x-protobuf" | "application/protobuf"
        )
    })
}

fn is_loki_json_content_type(headers: &HeaderMap) -> Result<bool, DistributorError> {
    let Some(content_type) = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(false);
    };
    let content_type = content_type.trim();
    if content_type.is_empty() {
        return Ok(false);
    }

    let mut parts = content_type.split(';');
    let media_type = parts.next().unwrap_or_default().trim();
    if media_type.is_empty() {
        return Err(DistributorError::InvalidLokiContentType(
            content_type.to_string(),
        ));
    }

    let mut parameters = parts.peekable();
    while let Some(parameter) = parameters.next() {
        let parameter = parameter.trim();
        if parameter.is_empty() && parameters.peek().is_none() {
            continue;
        }
        let Some((name, value)) = parameter.split_once('=') else {
            return Err(DistributorError::InvalidLokiContentType(
                content_type.to_string(),
            ));
        };
        if name.trim().is_empty() || value.trim().is_empty() {
            return Err(DistributorError::InvalidLokiContentType(
                content_type.to_string(),
            ));
        }
    }

    Ok(media_type.eq_ignore_ascii_case("application/json"))
}

fn normalize_loki_push(
    headers: &HeaderMap,
    payload: LokiTypedPushRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
    let mut records = Vec::new();

    for stream in payload.streams {
        let Some(original_stream_labels) = stream.stream else {
            continue;
        };
        validate_loki_stream_labels(&original_stream_labels)?;
        let mut stream_labels = original_stream_labels.clone();
        discover_service_name_label(&mut stream_labels);

        let Some(values) = stream.values else {
            continue;
        };
        for value in values {
            let Some(value) = value.as_array() else {
                return Err(DistributorError::InvalidPushValue);
            };
            let zero_timestamp;
            let (timestamp, line, metadata, is_empty_value) = match value.as_slice() {
                [timestamp] => (timestamp, "", [].as_slice(), false),
                [timestamp, line, metadata @ ..] => (
                    timestamp,
                    line.as_str().ok_or_else(|| {
                        DistributorError::InvalidJsonLineSyntax(loki_json_line_parse_error(
                            &original_stream_labels,
                            timestamp.as_str().unwrap_or_default(),
                            line,
                        ))
                    })?,
                    metadata,
                    false,
                ),
                [] => {
                    zero_timestamp = Value::String("0".to_string());
                    (&zero_timestamp, "", [].as_slice(), true)
                }
            };
            let timestamp = timestamp
                .as_str()
                .ok_or(DistributorError::InvalidTimestamp)?;
            let timestamp_ns = timestamp.parse().map_err(|_| {
                DistributorError::InvalidJsonTimestampSyntax(loki_json_timestamp_parse_error(
                    timestamp, line,
                ))
            })?;
            let timestamp_ns = validate_ingest_timestamp_ns(timestamp_ns)?;
            if is_empty_value {
                validate_loki_empty_json_value_timestamp_window(
                    &stream_labels,
                    reject_old_samples_max_age,
                )?;
            }
            validate_loki_timestamp_window(
                timestamp_ns,
                &stream_labels,
                reject_old_samples_max_age,
                creation_grace_period,
            )?;
            let labels = loki_push_entry_labels(&stream_labels, line);

            records.push(WalLogRecord {
                tenant: tenant.clone(),
                labels,
                timestamp_ns,
                line: line.to_string(),
                structured_metadata: parse_structured_metadata(metadata.first())?,
                position: None,
            });
        }
    }

    Ok(records)
}

fn validate_loki_empty_json_value_timestamp_window(
    stream_labels: &Labels,
    max_age: Option<Time>,
) -> Result<(), DistributorError> {
    let Some(max_age) = max_age else {
        return Ok(());
    };
    let oldest_acceptable_timestamp_ns = current_unix_time_ns().saturating_sub(max_age.nanos_i64());
    Err(DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    })
}

fn normalize_loki_proto_push(
    headers: &HeaderMap,
    payload: LokiProtoPushRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
    let mut records = Vec::new();

    for stream in payload.streams {
        let mut stream_labels = parse_loki_proto_labels(&stream.labels)?;
        validate_loki_stream_labels(&stream_labels)?;
        discover_service_name_label(&mut stream_labels);

        for entry in stream.entries {
            let timestamp_ns = if let Some(timestamp) = entry.timestamp.as_ref() {
                loki_proto_timestamp_ns(Some(timestamp))?
            } else {
                return Err(loki_missing_proto_timestamp_error(
                    &stream_labels,
                    reject_old_samples_max_age,
                ));
            };
            validate_loki_timestamp_window(
                timestamp_ns,
                &stream_labels,
                reject_old_samples_max_age,
                creation_grace_period,
            )?;
            let labels = loki_push_entry_labels(&stream_labels, &entry.line);
            records.push(WalLogRecord {
                tenant: tenant.clone(),
                labels,
                timestamp_ns,
                line: entry.line,
                structured_metadata: loki_proto_label_pairs_to_labels(&entry.structured_metadata),
                position: None,
            });
        }
    }

    if records.is_empty() {
        return Err(DistributorError::NoValidStreams);
    }

    Ok(records)
}

fn loki_push_entry_labels(stream_labels: &Labels, line: &str) -> Labels {
    let mut labels = stream_labels.clone();
    discover_detected_level_label(&mut labels, line);
    labels
}

fn normalize_otlp_logs(
    headers: &HeaderMap,
    payload: OtlpLogsRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let resource_labels = otlp_attributes_to_labels(
            resource_logs
                .resource
                .as_ref()
                .and_then(|resource| resource.attributes.as_deref()),
        )?;

        for scope_logs in resource_logs.scope_logs {
            let mut labels = resource_labels.clone();
            labels.extend(otlp_attributes_to_labels(
                scope_logs
                    .scope
                    .as_ref()
                    .and_then(|scope| scope.attributes.as_deref()),
            )?);
            discover_service_name_label(&mut labels);
            if labels.is_empty() {
                return Err(DistributorError::EmptyStreamLabels);
            }

            for log_record in scope_logs.log_records {
                let timestamp_ns = otlp_timestamp_ns(&log_record.time_unix_nano)?;
                validate_loki_timestamp_window(
                    timestamp_ns,
                    &labels,
                    reject_old_samples_max_age,
                    creation_grace_period,
                )?;
                records.push(WalLogRecord {
                    tenant: tenant.clone(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line: log_record
                        .body
                        .as_ref()
                        .map(otlp_value_to_string)
                        .unwrap_or_default(),
                    structured_metadata: otlp_log_record_structured_metadata(&log_record)?,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}

fn loki_json_timestamp_parse_error(timestamp: &str, line: &str) -> String {
    let found_context = timestamp
        .char_indices()
        .nth(9)
        .map_or(timestamp, |(offset, _)| &timestamp[offset..]);
    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|{found_context}\"]]}}]}}|..., bigger context ...|s\":[[\"{timestamp}\",\"{line}\"]]}}]}}|...\n"
    )
}

fn loki_json_timestamp_value_parse_error(
    body: &[u8],
    timestamp: &Value,
    line: Option<&Value>,
) -> String {
    let body = String::from_utf8_lossy(body);
    let timestamp_text = timestamp.to_string();
    let value_start = body.find(&timestamp_text).unwrap_or(body.len());
    let found_context = line.and_then(Value::as_str).map_or_else(
        || loki_decode_error_context(&body, value_start.saturating_add(10)).to_string(),
        |line| {
            let start = line
                .char_indices()
                .nth(line.chars().count().saturating_sub(6))
                .map_or(0, |(offset, _)| offset);
            format!("{}\"]]}}]}}", &line[start..])
        },
    );
    let context_prefix_len = if timestamp.is_array() {
        10
    } else if timestamp.is_object() {
        4
    } else {
        9
    };
    let bigger_context =
        loki_decode_error_context(&body, value_start.saturating_sub(context_prefix_len));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|{found_context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

fn loki_json_line_parse_error(stream_labels: &Labels, timestamp: &str, line: &Value) -> String {
    let line = line.to_string();
    let found_context = format!(
        "{}\",{}]]}}]}}",
        timestamp
            .char_indices()
            .nth(timestamp.chars().count().saturating_sub(2))
            .map_or(timestamp, |(offset, _)| &timestamp[offset..]),
        line
    );
    let labels = serde_json::to_string(stream_labels).unwrap_or_else(|_| "{}".to_string());
    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|{found_context}|..., bigger context ...|ream\":{labels},\"values\":[[\"{timestamp}\",{line}]]}}]}}|...\n"
    )
}

fn validate_loki_stream_labels(labels: &Labels) -> Result<(), DistributorError> {
    if let Some(name) = labels.keys().find(|name| !is_loki_label_name(name)) {
        return Err(DistributorError::InvalidPushLabelSyntax(
            loki_push_label_parse_error(labels, name),
        ));
    }
    Ok(())
}

fn loki_push_label_parse_error(labels: &Labels, invalid_name: &str) -> String {
    let rendered = loki_label_set(labels);
    let name_start = rendered.find(invalid_name).unwrap_or(1);
    let invalid_offset = invalid_name
        .char_indices()
        .find_map(|(offset, value)| {
            (!is_loki_label_name_char(value, offset == 0)).then_some(offset)
        })
        .unwrap_or(0);
    let column = name_start + invalid_offset + 1;
    let unexpected = invalid_name[invalid_offset..].chars().next().unwrap_or('}');
    format!(
        "couldn't parse labels: 1:{column}: parse error: unexpected character inside braces: '{unexpected}'\n"
    )
}

fn loki_label_set(labels: &Labels) -> String {
    let values = labels
        .iter()
        .map(|(name, value)| format!("{name}={}", quote_logql_string(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{values}}}")
}

fn is_loki_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    is_loki_label_name_char(first, true) && chars.all(|value| is_loki_label_name_char(value, false))
}

fn is_loki_label_name_char(value: char, first: bool) -> bool {
    value == '_' || value.is_ascii_alphabetic() || (!first && value.is_ascii_digit())
}

fn parse_loki_proto_labels(labels: &str) -> Result<Labels, DistributorError> {
    let labels = labels.trim();
    if labels.is_empty() || labels == "{}" {
        return Ok(Labels::new());
    }

    let query = parse_query(labels).map_err(|_| {
        loki_proto_label_parse_error(labels).map_or(
            DistributorError::InvalidPushLabels,
            DistributorError::InvalidPushLabelSyntax,
        )
    })?;
    if !query.pipeline.is_empty() {
        return Err(DistributorError::InvalidPushLabels);
    }

    let mut labels = Labels::new();
    let mut rendered_labels = Vec::new();
    for matcher in query.matchers {
        if matcher.op != MatchOp::Equal {
            return Err(DistributorError::InvalidPushLabels);
        }
        rendered_labels.push(format!(
            "{}={}",
            matcher.name,
            quote_logql_string(&matcher.value)
        ));
        if labels.contains_key(&matcher.name) {
            let mut discovered_labels = labels.clone();
            discover_service_name_label(&mut discovered_labels);
            if !rendered_labels
                .iter()
                .any(|label| label.starts_with("service_name="))
                && let Some(service_name) = discovered_labels.get("service_name")
            {
                rendered_labels.push(format!("service_name={}", quote_logql_string(service_name)));
            }
            return Err(DistributorError::InvalidPushLabelSyntax(format!(
                "stream '{{{}}}' has duplicate label name: '{}'\n",
                rendered_labels.join(", "),
                matcher.name
            )));
        }
        labels.insert(matcher.name, matcher.value);
    }

    Ok(labels)
}

fn loki_proto_label_parse_error(labels: &str) -> Option<String> {
    let labels = labels.trim();
    let mut chars = labels.char_indices();
    if chars.next()? != (0, '{') {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut expecting_name = true;
    let mut first_name_char = true;

    for (offset, value) in chars {
        if in_string {
            if escaped {
                escaped = false;
            } else if value == '\\' {
                escaped = true;
            } else if value == '"' {
                in_string = false;
            }
            continue;
        }

        match value {
            '"' => in_string = true,
            ',' => {
                expecting_name = true;
                first_name_char = true;
            }
            // No `first_name_char` here: nothing reads it until a `,` starts
            // the next name, and that arm sets it itself.
            '=' => expecting_name = false,
            '}' => break,
            value if expecting_name && value.is_whitespace() => {}
            value if expecting_name => {
                if !is_loki_label_name_char(value, first_name_char) {
                    let column = labels[..offset].chars().count() + 1;
                    return Some(format!(
                        "couldn't parse labels: 1:{column}: parse error: unexpected character inside braces: '{value}'\n"
                    ));
                }
                first_name_char = false;
            }
            _ => {}
        }
    }

    None
}

fn loki_proto_timestamp_ns(
    timestamp: Option<&LokiProtoTimestamp>,
) -> Result<i64, DistributorError> {
    let timestamp = timestamp.ok_or(DistributorError::InvalidTimestamp)?;
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(DistributorError::InvalidTimestamp);
    }

    timestamp
        .seconds
        .checked_mul(1_000_000_000)
        .and_then(|seconds_ns| seconds_ns.checked_add(i64::from(timestamp.nanos)))
        .ok_or(DistributorError::InvalidTimestamp)
}

fn loki_missing_proto_timestamp_error(
    stream_labels: &Labels,
    max_age: Option<Time>,
) -> DistributorError {
    let max_age = max_age.unwrap_or(LOKI_REJECT_OLD_SAMPLES_MAX_AGE);
    let oldest_acceptable_timestamp_ns = current_unix_time_ns().saturating_sub(max_age.nanos_i64());
    DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    }
}

fn loki_proto_label_pairs_to_labels(labels: &[LokiProtoLabelPair]) -> Labels {
    let mut labels_by_name = Labels::new();
    for label in labels {
        labels_by_name.insert(label.name.clone(), label.value.clone());
    }
    labels_by_name
}

fn normalize_otlp_proto_logs(
    headers: &HeaderMap,
    payload: ProtoExportLogsServiceRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?;
    normalize_otlp_proto_logs_for_tenant(
        tenant,
        payload,
        reject_old_samples_max_age,
        creation_grace_period,
    )
}

fn normalize_otlp_proto_logs_for_tenant(
    tenant: &str,
    payload: ProtoExportLogsServiceRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.to_string();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let resource_labels = proto_attributes_to_labels(
            resource_logs
                .resource
                .as_ref()
                .map(|resource| resource.attributes.as_slice()),
        )?;

        for scope_logs in resource_logs.scope_logs {
            let mut labels = resource_labels.clone();
            labels.extend(proto_attributes_to_labels(
                scope_logs
                    .scope
                    .as_ref()
                    .map(|scope| scope.attributes.as_slice()),
            )?);
            discover_service_name_label(&mut labels);
            if labels.is_empty() {
                return Err(DistributorError::EmptyStreamLabels);
            }

            for log_record in scope_logs.log_records {
                let timestamp_ns = proto_timestamp_ns(
                    log_record.time_unix_nano,
                    log_record.observed_time_unix_nano,
                )?;
                validate_loki_timestamp_window(
                    timestamp_ns,
                    &labels,
                    reject_old_samples_max_age,
                    creation_grace_period,
                )?;
                records.push(WalLogRecord {
                    tenant: tenant.clone(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line: log_record
                        .body
                        .as_ref()
                        .map(proto_value_to_string)
                        .unwrap_or_default(),
                    structured_metadata: proto_log_record_structured_metadata(&log_record)?,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}

fn otlp_attributes_to_labels(
    attributes: Option<&[OtlpKeyValue]>,
) -> Result<Labels, DistributorError> {
    let mut labels = Labels::new();
    for attribute in attributes.unwrap_or_default() {
        if attribute.key.is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let name = normalize_otlp_attribute_name(&attribute.key);
        if labels
            .insert(name, otlp_value_to_string(&attribute.value))
            .is_some()
        {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(labels)
}

fn proto_attributes_to_labels(
    attributes: Option<&[ProtoKeyValue]>,
) -> Result<Labels, DistributorError> {
    let mut labels = Labels::new();
    for attribute in attributes.unwrap_or_default() {
        if attribute.key.is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let name = normalize_otlp_attribute_name(&attribute.key);
        let value = attribute
            .value
            .as_ref()
            .map(proto_value_to_string)
            .unwrap_or_default();
        if labels.insert(name, value).is_some() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(labels)
}

fn proto_log_record_structured_metadata(
    log_record: &ProtoLogRecord,
) -> Result<Labels, DistributorError> {
    let mut metadata = proto_attributes_to_labels(Some(log_record.attributes.as_slice()))?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_number",
        (log_record.severity_number != 0).then(|| log_record.severity_number.to_string()),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_text",
        (!log_record.severity_text.is_empty()).then(|| log_record.severity_text.clone()),
    )?;
    insert_proto_trace_context_metadata(&mut metadata, "trace_id", &log_record.trace_id);
    insert_proto_trace_context_metadata(&mut metadata, "span_id", &log_record.span_id);
    Ok(metadata)
}

fn otlp_log_record_structured_metadata(
    log_record: &OtlpLogRecord,
) -> Result<Labels, DistributorError> {
    let mut metadata = otlp_attributes_to_labels(log_record.attributes.as_deref())?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_number",
        log_record
            .severity_number
            .as_ref()
            .map(otlp_severity_number_to_string)
            .transpose()?,
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_text",
        log_record
            .severity_text
            .as_ref()
            .filter(|severity_text| !severity_text.is_empty())
            .cloned(),
    )?;
    Ok(metadata)
}

fn insert_metadata_if_absent(
    metadata: &mut Labels,
    name: &str,
    value: Option<String>,
) -> Result<(), DistributorError> {
    let Some(value) = value else {
        return Ok(());
    };
    if metadata.insert(name.to_string(), value).is_some() {
        return Err(DistributorError::InvalidOtlpAttribute);
    }
    Ok(())
}

fn insert_proto_trace_context_metadata(metadata: &mut Labels, name: &str, value: &[u8]) {
    if !value.is_empty() {
        metadata.insert(name.to_string(), hex_string(value));
    }
}

fn normalize_otlp_attribute_name(name: &str) -> String {
    let mut normalized = name
        .chars()
        .map(|ch| {
            if ch == '_' || ch.is_ascii_alphanumeric() {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        normalized.push('_');
    }
    if normalized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        normalized.insert(0, '_');
    }
    normalized
}

fn discover_service_name_label(labels: &mut Labels) {
    if labels.contains_key("service_name") {
        return;
    }

    let service_name = SERVICE_NAME_DISCOVERY_LABELS
        .iter()
        .filter_map(|name| labels.get(*name))
        .find(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| "unknown_service".to_string());
    labels.insert("service_name".to_string(), service_name);
}

fn discover_detected_level_label(labels: &mut Labels, line: &str) {
    if labels.contains_key("detected_level")
        || labels.contains_key("level")
        || labels.contains_key("severity")
        || labels.contains_key("severity_text")
    {
        return;
    }

    let level = detect_log_level(line);
    if let Some(level) = level {
        labels.insert("detected_level".to_string(), level.to_string());
    }
}

fn detect_log_level(line: &str) -> Option<&'static str> {
    let line = line.to_ascii_lowercase();
    for level in [
        "critical", "crit", "fatal", "error", "warn", "warning", "info", "debug", "trace",
    ] {
        if contains_log_level_token(&line, level) {
            return Some(match level {
                "crit" => "critical",
                "warning" => "warn",
                level => level,
            });
        }
    }
    None
}

fn contains_log_level_token(line: &str, level: &str) -> bool {
    line.match_indices(level).any(|(start, _)| {
        let end = start + level.len();
        let before = start
            .checked_sub(1)
            .and_then(|index| line.as_bytes().get(index))
            .copied();
        let after = line.as_bytes().get(end).copied();
        !before.is_some_and(is_log_level_word_byte) && !after.is_some_and(is_log_level_word_byte)
    })
}

fn is_log_level_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

const SERVICE_NAME_DISCOVERY_LABELS: &[&str] = &[
    "service",
    "app",
    "application",
    "name",
    "app_kubernetes_io_name",
    "container",
    "container_name",
    "component",
    "workload",
    "job",
];

fn proto_timestamp_ns(
    time_unix_nano: u64,
    observed_time_unix_nano: u64,
) -> Result<i64, DistributorError> {
    let timestamp = if time_unix_nano == 0 {
        observed_time_unix_nano
    } else {
        time_unix_nano
    };
    i64::try_from(timestamp).map_err(|_| DistributorError::InvalidTimestamp)
}

fn otlp_timestamp_ns(timestamp: &Value) -> Result<i64, DistributorError> {
    let timestamp_ns = match timestamp {
        Value::String(timestamp) => timestamp
            .parse()
            .map_err(|_| DistributorError::InvalidTimestamp),
        Value::Number(timestamp) => timestamp.as_i64().ok_or(DistributorError::InvalidTimestamp),
        _ => Err(DistributorError::InvalidTimestamp),
    }?;
    validate_ingest_timestamp_ns(timestamp_ns)
}

fn validate_ingest_timestamp_ns(timestamp_ns: i64) -> Result<i64, DistributorError> {
    if timestamp_ns < 0 {
        Err(DistributorError::InvalidTimestamp)
    } else {
        Ok(timestamp_ns)
    }
}

fn validate_loki_timestamp_window(
    timestamp_ns: i64,
    stream_labels: &Labels,
    max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<(), DistributorError> {
    validate_loki_timestamp_window_at(
        timestamp_ns,
        current_unix_time_ns(),
        stream_labels,
        max_age,
        creation_grace_period,
    )
}

/// The window check against a caller-supplied `now`.
///
/// Split out so the two bounds can be tested exactly at their edges. Both are
/// strict comparisons -- a timestamp precisely at the oldest or newest
/// acceptable value is accepted -- and against a wall clock that boundary is
/// unreachable: `now` advances between choosing the timestamp and reading it.
fn validate_loki_timestamp_window_at(
    timestamp_ns: i64,
    now_ns: i64,
    stream_labels: &Labels,
    max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<(), DistributorError> {
    if let Some(max_age) = max_age {
        let oldest_acceptable_timestamp_ns = now_ns.saturating_sub(max_age.nanos_i64());
        if timestamp_ns < oldest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooOld {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
                oldest_acceptable_timestamp_ns,
            });
        }
    }
    if let Some(creation_grace_period) = creation_grace_period {
        let newest_acceptable_timestamp_ns =
            now_ns.saturating_add(creation_grace_period.nanos_i64());
        if timestamp_ns > newest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooNew {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
            });
        }
    }
    Ok(())
}

fn loki_stale_sample_label_set(labels: &Labels) -> String {
    let values = labels
        .iter()
        .map(|(name, value)| format!("{name}={}", quote_logql_string(value)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{values}}}")
}

fn rfc3339_seconds(timestamp_ns: i64) -> String {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(seconds) else {
        return seconds.to_string();
    };
    let date = timestamp.date();
    let time = timestamp.time();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        date.year(),
        u8::from(date.month()),
        date.day(),
        time.hour(),
        time.minute(),
        time.second()
    )
}

fn otlp_severity_number_to_string(value: &Value) -> Result<String, DistributorError> {
    match value {
        Value::Number(number) => Ok(number.to_string()),
        Value::String(string) => Ok(string.clone()),
        _ => Err(DistributorError::InvalidOtlpPayload),
    }
}

fn otlp_value_to_string(value: &OtlpAnyValue) -> String {
    match value {
        OtlpAnyValue::String(value) | OtlpAnyValue::Bytes(value) => value.clone(),
        OtlpAnyValue::Bool(value) => value.to_string(),
        OtlpAnyValue::Int(value) | OtlpAnyValue::Double(value) => metadata_value_to_string(value),
        OtlpAnyValue::Array(value) => serde_json::to_string(
            &value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(otlp_value_to_json)
                .collect::<Vec<_>>(),
        )
        .expect("OTLP array values serialize to JSON"),
        OtlpAnyValue::Kvlist(value) => serde_json::to_string(
            &value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|attribute| (attribute.key.clone(), otlp_value_to_json(&attribute.value)))
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("OTLP key-value lists serialize to JSON"),
    }
}

fn otlp_value_to_json(value: &OtlpAnyValue) -> Value {
    match value {
        OtlpAnyValue::String(value) | OtlpAnyValue::Bytes(value) => Value::String(value.clone()),
        OtlpAnyValue::Bool(value) => Value::Bool(*value),
        OtlpAnyValue::Int(value) | OtlpAnyValue::Double(value) => value.clone(),
        OtlpAnyValue::Array(value) => Value::Array(
            value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(otlp_value_to_json)
                .collect(),
        ),
        OtlpAnyValue::Kvlist(value) => Value::Object(
            value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|attribute| (attribute.key.clone(), otlp_value_to_json(&attribute.value)))
                .collect(),
        ),
    }
}

fn proto_value_to_string(value: &ProtoAnyValue) -> String {
    value
        .value
        .as_ref()
        .map(proto_any_value_to_string)
        .unwrap_or_default()
}

fn proto_any_value_to_string(value: &proto_any_value::Value) -> String {
    match value {
        proto_any_value::Value::StringValue(value) => value.clone(),
        proto_any_value::Value::BoolValue(value) => value.to_string(),
        proto_any_value::Value::IntValue(value) => value.to_string(),
        proto_any_value::Value::DoubleValue(value) => value.to_string(),
        proto_any_value::Value::BytesValue(value) => hex_string(value),
        proto_any_value::Value::ArrayValue(value) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(proto_value_to_json)
                .collect::<Vec<_>>(),
        )
        .expect("OTLP protobuf array values serialize to JSON"),
        proto_any_value::Value::KvlistValue(value) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(|attribute| {
                    (
                        attribute.key.clone(),
                        attribute
                            .value
                            .as_ref()
                            .map_or(Value::Null, proto_value_to_json),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("OTLP protobuf key-value lists serialize to JSON"),
        proto_any_value::Value::StringValueStrindex(value) => value.to_string(),
    }
}

fn proto_value_to_json(value: &ProtoAnyValue) -> Value {
    match value.value.as_ref() {
        Some(proto_any_value::Value::StringValue(value)) => Value::String(value.clone()),
        Some(proto_any_value::Value::BoolValue(value)) => Value::Bool(*value),
        Some(proto_any_value::Value::IntValue(value)) => Value::Number((*value).into()),
        Some(proto_any_value::Value::DoubleValue(value)) => {
            serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
        }
        Some(proto_any_value::Value::BytesValue(value)) => Value::String(hex_string(value)),
        Some(proto_any_value::Value::ArrayValue(value)) => {
            Value::Array(value.values.iter().map(proto_value_to_json).collect())
        }
        Some(proto_any_value::Value::KvlistValue(value)) => Value::Object(
            value
                .values
                .iter()
                .map(|attribute| {
                    (
                        attribute.key.clone(),
                        attribute
                            .value
                            .as_ref()
                            .map_or(Value::Null, proto_value_to_json),
                    )
                })
                .collect(),
        ),
        Some(proto_any_value::Value::StringValueStrindex(value)) => Value::Number((*value).into()),
        None => Value::Null,
    }
}

fn hex_string(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn parse_structured_metadata(
    metadata: Option<&Value>,
) -> Result<BTreeMap<String, String>, DistributorError> {
    let Some(metadata) = metadata else {
        return Ok(BTreeMap::new());
    };
    let Value::Object(metadata) = metadata else {
        return Err(DistributorError::InvalidStructuredMetadata);
    };

    metadata
        .iter()
        .map(|(name, value)| {
            let value = value
                .as_str()
                .ok_or(DistributorError::InvalidStructuredMetadata)?;
            Ok((name.clone(), value.to_string()))
        })
        .collect::<Result<BTreeMap<_, _>, DistributorError>>()
}

fn metadata_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

