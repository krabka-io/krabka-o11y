//! Metrics distributor role. It validates a request, applies HA deduplication,
//! and appends to the WAL.

pub mod ha;

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Bytes as BodyBytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use bytes::Bytes;
use crabka_blockstore::SeriesFingerprint;
use crabka_client_consumer::{Consumer, ConsumerRecord};
use crabka_client_producer::{Header as ProducerHeader, Producer, ProducerRecord};
use crabka_ids::{Offset, PartitionIndex};
use crabka_telemetry::propagation::current_trace_headers;
use crabka_units::prelude::*;
pub use ha::{
    DEFAULT_HA_FAILOVER_TIMEOUT, HA_TRACKER_TOPIC, HaDecision, HaElection, HaElectionRecord,
    HaTracker, ha_decision, ha_election, strip_replica_label,
};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::{MetricsService, MetricsServiceServer},
    },
    metrics::v1::MetricsData,
};
use tokio::net::TcpListener;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status};
use tracing::Instrument as _;

use crate::{
    IngestEnforcer, LimitError, Limits, OverridesProvider,
    metrics::ServiceMetrics,
    otlp::{
        DeltaAccumulator, OtlpError, TranslationStrategy, decode_otlp_stateful,
        decode_otlp_stateful_bytes,
    },
    validate_tenant,
    wal::{ClockReadingPayload, SamplePayload, WAL_TOPIC, WalExemplar, WalRecord, partition_key},
    wire::{
        ClockSyncState, ClockWireError, DecodedClockReading, DecodedExemplar, DecodedSample,
        DecodedSeries, GnssFix, UnixNanos, WireError, WireFormat, WrittenCounts,
        decode_clock_readings, decode_v1, decode_v2, negotiate,
    },
};

const MAX_EXEMPLAR_LABEL_CODEPOINTS: usize = 128;
pub const DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED: ByteSize = mebibytes(32);

/// Metric name of the clock reading series itself.
///
/// The columnar clock block is the source of truth for a reading. This series
/// names it so the block rows fingerprint, index, and shard exactly as every
/// other series does.
pub const CLOCK_READING_METRIC: &str = "krabka_clock_reading";

/// Structural per-request limits enforced before WAL append.
#[derive(Clone, Debug, PartialEq)]
pub struct TenantLimits {
    pub max_label_name_len: ByteSize,
    pub max_label_value_len: ByteSize,
    pub max_samples_per_series: usize,
    pub max_series_per_request: usize,
    /// Accepted sample rate. A zero rate turns the ingestion rate limit off.
    pub ingestion_rate: Frequency,
    /// Samples the token bucket may hand out in one burst.
    pub ingestion_burst_size: usize,
    /// Accepted out-of-order ingest window. A negative extent removes the cap.
    pub out_of_order_time_window: Time,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_label_name_len: kibibytes(2),
            max_label_value_len: kibibytes(2),
            max_samples_per_series: 10_000,
            max_series_per_request: 100_000,
            ingestion_rate: per_sec(1_000_000),
            ingestion_burst_size: 1_000_000,
            out_of_order_time_window: Time::ZERO,
        }
    }
}

/// Errors raised while appending to the metrics WAL.
#[derive(Debug, thiserror::Error)]
pub enum ProduceError {
    #[error("wal append failed: {0}")]
    Append(String),
}

#[derive(Debug, thiserror::Error)]
pub enum HaElectionReplayError {
    #[error("HA election record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("HA election record decode failed: {0}")]
    Decode(String),
}

#[derive(Debug, thiserror::Error)]
pub enum HaElectionConsumerError {
    #[error("HA election consumer poll failed: {0}")]
    Poll(String),

    #[error(transparent)]
    Replay(#[from] HaElectionReplayError),

    #[error("HA election consumer commit failed: {0}")]
    Commit(String),
}

/// Testable sink for metrics WAL records.
#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError>;
}

/// Producer-backed metrics WAL sink.
pub struct KafkaSink {
    producer: Arc<Producer>,
}

impl KafkaSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>) -> Self {
        Self { producer }
    }
}

/// Builds the WAL producer record for a serialized entry.
///
/// Separated from the send so the record's shape can be checked without a
/// broker: the topic it lands on, that partitioning is left to the producer,
/// and that the key and value are not transposed.
fn wal_producer_record(
    key: Bytes,
    value: Vec<u8>,
    trace_headers: Vec<(String, String)>,
) -> ProducerRecord {
    ProducerRecord {
        topic: WAL_TOPIC.to_string(),
        // No explicit partition: the producer's partitioner keys on `key`, so
        // every record for a series lands on one partition and stays ordered.
        partition: None,
        key: Some(key),
        value: Some(Bytes::from(value)),
        headers: trace_headers
            .into_iter()
            .map(|(key, value)| ProducerHeader {
                key,
                value: Some(Bytes::from(value.into_bytes())),
            })
            .collect(),
        ..Default::default()
    }
}

#[async_trait::async_trait]
impl WalSink for KafkaSink {
    async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        let value = record
            .encode()
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        // Inject the current ingest span's W3C trace context into the WAL record
        // headers so the downstream compactor can stitch its `metrics_compaction`
        // span onto this producer's trace. Additive: it only appends the
        // traceparent/tracestate headers, and is an empty `Vec` (no-op) when no
        // span is active or OTLP is disabled.
        let ack = self
            .producer
            .send(wal_producer_record(key, value, current_trace_headers()))
            .await;
        ack.await
            .map_err(|error| ProduceError::Append(error.to_string()))?
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        Ok(())
    }
}

#[must_use]
/// Builds a keyed producer record for a compacted topic.
///
/// Separated from the send so the record's shape can be checked without a
/// broker. The partition is deliberately absent: the producer keys on `key`,
/// which is what keeps a compacted topic's records for one entity together.
fn keyed_producer_record(topic: String, key: Bytes, value: Vec<u8>) -> ProducerRecord {
    ProducerRecord {
        topic,
        partition: None,
        key: Some(key),
        value: Some(Bytes::from(value)),
        ..Default::default()
    }
}

#[must_use]
pub fn ha_election_compaction_key(record: &HaElectionRecord) -> Bytes {
    Bytes::from(format!("{}\0{}", record.tenant, record.cluster))
}

/// Testable sink for compacted HA election records.
#[async_trait::async_trait]
pub trait HaElectionSink: Send + Sync {
    async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError>;
}

/// Producer-backed compacted HA election sink.
pub struct KafkaHaElectionSink {
    producer: Arc<Producer>,
    topic: String,
}

impl KafkaHaElectionSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }
}

#[async_trait::async_trait]
impl HaElectionSink for KafkaHaElectionSink {
    async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError> {
        let key = ha_election_compaction_key(&record);
        let value = record
            .encode()
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        let ack = self
            .producer
            .send(keyed_producer_record(self.topic.clone(), key, value))
            .await;
        ack.await
            .map_err(|error| ProduceError::Append(error.to_string()))?
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaElectionConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaElectionPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset. This is the next offset after the last replayed
    /// record.
    pub offset: Offset,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HaElectionReplayResult {
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<HaElectionPartitionOffset>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HaElectionConsumerLoopSummary {
    pub polls: usize,
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<HaElectionPartitionOffset>,
}

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn replay_ha_election_records(
    tracker: &HaTracker,
    ha_topic: &str,
    records: &[HaElectionConsumerRecord],
) -> Result<HaElectionReplayResult, HaElectionReplayError> {
    let mut committed_offsets = BTreeMap::<PartitionIndex, Offset>::new();
    let mut replayed_records = 0;
    for record in records {
        if record.topic != ha_topic {
            continue;
        }
        let value = record
            .value
            .as_deref()
            .ok_or(HaElectionReplayError::MissingValue {
                partition: record.partition,
                offset: record.offset,
            })?;
        let election_record = HaElectionRecord::decode(value)
            .map_err(|error| HaElectionReplayError::Decode(error.to_string()))?;
        tracker.persist_elected(&election_record);
        replayed_records += 1;
        committed_offsets
            .entry(record.partition)
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }

    Ok(HaElectionReplayResult {
        polled_records: records.len(),
        replayed_records,
        committed_offsets: committed_offsets
            .into_iter()
            .map(|(partition, offset)| HaElectionPartitionOffset { partition, offset })
            .collect(),
    })
}

#[async_trait::async_trait]
pub trait HaElectionConsumerPoll: Send {
    async fn poll(&mut self, timeout: Time)
    -> Result<Vec<ConsumerRecord>, HaElectionConsumerError>;
}

#[async_trait::async_trait]
pub trait HaElectionConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError>;
}

#[async_trait::async_trait]
impl HaElectionConsumerPoll for Consumer {
    async fn poll(
        &mut self,
        timeout: Time,
    ) -> Result<Vec<ConsumerRecord>, HaElectionConsumerError> {
        Consumer::poll(self, timeout)
            .await
            .map_err(|error| HaElectionConsumerError::Poll(error.to_string()))
    }
}

#[async_trait::async_trait]
impl HaElectionConsumerCommit for Consumer {
    async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|error| HaElectionConsumerError::Commit(error.to_string()))
    }
}

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn poll_ha_election_consumer_once<C>(
    consumer: &mut C,
    tracker: &HaTracker,
    ha_topic: &str,
    timeout: Time,
) -> Result<HaElectionReplayResult, HaElectionConsumerError>
where
    C: HaElectionConsumerPoll + HaElectionConsumerCommit + ?Sized,
{
    let records = consumer.poll(timeout).await?;
    let replay_records = records
        .into_iter()
        .map(|record| HaElectionConsumerRecord {
            topic: record.topic,
            partition: PartitionIndex(record.partition),
            offset: Offset(record.offset),
            value: record.value.map(|value| value.to_vec()),
        })
        .collect::<Vec<_>>();
    let result = replay_ha_election_records(tracker, ha_topic, &replay_records)?;
    if result.replayed_records > 0 {
        consumer.commit_sync().await?;
    }
    Ok(result)
}

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_ha_election_consumer_loop<C, Stop>(
    consumer: &mut C,
    tracker: &HaTracker,
    ha_topic: &str,
    timeout: Time,
    mut should_stop: Stop,
) -> Result<HaElectionConsumerLoopSummary, HaElectionConsumerError>
where
    C: HaElectionConsumerPoll + HaElectionConsumerCommit + ?Sized,
    Stop: FnMut(&HaElectionConsumerLoopSummary) -> bool,
{
    let mut summary = HaElectionConsumerLoopSummary::default();
    loop {
        let result = poll_ha_election_consumer_once(consumer, tracker, ha_topic, timeout).await?;
        summary.polls += 1;
        summary.polled_records += result.polled_records;
        summary.replayed_records += result.replayed_records;
        summary.committed_offsets.extend(result.committed_offsets);

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}

/// Shared distributor handler state.
pub struct DistributorState {
    sink: Arc<dyn WalSink>,
    ha_election_sink: Option<Arc<dyn HaElectionSink>>,
    tracker: HaTracker,
    otlp_delta_accumulator: Mutex<DeltaAccumulator>,
    ingest_enforcer: IngestEnforcer,
    overrides: Option<OverridesProvider>,
    active_series: Mutex<BTreeMap<String, BTreeSet<SeriesFingerprint>>>,
    latest_timestamps: Mutex<BTreeMap<(String, SeriesFingerprint), i64>>,
    limits: TenantLimits,
    ha_failover_timeout: Time,
    max_decompressed: ByteSize,
    metrics: Option<ServiceMetrics>,
}

impl DistributorState {
    #[must_use]
    pub fn new(sink: Arc<dyn WalSink>) -> Self {
        Self {
            sink,
            ha_election_sink: None,
            tracker: HaTracker::default(),
            otlp_delta_accumulator: Mutex::new(DeltaAccumulator::default()),
            ingest_enforcer: IngestEnforcer::new(),
            overrides: None,
            active_series: Mutex::new(BTreeMap::new()),
            latest_timestamps: Mutex::new(BTreeMap::new()),
            limits: TenantLimits::default(),
            ha_failover_timeout: DEFAULT_HA_FAILOVER_TIMEOUT,
            max_decompressed: DEFAULT_DISTRIBUTOR_MAX_DECOMPRESSED,
            metrics: None,
        }
    }

    #[must_use]
    pub fn with_limits(mut self, limits: TenantLimits) -> Self {
        self.limits = limits;
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn with_overrides(mut self, overrides: OverridesProvider) -> Self {
        self.overrides = Some(overrides);
        self
    }

    #[must_use]
    pub fn with_max_decompressed(mut self, max_decompressed: ByteSize) -> Self {
        self.max_decompressed = max_decompressed;
        self
    }

    #[must_use]
    pub fn with_ha_failover_timeout(mut self, timeout: Time) -> Self {
        self.ha_failover_timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_max_rate_buckets(mut self, cap: usize) -> Self {
        self.ingest_enforcer = IngestEnforcer::with_max_rate_buckets(cap);
        self
    }

    #[must_use]
    pub fn with_ha_election_sink(mut self, sink: Arc<dyn HaElectionSink>) -> Self {
        self.ha_election_sink = Some(sink);
        self
    }

    #[must_use]
    pub fn tracker(&self) -> &HaTracker {
        &self.tracker
    }
}

/// Builds the distributor HTTP router.
pub fn router(state: Arc<DistributorState>) -> Router {
    let grpc_service = otlp_metrics_service_server(Arc::clone(&state));
    // Cap the (compressed) push body explicitly rather than relying on axum's
    // implicit 2 MiB default. A snappy body cannot usefully exceed the
    // decompressed cap, so `max_decompressed` is a sound, configurable ceiling
    // — applied per-route so the tonic gRPC `route_service` keeps its own limit.
    let max_body = state.max_decompressed.bytes_usize();
    Router::new()
        .route(
            "/api/v1/push",
            post(push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route(
            "/api/v1/write",
            post(push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route(
            "/api/v1/clocks",
            post(clocks_push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route(
            "/otlp/v1/metrics",
            post(otlp_push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route_service(
            "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
            grpc_service,
        )
        .with_state(state)
}

/// Builds the OTLP gRPC metrics service implementation.
#[must_use]
pub fn otlp_metrics_service(state: Arc<DistributorState>) -> OtlpMetricsService {
    OtlpMetricsService { state }
}

/// Builds a tonic server for OTLP metrics export.
#[must_use]
pub fn otlp_metrics_service_server(
    state: Arc<DistributorState>,
) -> MetricsServiceServer<OtlpMetricsService> {
    MetricsServiceServer::new(otlp_metrics_service(state))
}

/// OTLP `MetricsService` implementation backed by the distributor WAL pipeline.
#[derive(Clone)]
pub struct OtlpMetricsService {
    state: Arc<DistributorState>,
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: TonicRequest<ExportMetricsServiceRequest>,
    ) -> Result<TonicResponse<ExportMetricsServiceResponse>, Status> {
        let started = std::time::Instant::now();
        let result = otlp_grpc_export_inner(&self.state, request).await;
        if let Some(metrics) = &self.state.metrics {
            let elapsed = started.elapsed().as_time();
            match &result {
                Ok(items) => metrics.record_ingest(true, ByteSize::ZERO, *items, elapsed),
                Err(_) => metrics.record_ingest(false, ByteSize::ZERO, 0, elapsed),
            }
        }
        match result {
            Ok(_) => Ok(TonicResponse::new(ExportMetricsServiceResponse {
                partial_success: None,
            })),
            Err(error) => Err(status_from_push_error(&error)),
        }
    }
}

/// Binds and serves the metrics distributor until `shutdown` resolves.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router(state))
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%error, "metrics distributor server stopped with error");
        }
    });
    Ok(bound)
}

async fn push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let started = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    // ONE ingest span per request (not per series/sample). `crabka.ingest.series`
    // starts empty and is recorded from inside `push_inner` once the body is
    // decoded; the WAL producer injects this span's trace context into the record
    // headers so the compactor's span joins the same distributed trace.
    let span = ingest_span(&headers, body_size);
    let result = push_inner(&state, &headers, &body).instrument(span).await;
    record_ingest_outcome(&state, &result, body_size, started.elapsed().as_time());
    match result {
        Ok((success, _items)) => success.into_response(),
        Err(error) => error.into_response(),
    }
}

/// Builds the per-request ingest span. This function declares
/// `crabka.ingest.series` empty, and `push_inner` records it after it decodes
/// the request body.
fn ingest_span(headers: &HeaderMap, body_size: ByteSize) -> tracing::Span {
    let tenant = tenant_for_span(headers);
    tracing::info_span!(
        "metrics_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = WAL_TOPIC,
        crabka.tenant = %tenant,
        crabka.ingest.series = tracing::field::Empty,
        crabka.ingest.bytes = body_size.bytes_u64(),
    )
}

/// Tenant label for the ingest span. It falls back to `"unknown"` when the
/// `X-Scope-OrgID` header is absent or non-ASCII. This label is for the span
/// only and never rejects the request. Validation stays in
/// `tenant_from_headers`.
fn tenant_for_span(headers: &HeaderMap) -> String {
    headers
        .get("X-Scope-OrgID")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

async fn push_inner(
    state: &DistributorState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(PushSuccess, u64), PushError> {
    let tenant = tenant_from_headers(headers)?;
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let format = negotiate(content_type)?;
    require_snappy_encoding(headers)?;

    let (mut series, counts) = match format {
        WireFormat::RemoteWriteV1 => (decode_v1(body, state.max_decompressed)?, None),
        WireFormat::RemoteWriteV2 => {
            let (series, counts) = decode_v2(body, state.max_decompressed)?;
            (series, Some(counts))
        }
    };
    let items = series.len() as u64;
    // Backfill the decoded series count onto the enclosing `metrics_ingest` span.
    tracing::Span::current().record("crabka.ingest.series", items);

    if !append_decoded_series(state, tenant, &mut series).await? {
        return Ok((
            PushSuccess::Accepted {
                counts: counts.map(|_| WrittenCounts::default()),
            },
            items,
        ));
    }

    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant, items);
    }
    Ok((PushSuccess::NoContent { counts }, items))
}

async fn clocks_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let started = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    // ONE ingest span per clock batch, as on the `remote_write` push path.
    let span = ingest_span(&headers, body_size);
    let result = clocks_push_inner(&state, &headers, &body)
        .instrument(span)
        .await;
    record_ingest_outcome(&state, &result, body_size, started.elapsed().as_time());
    match result {
        Ok((success, _items)) => success.into_response(),
        Err(error) => error.into_response(),
    }
}

async fn clocks_push_inner(
    state: &DistributorState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(PushSuccess, u64), PushError> {
    let tenant = tenant_from_headers(headers)?;
    require_snappy_encoding(headers)?;
    let readings = decode_clock_readings(body, state.max_decompressed)?;
    // Stamp the receive time once for the whole request. A per-record stamp
    // would spread the decode cost of the batch across the readings and report
    // a skew that grows with the batch size.
    let ingest_unix_nanos = ingest_stamp();

    let items = readings.len() as u64;
    // Backfill the decoded reading count onto the enclosing span.
    tracing::Span::current().record("crabka.ingest.series", items);

    if !append_clock_readings(state, tenant, &readings, ingest_unix_nanos).await? {
        return Ok((PushSuccess::Accepted { counts: None }, items));
    }

    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant, items);
    }
    Ok((PushSuccess::NoContent { counts: None }, items))
}

/// This ingester's own clock, at the moment a clock batch arrives.
///
/// A clock before the epoch, or one past the `i64` nanosecond ceiling in the
/// year 2262, saturates rather than wrapping. Either reading is already a
/// broken host clock, and the skew series is what says so.
fn ingest_stamp() -> UnixNanos {
    UnixNanos::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
            }),
    )
}

fn require_snappy_encoding(headers: &HeaderMap) -> Result<(), WireError> {
    let encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !header_list_includes(encoding, "snappy") {
        return Err(WireError::UnsupportedContentEncoding(encoding.to_string()));
    }
    Ok(())
}

fn header_list_includes(value: &str, expected: &str) -> bool {
    value
        .split(',')
        .any(|item| item.trim().eq_ignore_ascii_case(expected))
}

async fn otlp_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let started = std::time::Instant::now();
    let body_size = ByteSize::from_bytes(body.len() as u64);
    // ONE ingest span per OTLP HTTP push request; series recorded post-decode.
    let span = ingest_span(&headers, body_size);
    let result = otlp_push_inner(&state, &headers, &body)
        .instrument(span)
        .await;
    record_ingest_outcome(&state, &result, body_size, started.elapsed().as_time());
    match result {
        Ok((success, _items)) => success.into_response(),
        Err(error) => error.into_response(),
    }
}

async fn otlp_push_inner(
    state: &DistributorState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(PushSuccess, u64), PushError> {
    let tenant = tenant_from_headers(headers)?;
    require_otlp_protobuf_content_type(headers)?;
    let mut series = {
        let mut accumulator = state
            .otlp_delta_accumulator
            .lock()
            .expect("otlp delta accumulator poisoned");
        decode_otlp_stateful_bytes(body, TranslationStrategy::default(), &mut accumulator)?
    };
    let items = series.len() as u64;
    // Backfill the decoded series count onto the enclosing `metrics_ingest` span.
    tracing::Span::current().record("crabka.ingest.series", items);
    if !append_decoded_series(state, tenant, &mut series).await? {
        return Ok((PushSuccess::Accepted { counts: None }, items));
    }
    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant, items);
    }
    Ok((PushSuccess::Ok, items))
}

/// Records an ingest request outcome on the distributor metrics bundle, if one
/// is configured. `body_size` is the compressed request-body length. `items` is
/// the decoded series count on success and `0` on error.
fn record_ingest_outcome(
    state: &DistributorState,
    result: &Result<(PushSuccess, u64), PushError>,
    body_size: ByteSize,
    elapsed: Time,
) {
    let Some(metrics) = &state.metrics else {
        return;
    };
    match result {
        Ok((_, items)) => metrics.record_ingest(true, body_size, *items, elapsed),
        Err(_) => metrics.record_ingest(false, body_size, 0, elapsed),
    }
}

/// Decodes and appends an OTLP gRPC export. Returns the decoded series count on
/// success, which is the ingest `items` measure.
async fn otlp_grpc_export_inner(
    state: &DistributorState,
    request: TonicRequest<ExportMetricsServiceRequest>,
) -> Result<u64, PushError> {
    let tenant = tenant_from_metadata(request.metadata())?.to_string();
    let data = MetricsData {
        resource_metrics: request.into_inner().resource_metrics,
    };
    let mut series = {
        let mut accumulator = state
            .otlp_delta_accumulator
            .lock()
            .expect("otlp delta accumulator poisoned");
        decode_otlp_stateful(&data, TranslationStrategy::default(), &mut accumulator)?
    };
    let items = series.len() as u64;
    if append_decoded_series(state, &tenant, &mut series).await?
        && let Some(metrics) = &state.metrics
    {
        metrics.record_ingest_series(&tenant, items);
    }
    Ok(items)
}

fn require_otlp_protobuf_content_type(headers: &HeaderMap) -> Result<(), WireError> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let base = content_type.split(';').next().unwrap_or_default().trim();
    if !base.eq_ignore_ascii_case("application/x-protobuf") {
        return Err(WireError::UnsupportedContentType(base.to_string()));
    }
    Ok(())
}

async fn append_decoded_series(
    state: &DistributorState,
    tenant: &str,
    series: &mut [DecodedSeries],
) -> Result<bool, PushError> {
    if !enforce_ingest_limits(state, tenant, series).await? {
        return Ok(false);
    }
    append_wal_records(state, tenant, wal_records_from_series(tenant, series)).await?;
    Ok(true)
}

/// Applies every per-tenant ingest gate to `series`, in the order the push path
/// needs them.
///
/// Returns `false` when the HA tracker drops the request, which is an accepted
/// request that writes nothing.
async fn enforce_ingest_limits(
    state: &DistributorState,
    tenant: &str,
    series: &mut [DecodedSeries],
) -> Result<bool, PushError> {
    validate(series, &state.limits)?;
    let limits = state.limits_for_tenant(tenant);
    enforce_label_limits(&limits, series)?;
    // Decide-and-commit the in-memory HA winner atomically so a racing replica
    // cannot also win the same (tenant, cluster); only the durable Kafka persist
    // is left async, after the in-memory winner is already fixed.
    match state
        .tracker
        .elect_now_with_timeout(tenant, series, state.ha_failover_timeout)
    {
        HaElection::Accept => {}
        HaElection::Drop => return Ok(false),
        HaElection::Elect(record) | HaElection::Update(record) => {
            // The in-memory winner is already committed under the tracker lock;
            // only the durable Kafka persist remains and may proceed async.
            if let Some(sink) = &state.ha_election_sink {
                sink.persist_election(record.clone()).await?;
            }
        }
    }

    strip_replica_label(series);
    enforce_and_record_active_series(state, &limits, tenant, series)?;
    enforce_ingestion_rate(state, &limits, tenant, series)?;
    enforce_out_of_order_window(state, &limits, tenant, series)?;
    Ok(true)
}

/// Appends already-gated records to the WAL, one produce per record.
async fn append_wal_records(
    state: &DistributorState,
    tenant: &str,
    records: Vec<WalRecord>,
) -> Result<(), PushError> {
    for record in records {
        let key = partition_key(tenant, record.series_fingerprint());
        if let Err(error) = state.sink.append(key, record).await {
            // The actual WAL/produce error site — count it distinctly from
            // 4xx client/validation rejects so operators can alert on durable
            // append failures via rate(wal_append_failures_total).
            if let Some(metrics) = &state.metrics {
                metrics.wal_append_failures.inc();
            }
            return Err(error.into());
        }
    }
    Ok(())
}

/// Gates a clock batch and appends both the clock block records and the
/// projected float records.
async fn append_clock_readings(
    state: &DistributorState,
    tenant: &str,
    readings: &[DecodedClockReading],
    ingest_unix_nanos: UnixNanos,
) -> Result<bool, PushError> {
    let mut series = clock_series(readings, ingest_unix_nanos);
    if !enforce_ingest_limits(state, tenant, &mut series).await? {
        return Ok(false);
    }

    let mut records = clock_wal_records(tenant, readings, ingest_unix_nanos);
    records.extend(wal_records_from_series(tenant, &series));
    append_wal_records(state, tenant, records).await?;
    Ok(true)
}

impl DistributorState {
    fn limits_for_tenant(&self, tenant: &str) -> Limits {
        self.overrides.as_ref().map_or_else(
            || tenant_limits_to_limits(&self.limits),
            |overrides| overrides.for_tenant(tenant).clone(),
        )
    }
}

fn tenant_limits_to_limits(limits: &TenantLimits) -> Limits {
    Limits {
        ingestion_rate: limits.ingestion_rate,
        ingestion_burst_size: u64::try_from(limits.ingestion_burst_size).unwrap_or(u64::MAX),
        max_label_name_length: limits.max_label_name_len,
        max_label_value_length: limits.max_label_value_len,
        out_of_order_time_window: limits.out_of_order_time_window,
        ..Limits::default()
    }
}

fn enforce_label_limits(limits: &Limits, series: &[DecodedSeries]) -> Result<(), LimitError> {
    for series in series {
        IngestEnforcer::check_labels(limits, &series.labels)?;
    }
    Ok(())
}

/// Enforces the per-user active-series limit and records the new series under a
/// single lock acquisition. The lock covers the check AND the insert, which
/// closes the active-series TOCTOU. Two concurrent pushes can no longer both see
/// the same pre-insert count and overshoot `max_global_series_per_user`.
fn enforce_and_record_active_series(
    state: &DistributorState,
    limits: &Limits,
    tenant: &str,
    series: &[DecodedSeries],
) -> Result<(), LimitError> {
    let mut active = state
        .active_series
        .lock()
        .expect("active series tracker poisoned");

    if limits.max_global_series_per_user != 0 {
        let existing = active.get(tenant);
        let current = existing.map_or(0, BTreeSet::len);
        let would_add = series
            .iter()
            .map(|series| series.labels.fingerprint())
            .filter(|fingerprint| existing.is_none_or(|set| !set.contains(fingerprint)))
            .collect::<BTreeSet<_>>()
            .len();

        state.ingest_enforcer.check_active_series(
            limits,
            tenant,
            u64::try_from(would_add).unwrap_or(u64::MAX),
            u64::try_from(current).unwrap_or(u64::MAX),
        )?;
    }

    let tenant_active = active.entry(tenant.to_string()).or_default();
    for series in series {
        tenant_active.insert(series.labels.fingerprint());
    }
    Ok(())
}

fn enforce_ingestion_rate(
    state: &DistributorState,
    limits: &Limits,
    tenant: &str,
    series: &[DecodedSeries],
) -> Result<(), PushError> {
    let sample_count = decoded_sample_count(series);
    if sample_count == 0 {
        return Ok(());
    }

    state
        .ingest_enforcer
        .check_sample_rate(
            limits,
            tenant,
            u64::try_from(sample_count).unwrap_or(u64::MAX),
        )
        .map_err(PushError::from)
}

fn decoded_sample_count(series: &[DecodedSeries]) -> usize {
    series
        .iter()
        .map(|series| series.samples.len() + series.histograms.len() + series.exemplars.len())
        .sum()
}

fn enforce_out_of_order_window(
    state: &DistributorState,
    limits: &Limits,
    tenant: &str,
    series: &[DecodedSeries],
) -> Result<(), PushError> {
    if limits.out_of_order_time_window < Time::ZERO {
        return Ok(());
    }
    let window_ms = limits.out_of_order_time_window.millis_i64();

    let mut latest = state
        .latest_timestamps
        .lock()
        .expect("latest timestamp tracker poisoned");
    let mut updates = Vec::new();
    for series in series {
        let Some((min_timestamp, max_timestamp)) = sample_timestamp_bounds(series) else {
            continue;
        };
        let fingerprint = series.labels.fingerprint();
        let key = (tenant.to_string(), fingerprint);
        if let Some(previous_latest) = latest.get(&key).copied() {
            let oldest_allowed = previous_latest - window_ms;
            if min_timestamp < oldest_allowed {
                return Err(PushError::TooOldSample {
                    timestamp_ms: min_timestamp,
                    oldest_allowed_ms: oldest_allowed,
                });
            }
        }
        updates.push((key, max_timestamp));
    }

    for (key, max_timestamp) in updates {
        latest
            .entry(key)
            .and_modify(|previous| *previous = (*previous).max(max_timestamp))
            .or_insert(max_timestamp);
    }
    Ok(())
}

fn sample_timestamp_bounds(series: &DecodedSeries) -> Option<(i64, i64)> {
    series
        .samples
        .iter()
        .map(|sample| sample.timestamp_ms)
        .chain(
            series
                .histograms
                .iter()
                .map(|(timestamp_ms, _)| *timestamp_ms),
        )
        .chain(
            series
                .exemplars
                .iter()
                .map(|exemplar| exemplar.timestamp_ms),
        )
        .fold(None, |bounds, timestamp| match bounds {
            None => Some((timestamp, timestamp)),
            Some((min_timestamp, max_timestamp)) => {
                Some((min_timestamp.min(timestamp), max_timestamp.max(timestamp)))
            }
        })
}

// cargo-mutants: covered through HTTP push-path tenant validation tests.
#[cfg_attr(test, mutants::skip)]
fn tenant_from_headers(headers: &HeaderMap) -> Result<&str, PushError> {
    headers
        .get("X-Scope-OrgID")
        .ok_or(PushError::MissingTenant)?
        .to_str()
        .map_err(|_| PushError::MissingTenant)
        .and_then(validate_request_tenant)
}

// cargo-mutants: covered through OTLP gRPC push-path tenant validation tests.
#[cfg_attr(test, mutants::skip)]
fn tenant_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Result<&str, PushError> {
    metadata
        .get("x-scope-orgid")
        .ok_or(PushError::MissingTenant)?
        .to_str()
        .map_err(|_| PushError::MissingTenant)
        .and_then(validate_request_tenant)
}

// cargo-mutants: shared tenant validation glue is covered by HTTP and gRPC callers.
#[cfg_attr(test, mutants::skip)]
fn validate_request_tenant(tenant: &str) -> Result<&str, PushError> {
    if tenant.is_empty() {
        Err(PushError::MissingTenant)
    } else {
        validate_tenant(tenant).map_err(PushError::InvalidTenant)?;
        Ok(tenant)
    }
}

/// Validates the decoded series against the structural limits.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn validate(series: &[DecodedSeries], limits: &TenantLimits) -> Result<(), WireError> {
    if series.len() > limits.max_series_per_request {
        return Err(WireError::Invalid(format!(
            "series per request {} exceeds limit {}",
            series.len(),
            limits.max_series_per_request
        )));
    }

    for series in series {
        let sample_count = series.samples.len() + series.histograms.len() + series.exemplars.len();
        if sample_count > limits.max_samples_per_series {
            return Err(WireError::Invalid(format!(
                "samples per series {sample_count} exceeds limit {}",
                limits.max_samples_per_series
            )));
        }
        for (name, value) in series.labels.iter() {
            if !is_valid_label_name(name) {
                return Err(WireError::Invalid(format!("invalid label name `{name}`")));
            }
            let name_limit = limits.max_label_name_len.bytes_usize();
            if name.len() > name_limit {
                return Err(WireError::Invalid(format!(
                    "label name length {} exceeds limit {name_limit}",
                    name.len(),
                )));
            }
            let value_limit = limits.max_label_value_len.bytes_usize();
            if value.len() > value_limit {
                return Err(WireError::Invalid(format!(
                    "label value length {} exceeds limit {value_limit}",
                    value.len(),
                )));
            }
        }
        for exemplar in &series.exemplars {
            validate_exemplar_labels(exemplar)?;
        }
    }

    Ok(())
}

fn is_valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn validate_exemplar_labels(exemplar: &DecodedExemplar) -> Result<(), WireError> {
    let codepoints = exemplar
        .labels
        .iter()
        .try_fold(0usize, |codepoints, (name, value)| {
            if !is_valid_label_name(name) {
                return Err(WireError::Invalid(format!(
                    "invalid exemplar label name `{name}`"
                )));
            }
            Ok(codepoints + name.chars().count() + value.chars().count())
        })?;
    if codepoints > MAX_EXEMPLAR_LABEL_CODEPOINTS {
        return Err(WireError::Invalid(format!(
            "exemplar label set has {codepoints} codepoints, exceeding limit {MAX_EXEMPLAR_LABEL_CODEPOINTS}"
        )));
    }
    Ok(())
}

enum PushSuccess {
    Ok,
    Accepted { counts: Option<WrittenCounts> },
    NoContent { counts: Option<WrittenCounts> },
}

impl IntoResponse for PushSuccess {
    fn into_response(self) -> Response {
        match self {
            Self::Ok => StatusCode::OK.into_response(),
            Self::Accepted { counts: None } => StatusCode::ACCEPTED.into_response(),
            Self::Accepted {
                counts: Some(counts),
            } => written_counts_response(StatusCode::ACCEPTED, counts),
            Self::NoContent { counts: None } => StatusCode::NO_CONTENT.into_response(),
            Self::NoContent {
                counts: Some(counts),
            } => written_counts_response(StatusCode::NO_CONTENT, counts),
        }
    }
}

fn written_counts_response(status: StatusCode, counts: WrittenCounts) -> Response {
    let mut response = status.into_response();
    let headers = response.headers_mut();
    insert_written_header(
        headers,
        "X-Prometheus-Remote-Write-Samples-Written",
        counts.samples,
    );
    insert_written_header(
        headers,
        "X-Prometheus-Remote-Write-Histograms-Written",
        counts.histograms,
    );
    insert_written_header(
        headers,
        "X-Prometheus-Remote-Write-Exemplars-Written",
        counts.exemplars,
    );
    response
}

fn insert_written_header(headers: &mut HeaderMap, name: &'static str, value: u64) {
    headers.insert(
        name,
        HeaderValue::from_str(&value.to_string()).expect("u64 header value"),
    );
}

#[derive(Debug, thiserror::Error)]
enum PushError {
    #[error("missing X-Scope-OrgID tenant header")]
    MissingTenant,
    #[error("invalid tenant: {0}")]
    InvalidTenant(String),
    #[error(
        "too-old-sample: timestamp {timestamp_ms} is older than oldest allowed {oldest_allowed_ms}"
    )]
    TooOldSample {
        timestamp_ms: i64,
        oldest_allowed_ms: i64,
    },
    #[error(transparent)]
    Limit(#[from] LimitError),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Clock(#[from] ClockWireError),
    #[error(transparent)]
    Otlp(#[from] OtlpError),
    #[error(transparent)]
    Produce(#[from] ProduceError),
}

impl IntoResponse for PushError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Limit(error) => {
                StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::MissingTenant | Self::InvalidTenant(_) | Self::TooOldSample { .. } => {
                StatusCode::BAD_REQUEST
            }
            Self::Wire(error) => StatusCode::from_u16(error.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Self::Clock(error) => {
                StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::Otlp(error) => {
                StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::Produce(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}

/// The gRPC status a push failure reaches the client as.
///
/// The errors that carry an HTTP status share one mapping rather than each
/// repeating it as a match guard. Three of those guards could never match:
/// wire errors only ever report 400 or 415, and OTLP errors only 400, so the
/// arms testing them for 429 and 500 were unreachable. Going through the
/// status code keeps the intent, applies it to all three uniformly, and stays
/// correct if any of them gains a new code.
fn status_from_push_error(error: &PushError) -> Status {
    let message = error.to_string();
    match error {
        PushError::Produce(_) => Status::internal(message),
        PushError::Limit(limit) => status_from_http_status(limit.http_status(), message),
        PushError::Wire(wire) => status_from_http_status(wire.status_code(), message),
        PushError::Otlp(otlp) => status_from_http_status(otlp.status_code(), message),
        PushError::MissingTenant
        | PushError::InvalidTenant(_)
        | PushError::Clock(_)
        | PushError::TooOldSample { .. } => Status::invalid_argument(message),
    }
}

fn status_from_http_status(http_status: u16, message: String) -> Status {
    if http_status == StatusCode::TOO_MANY_REQUESTS.as_u16() {
        Status::resource_exhausted(message)
    } else if http_status == StatusCode::INTERNAL_SERVER_ERROR.as_u16() {
        Status::internal(message)
    } else {
        Status::invalid_argument(message)
    }
}

/// Fans the decoded series into one WAL record per float sample or native-
/// histogram sample.
#[must_use]
pub fn wal_records_from_series(tenant: &str, series: &[DecodedSeries]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    for series in series {
        let labels = label_pairs(series);
        let exemplars = series
            .exemplars
            .iter()
            .map(|exemplar| WalExemplar {
                labels: exemplar
                    .labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                value: exemplar.value,
                timestamp_ms: exemplar.timestamp_ms,
            })
            .collect::<Vec<_>>();

        out.extend(series.samples.iter().map(|sample| WalRecord {
            tenant: tenant.to_string(),
            labels: labels.clone(),
            payload: SamplePayload::Float {
                timestamp_ms: sample.timestamp_ms,
                value: sample.value,
                start_timestamp_ms: sample.start_timestamp_ms,
            },
            exemplars: Vec::new(),
        }));
        out.extend(
            series
                .histograms
                .iter()
                .map(|(timestamp_ms, hist)| WalRecord {
                    tenant: tenant.to_string(),
                    labels: labels.clone(),
                    payload: SamplePayload::Hist {
                        timestamp_ms: *timestamp_ms,
                        hist: hist.clone(),
                    },
                    exemplars: Vec::new(),
                }),
        );
        if let Some(metadata) = &series.metadata {
            out.push(WalRecord {
                tenant: tenant.to_string(),
                labels: labels.clone(),
                payload: SamplePayload::Metadata {
                    metric_family_name: metadata.metric_family_name.clone(),
                    metric_type: metadata.metric_type.clone(),
                    help: metadata.help.clone(),
                    unit: metadata.unit.clone(),
                },
                exemplars: Vec::new(),
            });
        }
        if !exemplars.is_empty() {
            out.push(WalRecord {
                tenant: tenant.to_string(),
                labels,
                payload: SamplePayload::Exemplars,
                exemplars,
            });
        }
    }
    out
}

/// Builds the clock block WAL records, one per reading.
///
/// The record rides the ordinary [`WalRecord`] envelope, with the node and
/// clock identity in its labels, so fingerprinting, partitioning, and tenancy
/// work on it exactly as they do on a float sample.
#[must_use]
pub fn clock_wal_records(
    tenant: &str,
    readings: &[DecodedClockReading],
    ingest_unix_nanos: UnixNanos,
) -> Vec<WalRecord> {
    readings
        .iter()
        .map(|reading| WalRecord {
            tenant: tenant.to_string(),
            labels: clock_identity_labels(reading),
            payload: SamplePayload::ClockReading(Box::new(ClockReadingPayload {
                reading: reading.clone(),
                ingest_unix_nanos,
            })),
            exemplars: Vec::new(),
        })
        .collect()
}

/// Builds every series a clock batch publishes.
///
/// The first series of each reading is the clock block's own identity, which
/// carries no float sample. The rest are the projection: ordinary float series
/// that `PromQL`, the ruler, and Grafana read with no query-path change. The
/// block stays the source of truth, and the projection is a derived view of it.
#[must_use]
pub fn clock_series(
    readings: &[DecodedClockReading],
    ingest_unix_nanos: UnixNanos,
) -> Vec<DecodedSeries> {
    let mut out = Vec::new();
    for reading in readings {
        out.push(decoded_series(clock_identity_labels(reading), None));
        out.extend(clock_projection(reading, ingest_unix_nanos));
    }
    out
}

/// The label set that identifies one clock on one host.
fn clock_identity_labels(reading: &DecodedClockReading) -> Vec<(String, String)> {
    projected_labels(reading, CLOCK_READING_METRIC, &[])
}

/// Builds the projected float series for one reading.
fn clock_projection(
    reading: &DecodedClockReading,
    ingest_unix_nanos: UnixNanos,
) -> Vec<DecodedSeries> {
    let timestamp_ms = reading.timestamp_ms();
    let payload = ClockReadingPayload {
        reading: reading.clone(),
        ingest_unix_nanos,
    };
    let mut out = Vec::new();

    let mut gauge = |name: &str, value: f64| {
        out.push(decoded_series(
            projected_labels(reading, name, &[]),
            Some(DecodedSample::new(timestamp_ms, value)),
        ));
    };

    // Always present.
    gauge(
        "krabka_clock_uncertainty_seconds",
        reading.uncertainty().secs_f64(),
    );
    gauge(
        "krabka_clock_offset_seconds",
        Time::from_nanos(reading.offset_nanos).secs_f64(),
    );
    gauge(
        "krabka_clock_ingest_skew_seconds",
        payload.ingest_skew().secs_f64(),
    );

    // Discipline state, when the host reported it.
    if let Some(last_sync) = reading.last_sync_unix_nanos {
        gauge("krabka_clock_last_sync_seconds", last_sync.epoch_secs_f64());
    }
    if let Some(frequency_ppb) = reading.frequency_ppb {
        gauge("krabka_clock_frequency_ppb", widen(frequency_ppb));
    }
    if let Some(last_step_nanos) = reading.last_step_nanos {
        gauge(
            "krabka_clock_step_seconds_total",
            Time::from_nanos(last_step_nanos).secs_f64(),
        );
    }

    // NTP.
    if let Some(ntp) = reading.ntp {
        gauge(
            "krabka_clock_root_delay_seconds",
            Time::from_nanos(ntp.root_delay_nanos).secs_f64(),
        );
        gauge(
            "krabka_clock_root_dispersion_seconds",
            Time::from_nanos(ntp.root_dispersion_nanos).secs_f64(),
        );
        gauge("krabka_clock_stratum", f64::from(ntp.stratum));
    }

    // PTP and PHC.
    if let Some(ptp) = reading.ptp {
        gauge(
            "krabka_clock_path_delay_seconds",
            Time::from_nanos(ptp.mean_path_delay_nanos).secs_f64(),
        );
        gauge("krabka_clock_steps_removed", f64::from(ptp.steps_removed));
        gauge("krabka_clock_class", f64::from(ptp.gm_clock_class));
    }

    // GNSS.
    if let Some(gnss) = reading.gnss {
        gauge(
            "krabka_gnss_satellites_used",
            f64::from(gnss.satellites_used),
        );
    }

    out.extend(clock_state_series(reading, timestamp_ms));
    out
}

/// Builds the two state-enum families a clock reading publishes.
///
/// Prometheus carries an enumerated state as one series per value with an extra
/// label, the current value at `1` and every other value at `0`. Every value
/// goes out on every reading, so a transition overwrites the old `1` with a `0`
/// in the same scrape rather than leaving it to go stale.
fn clock_state_series(reading: &DecodedClockReading, timestamp_ms: i64) -> Vec<DecodedSeries> {
    let mut out = ClockSyncState::ALL
        .iter()
        .map(|state| {
            decoded_series(
                projected_labels(
                    reading,
                    "krabka_clock_sync_state",
                    &[("state", state.as_label())],
                ),
                Some(DecodedSample::new(
                    timestamp_ms,
                    indicator(*state == reading.sync_state),
                )),
            )
        })
        .collect::<Vec<_>>();

    // A reading from a source other than GNSS carries no fix quality, so it
    // publishes no fix family at all rather than a family of zeros.
    if let Some(current) = reading.gnss.and_then(|gnss| gnss.fix) {
        out.extend(GnssFix::ALL.iter().map(|fix| {
            decoded_series(
                projected_labels(reading, "krabka_gnss_fix", &[("fix", fix.as_label())]),
                Some(DecodedSample::new(timestamp_ms, indicator(*fix == current))),
            )
        }));
    }
    out
}

/// The label set for one projected series: the clock identity, the metric
/// name, and any state label the family adds.
fn projected_labels(
    reading: &DecodedClockReading,
    name: &str,
    extra: &[(&str, &str)],
) -> Vec<(String, String)> {
    let mut labels = vec![
        ("__name__".to_string(), name.to_string()),
        ("node".to_string(), reading.node.clone()),
        ("clock".to_string(), reading.clock.clone()),
        (
            "source".to_string(),
            reading.source_kind.as_label().to_string(),
        ),
    ];
    labels.extend(
        extra
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string())),
    );
    labels
}

fn decoded_series(labels: Vec<(String, String)>, sample: Option<DecodedSample>) -> DecodedSeries {
    DecodedSeries {
        labels: labels.into_iter().collect(),
        samples: sample.into_iter().collect(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: None,
    }
}

/// The Prometheus encoding of a boolean: `1` when it holds, `0` when it does
/// not.
fn indicator(holds: bool) -> f64 {
    f64::from(u8::from(holds))
}

/// Widens a signed count for a projected sample value.
///
/// `i64::to_f64` never fails, so the fallback is unreachable. It keeps the
/// conversion free of a lossy `as` cast.
fn widen(value: i64) -> f64 {
    num_traits::ToPrimitive::to_f64(&value).unwrap_or(f64::MAX)
}

fn label_pairs(series: &DecodedSeries) -> Vec<(String, String)> {
    series
        .labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {

    /// `wal_producer_record` shapes one WAL append. The partition is left
    /// unset deliberately: the producer's partitioner keys on the record key,
    /// which is what keeps a series on one partition and in order. Setting a
    /// partition here would silently defeat that, so its absence is asserted
    /// rather than left unmentioned.
    #[test]
    fn a_wal_record_carries_its_key_value_and_headers_without_a_partition() {
        let record = super::wal_producer_record(
            Bytes::from_static(b"series-key"),
            b"payload".to_vec(),
            vec![
                ("traceparent".to_string(), "00-abc-def-01".to_string()),
                ("tracestate".to_string(), "vendor=1".to_string()),
            ],
        );

        check!(record.topic == WAL_TOPIC);
        check!(record.partition == None, "the partitioner must choose, not this");
        check!(record.key.as_deref() == Some(&b"series-key"[..]));
        check!(record.value.as_deref() == Some(&b"payload"[..]), "not the key again");

        // Headers keep their order and their pairing; the two values differ so
        // a swap between them is visible.
        check!(record.headers.len() == 2);
        check!(record.headers[0].key == "traceparent");
        check!(record.headers[0].value.as_deref() == Some(&b"00-abc-def-01"[..]));
        check!(record.headers[1].key == "tracestate");
        check!(record.headers[1].value.as_deref() == Some(&b"vendor=1"[..]));

        // No trace context means no headers, rather than empty ones.
        let bare = super::wal_producer_record(
            Bytes::from_static(b"k"),
            b"v".to_vec(),
            Vec::new(),
        );
        check!(bare.headers.is_empty());
    }

    /// `decoded_sample_count` totals three collections across every series.
    /// Each carries a different number, so a term dropped from the sum is a
    /// specific shortfall rather than merely a smaller total -- with equal
    /// counts, dropping any one of the three looks the same.
    #[test]
    fn a_decoded_batch_counts_samples_histograms_and_exemplars() {
        use crate::{histogram::NativeHistogram, wire::DecodedExemplar, wire::DecodedSample};

        let series = |samples: usize, histograms: usize, exemplars: usize| DecodedSeries {
            labels: Labels::default(),
            samples: (0..samples)
                .map(|i| DecodedSample::new(i64::try_from(i).expect("small"), 1.0))
                .collect(),
            histograms: (0..histograms)
                .map(|i| {
                    (
                        i64::try_from(i).expect("small"),
                        NativeHistogram {
                            schema: 0,
                            is_float: false,
                            reset_hint: crate::ResetHint::Unknown,
                            zero_threshold: 0.0,
                            zero_count: 0.0,
                            count: 0.0,
                            sum: 0.0,
                            positive_spans: Vec::new(),
                            positive_counts: Vec::new(),
                            negative_spans: Vec::new(),
                            negative_counts: Vec::new(),
                            custom_values: None,
                            start_timestamp_ms: None,
                        },
                    )
                })
                .collect(),
            exemplars: (0..exemplars)
                .map(|i| DecodedExemplar {
                    labels: Labels::default(),
                    timestamp_ms: i64::try_from(i).expect("small"),
                    value: 1.0,
                })
                .collect(),
            metadata: None,
        };

        // Three different counts, so dropping any one term is distinguishable
        // from dropping either other.
        check!(super::decoded_sample_count(&[series(3, 5, 7)]) == 15);
        check!(super::decoded_sample_count(&[series(3, 0, 0)]) == 3, "samples alone");
        check!(super::decoded_sample_count(&[series(0, 5, 0)]) == 5, "histograms alone");
        check!(super::decoded_sample_count(&[series(0, 0, 7)]) == 7, "exemplars alone");

        // Several series add up rather than the largest winning.
        check!(super::decoded_sample_count(&[series(3, 0, 0), series(4, 0, 0)]) == 7);

        // Nothing at all is zero, not one.
        check!(super::decoded_sample_count(&[]) == 0);
        check!(super::decoded_sample_count(&[series(0, 0, 0)]) == 0, "an empty series counts none");
    }

    /// `tenant_limits_to_limits` copies five fields across and leaves the rest
    /// at their defaults. Two of the five are byte sizes and two are counts,
    /// so every value here is distinct: a field reading its neighbour still
    /// produces a well-formed limit, and only distinct values show it.
    #[test]
    fn tenant_limits_map_field_by_field_onto_the_shared_limits() {
        use crabka_units::{bytes, per_sec, secs};

        let tenant = super::TenantLimits {
            max_label_name_len: bytes(11),
            max_label_value_len: bytes(22),
            max_samples_per_series: 33,
            max_series_per_request: 44,
            ingestion_rate: per_sec(55),
            ingestion_burst_size: 66,
            out_of_order_time_window: secs(77),
        };

        let limits = super::tenant_limits_to_limits(&tenant);

        check!(limits.max_label_name_length == bytes(11));
        check!(limits.max_label_value_length == bytes(22), "not the name length");
        check!(limits.ingestion_burst_size == 66, "the burst, not a sample count");
        check!(limits.out_of_order_time_window == secs(77));
        check!(limits.ingestion_rate == per_sec(55));

        // The fields with no counterpart keep the shared default rather than
        // picking up a value from the tenant's own limits.
        let defaults = super::Limits::default();
        check!(
            limits.max_global_series_per_user == defaults.max_global_series_per_user,
            "a field with no source stays at its default"
        );
    }
    use std::sync::Mutex;

    fn decoded_series(labels: &[(&str, &str)], samples: usize) -> crate::wire::DecodedSeries {
        let mut set = crabka_blockstore::Labels::new();
        for (name, value) in labels {
            set.insert(*name, *value);
        }
        crate::wire::DecodedSeries {
            labels: set,
            samples: (0..samples)
                .map(|i| crate::wire::DecodedSample {
                    timestamp_ms: i64::try_from(i).unwrap_or(0),
                    value: 1.0,
                    start_timestamp_ms: None,
                })
                .collect(),
            histograms: vec![],
            exemplars: vec![],
            metadata: None,
        }
    }

    /// Every structural limit rejects what exceeds it, so a request sitting
    /// exactly on each one is still accepted. Each is checked at its edge and
    /// one past it, and the errors are matched on their text because the four
    /// differ only in which number they name.
    #[test]
    fn structural_limits_admit_exactly_their_boundary() {
        let limits = TenantLimits {
            max_series_per_request: 2,
            max_samples_per_series: 3,
            max_label_name_len: crabka_units::bytes(4),
            max_label_value_len: crabka_units::bytes(5),
            ..TenantLimits::default()
        };

        let two = [decoded_series(&[("ok", "v")], 1), decoded_series(&[("ok", "v")], 1)];
        assert!(super::validate(&two, &limits).is_ok(), "two series fit a limit of two");

        let three = [
            decoded_series(&[("ok", "v")], 1),
            decoded_series(&[("ok", "v")], 1),
            decoded_series(&[("ok", "v")], 1),
        ];
        let err = super::validate(&three, &limits).unwrap_err().to_string();
        assert!(err.contains("series per request 3 exceeds limit 2"), "got: {err}");

        let at_edge = [decoded_series(&[("ok", "v")], 3)];
        assert!(super::validate(&at_edge, &limits).is_ok(), "three samples fit a limit of three");
        let over = [decoded_series(&[("ok", "v")], 4)];
        let err = super::validate(&over, &limits).unwrap_err().to_string();
        assert!(err.contains("samples per series 4 exceeds limit 3"), "got: {err}");

        let at_edge = [decoded_series(&[("abcd", "v")], 1)];
        assert!(super::validate(&at_edge, &limits).is_ok(), "a four-byte name fits");
        let over = [decoded_series(&[("abcde", "v")], 1)];
        let err = super::validate(&over, &limits).unwrap_err().to_string();
        assert!(err.contains("label name length 5 exceeds limit 4"), "got: {err}");

        let at_edge = [decoded_series(&[("ok", "vwxyz")], 1)];
        assert!(super::validate(&at_edge, &limits).is_ok(), "a five-byte value fits");
        let over = [decoded_series(&[("ok", "vwxyz!")], 1)];
        let err = super::validate(&over, &limits).unwrap_err().to_string();
        assert!(err.contains("label value length 6 exceeds limit 5"), "got: {err}");

        let bad = [decoded_series(&[("has space", "v")], 1)];
        let err = super::validate(&bad, &limits).unwrap_err().to_string();
        assert!(err.contains("invalid label name"), "got: {err}");
    }

    /// The per-series sample budget counts samples, histograms and exemplars
    /// together, so a series can exceed it without any one kind doing so.
    #[test]
    fn the_sample_budget_counts_every_kind_together() {
        let limits = TenantLimits {
            max_samples_per_series: 3,
            ..TenantLimits::default()
        };

        let mut series = decoded_series(&[("ok", "v")], 2);
        series.exemplars = vec![crate::wire::DecodedExemplar {
            labels: crabka_blockstore::Labels::new(),
            value: 1.0,
            timestamp_ms: 1,
        }];
        assert!(
            super::validate(std::slice::from_ref(&series), &limits).is_ok(),
            "two samples and one exemplar is exactly three"
        );

        series.exemplars.push(crate::wire::DecodedExemplar {
            labels: crabka_blockstore::Labels::new(),
            value: 1.0,
            timestamp_ms: 2,
        });
        let err = super::validate(&[series], &limits).unwrap_err().to_string();
        assert!(err.contains("samples per series 4 exceeds limit 3"), "got: {err}");
    }

    /// The HA election compaction key identifies one tenant-and-cluster pair.
    /// Two pairs sharing a key would let one cluster's election overwrite
    /// another's, so the separator has to do its job.
    #[test]
    fn ha_election_keys_identify_one_tenant_and_cluster() {
        let key = |tenant: &str, cluster: &str| {
            super::ha_election_compaction_key(&crate::distributor::ha::HaElectionRecord {
                tenant: tenant.into(),
                cluster: cluster.into(),
                // Neither is part of the identity: a later election for the
                // same pair replaces the earlier one.
                replica: "replica-1".into(),
                lease_timestamp_ms: 1,
            })
        };

        check!(key("t", "c") == Bytes::from("t\0c"));
        check!(key("t", "c") == key("t", "c"), "the same pair keys alike");
        check!(key("t", "c") != key("t", "d"), "a different cluster differs");
        check!(key("t", "c") != key("u", "c"), "so does a different tenant");
        check!(
            key("t", "c") != key("tc", ""),
            "the separator stops a shifted split from colliding"
        );
    }

    /// The record both compacted sinks build, checked without a broker.
    #[test]
    fn a_compacted_record_carries_its_topic_key_and_value() {
        let record = super::keyed_producer_record(
            "ha-elections".to_string(),
            Bytes::from_static(b"the-key"),
            b"the-value".to_vec(),
        );

        check!(record.topic == "ha-elections");
        check!(record.partition == None, "partitioning is left to the producer");
        check!(record.key.as_deref() == Some(&b"the-key"[..]));
        check!(record.value.as_deref() == Some(&b"the-value"[..]));
    }

    /// The WAL record's shape, checked without a broker. The key and value
    /// are distinguishable byte strings so a transposition is visible, and
    /// the absent partition matters: supplying one would override the
    /// producer's key-based partitioner and break per-series ordering.
    #[test]
    fn a_wal_record_carries_its_key_value_and_trace_headers() {
        let record = super::wal_producer_record(
            Bytes::from_static(b"the-key"),
            b"the-value".to_vec(),
            vec![
                ("traceparent".to_string(), "00-abc-def-01".to_string()),
                ("tracestate".to_string(), "vendor=1".to_string()),
            ],
        );

        check!(record.topic == super::WAL_TOPIC);
        check!(record.partition == None, "partitioning is left to the producer");
        check!(record.key.as_deref() == Some(&b"the-key"[..]));
        check!(record.value.as_deref() == Some(&b"the-value"[..]));
        check!(
            record
                .headers
                .iter()
                .map(|header| (
                    header.key.as_str(),
                    header.value.as_deref().map(|v| String::from_utf8_lossy(v).into_owned())
                ))
                .collect::<Vec<_>>()
                == vec![
                    ("traceparent", Some("00-abc-def-01".to_string())),
                    ("tracestate", Some("vendor=1".to_string())),
                ],
            "headers keep their names, values and order"
        );

        // No active span means no headers, not an empty-valued one.
        let bare = super::wal_producer_record(
            Bytes::from_static(b"k"),
            b"v".to_vec(),
            vec![],
        );
        check!(bare.headers.is_empty());
    }

    /// The HTTP-to-gRPC mapping the error kinds share. Only two codes get a
    /// status of their own; everything else is the caller's fault by default,
    /// which is the safer reading for a code nobody has mapped yet.
    #[test]
    fn http_statuses_map_to_the_grpc_code_the_client_should_act_on() {
        let map = |code| super::status_from_http_status(code, "why".to_string()).code();

        check!(map(429) == tonic::Code::ResourceExhausted, "too many requests");
        check!(map(500) == tonic::Code::Internal, "our fault");

        for code in [400, 404, 415, 422, 428, 430, 499, 501, 503] {
            check!(
                map(code) == tonic::Code::InvalidArgument,
                "{code} has no status of its own"
            );
        }

        check!(
            super::status_from_http_status(500, "why".to_string()).message() == "why",
            "the reason is carried through"
        );
    }

    /// Every push failure has to reach the client as the status it should act
    /// on: back off, retry later, or stop sending this request. The table
    /// covers each error kind, including the ones that reach the catch-all,
    /// since that is where a guard that stopped matching would land.
    #[test]
    fn push_errors_reach_the_client_as_the_status_to_act_on() {
        use crate::limits::LimitError;
        use crate::wire::WireError;

        let cases: Vec<(super::PushError, tonic::Code, &str)> = vec![
            (
                LimitError::IngestionRateExceeded { rate: 1.0, observed: 2.0 }.into(),
                tonic::Code::ResourceExhausted,
                "a rate limit is a back-off",
            ),
            (
                LimitError::MaxSeriesPerUser { limit: 1, observed: 2 }.into(),
                tonic::Code::InvalidArgument,
                "a series limit is the request's fault",
            ),
            (
                LimitError::QueryRangeTooLong { limit_secs: 1, observed_secs: 2 }.into(),
                tonic::Code::InvalidArgument,
                "an unprocessable range is too",
            ),
            (
                super::ProduceError::Append("wal down".into()).into(),
                tonic::Code::Internal,
                "a failed append is ours, not the client's",
            ),
            (
                WireError::UnsupportedContentType("text/plain".into()).into(),
                tonic::Code::InvalidArgument,
                "an undecodable body is the request's fault",
            ),
            (
                WireError::Invalid("bad".into()).into(),
                tonic::Code::InvalidArgument,
                "so is an invalid one",
            ),
            (
                super::PushError::MissingTenant,
                tonic::Code::InvalidArgument,
                "a missing tenant header",
            ),
            (
                super::PushError::InvalidTenant("a/b".into()),
                tonic::Code::InvalidArgument,
                "an unusable tenant header",
            ),
            (
                super::PushError::TooOldSample { timestamp_ms: 1, oldest_allowed_ms: 2 },
                tonic::Code::InvalidArgument,
                "a sample the store will not take",
            ),
        ];

        for (error, expected, why) in cases {
            let status = super::status_from_push_error(&error);
            check!(status.code() == expected, "{why}: {error}");
            check!(!status.message().is_empty(), "{why}: the reason is carried through");
        }
    }

    /// The exemplar codepoint budget is summed across every label name and
    /// value, and compared with a strict `>` so a set landing exactly on the
    /// limit is allowed.
    ///
    /// Two things go wrong quietly here. Read as `>=`, the budget is one
    /// codepoint tighter than documented and an exemplar sitting on the limit
    /// is refused. And the running total is a sum: read as a product, a single
    /// label still totals plausibly while several no longer do, so the check
    /// only misfires once an exemplar carries more than one label.
    #[test]
    fn exemplar_codepoints_are_summed_and_capped_at_the_limit() {
        let exemplar = |pairs: &[(&str, &str)]| {
            let mut labels = crabka_blockstore::Labels::new();
            for (name, value) in pairs {
                labels.insert(*name, *value);
            }
            DecodedExemplar {
                labels,
                timestamp_ms: 0,
                value: 1.0,
            }
        };

        // Eight labels of eight codepoints each side: 128, exactly the budget.
        let owned: Vec<(String, String)> = (0..8)
            .map(|i| (format!("name{i:04}"), format!("valu{i:04}")))
            .collect();
        let at_limit: Vec<(&str, &str)> = owned
            .iter()
            .map(|(n, v)| (n.as_str(), v.as_str()))
            .collect();
        let total: usize = at_limit
            .iter()
            .map(|(n, v)| n.chars().count() + v.chars().count())
            .sum();
        check!(total == MAX_EXEMPLAR_LABEL_CODEPOINTS, "fixture is {total}, not the budget");

        check!(
            validate_exemplar_labels(&exemplar(&at_limit)).is_ok(),
            "a set landing exactly on the budget is allowed"
        );

        // One codepoint more, spread across the same number of labels, is not.
        let mut over = at_limit.clone();
        over.push(("x", ""));
        check!(
            validate_exemplar_labels(&exemplar(&over)).is_err(),
            "one codepoint past the budget is refused"
        );

        // One label whose value alone exceeds the budget.
        let long = "v".repeat(MAX_EXEMPLAR_LABEL_CODEPOINTS + 1);
        check!(
            validate_exemplar_labels(&exemplar(&[("trace_id", long.as_str())])).is_err(),
            "single label over the budget"
        );
    }

    /// A push failure's gRPC code tells the client what to do next: back off
    /// (`resource_exhausted`), retry later (`internal`), or stop and fix the
    /// request (`invalid_argument`). The mapping is a chain of match guards on
    /// the underlying HTTP status, and a guard forced either way sends the
    /// wrong instruction -- a rate limit reported as `invalid_argument` makes a
    /// client give up on a request that would succeed after a pause, and a bad
    /// request reported as `resource_exhausted` makes it retry-storm one that
    /// never will.
    #[test]
    fn push_errors_map_to_the_grpc_code_the_client_should_act_on() {
        use crate::limits::LimitError;

        let over_rate = PushError::Limit(LimitError::IngestionRateExceeded {
            rate: 100.0,
            observed: 150.0,
        });
        check!(
            status_from_push_error(&over_rate).code() == tonic::Code::ResourceExhausted,
            "429 limit is resource_exhausted"
        );

        // A 400-class limit is the client's mistake, not a reason to back off.
        let too_many_series = PushError::Limit(LimitError::MaxSeriesPerUser {
            limit: 10,
            observed: 11,
        });
        check!(
            status_from_push_error(&too_many_series).code() == tonic::Code::InvalidArgument,
            "400 limit is invalid_argument"
        );

        let too_long = PushError::Limit(LimitError::LabelNameTooLong {
            limit: 8,
            observed: 9,
        });
        check!(
            status_from_push_error(&too_long).code() == tonic::Code::InvalidArgument,
            "label length is invalid_argument"
        );
    }


    use assert2::{assert, check};
    use axum::{body::Body, http::Request};
    use crabka_blockstore::Labels;
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::{
            ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient,
            metrics_service_server::MetricsService,
        },
        common::v1::{AnyValue, KeyValue, any_value},
        metrics::v1::{
            AggregationTemporality, Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
            ScopeMetrics, Sum, metric, number_data_point,
        },
        resource::v1::Resource,
    };
    use prost::Message;
    use tower::ServiceExt as _;

    use super::*;
    use crate::wire::DecodedSample;

    /// Pins the span-label logic of `tenant_for_span`. A present, non-empty
    /// header goes through verbatim. A missing OR empty `X-Scope-OrgID` falls
    /// back to `"unknown"`. This kills the whole-function replacement mutants,
    /// `"xyzzy"` and `String::new()`, and the `delete !` mutant on
    /// `!value.is_empty()`. The empty-string case maps to `"unknown"` only
    /// while the negation stands.
    #[test]
    fn tenant_for_span_labels_present_and_falls_back_on_missing_or_empty() {
        let mut present = HeaderMap::new();
        present.insert("X-Scope-OrgID", "acme".parse().unwrap());
        assert!(tenant_for_span(&present) == "acme");

        let missing = HeaderMap::new();
        assert!(tenant_for_span(&missing) == "unknown");

        let mut empty = HeaderMap::new();
        empty.insert("X-Scope-OrgID", "".parse().unwrap());
        assert!(tenant_for_span(&empty) == "unknown");
    }

    #[derive(Default)]
    struct RecordingSink {
        appends: Mutex<Vec<(Bytes, WalRecord)>>,
    }

    #[async_trait::async_trait]
    impl WalSink for RecordingSink {
        async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
            self.appends
                .lock()
                .expect("recording sink poisoned")
                .push((key, record));
            Ok(())
        }
    }

    impl RecordingSink {
        fn records(&self) -> Vec<WalRecord> {
            self.appends
                .lock()
                .expect("recording sink poisoned")
                .iter()
                .map(|(_, record)| record.clone())
                .collect()
        }

        fn append_keys(&self) -> Vec<Bytes> {
            self.appends
                .lock()
                .expect("recording sink poisoned")
                .iter()
                .map(|(key, _)| key.clone())
                .collect()
        }
    }

    #[derive(Default)]
    struct RecordingHaElectionSink {
        elections: Mutex<Vec<HaElectionRecord>>,
    }

    #[async_trait::async_trait]
    impl HaElectionSink for RecordingHaElectionSink {
        async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError> {
            self.elections
                .lock()
                .expect("ha election sink poisoned")
                .push(record);
            Ok(())
        }
    }

    impl RecordingHaElectionSink {
        fn elections(&self) -> Vec<HaElectionRecord> {
            self.elections
                .lock()
                .expect("ha election sink poisoned")
                .clone()
        }
    }

    struct RecordingHaElectionConsumer {
        batches: Vec<Vec<ConsumerRecord>>,
        commit_calls: usize,
    }

    #[async_trait::async_trait]
    impl HaElectionConsumerPoll for RecordingHaElectionConsumer {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<ConsumerRecord>, HaElectionConsumerError> {
            Ok(self.batches.remove(0))
        }
    }

    #[async_trait::async_trait]
    impl HaElectionConsumerCommit for RecordingHaElectionConsumer {
        async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError> {
            self.commit_calls += 1;
            Ok(())
        }
    }

    fn consumer_record(
        topic: &str,
        partition: i32,
        offset: i64,
        value: Option<Vec<u8>>,
    ) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.to_string(),
            partition,
            offset,
            leader_epoch: -1,
            timestamp: 0,
            key: None,
            value: value.map(Bytes::from),
            headers: Vec::new(),
        }
    }

    struct FailingHaElectionSink;

    #[async_trait::async_trait]
    impl HaElectionSink for FailingHaElectionSink {
        async fn persist_election(&self, _record: HaElectionRecord) -> Result<(), ProduceError> {
            Err(ProduceError::Append("ha election unavailable".to_string()))
        }
    }

    fn test_state() -> (Arc<DistributorState>, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        (Arc::new(DistributorState::new(sink.clone())), sink)
    }

    #[test]
    fn distributor_state_stores_configured_runtime_policy() {
        let sink = Arc::new(RecordingSink::default());
        let state = DistributorState::new(sink)
            .with_ha_failover_timeout(Time::from_millis(-1_000))
            .with_max_rate_buckets(7)
            .with_max_decompressed(kibibytes(64));

        check!(state.ha_failover_timeout == Time::from_millis(-1_000));
        check!(state.ingest_enforcer.max_rate_buckets() == 7);
        check!(state.max_decompressed == kibibytes(64));
    }

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    fn v1_body(labels: Vec<crate::wire::pb::v1::Label>) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels,
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp: 100,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_samples(sample_count: usize) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: (0..sample_count)
                    .map(|index| crate::wire::pb::v1::Sample {
                        value: 1.0,
                        timestamp: i64::try_from(index).expect("test sample index fits in i64"),
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_sample_timestamp(timestamp: i64) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_metadata() -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "http_requests_total")],
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp: 100,
                }],
                ..Default::default()
            }],
            metadata: vec![crate::wire::pb::v1::MetricMetadata {
                r#type: crate::wire::pb::v1::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "http_requests_total".into(),
                help: "Total HTTP requests.".into(),
                unit: "requests".into(),
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_exemplar_label_value(value: &str) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "http_requests_total")],
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp: 100,
                }],
                exemplars: vec![crate::wire::pb::v1::Exemplar {
                    labels: vec![label("trace_id", value)],
                    value: 1.0,
                    timestamp: 100,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_exemplar_timestamp(timestamp: i64) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "up")],
                exemplars: vec![crate::wire::pb::v1::Exemplar {
                    labels: vec![label("trace_id", "abc123")],
                    value: 1.0,
                    timestamp,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v2_body() -> Vec<u8> {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                ..Default::default()
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn v2_body_with_metadata() -> Vec<u8> {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "http_requests_total".into(),
                "Total HTTP requests.".into(),
                "requests".into(),
            ],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                metadata: Some(crate::wire::pb::v2::Metadata {
                    r#type: crate::wire::pb::v2::metadata::MetricType::Counter as i32,
                    help_ref: 3,
                    unit_ref: 4,
                }),
                ..Default::default()
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn v2_body_with_ha_replica(replica: &str) -> Vec<u8> {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "up".into(),
                "cluster".into(),
                "c1".into(),
                "__replica__".into(),
                replica.into(),
            ],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4, 5, 6],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                ..Default::default()
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn otlp_body() -> Vec<u8> {
        otlp_gauge_body()
    }

    fn otlp_sum_body(value: f64, timestamp: u64, monotonic: bool, temporality: i32) -> Vec<u8> {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "system.cpu.utilization".into(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "host.name".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue("api-1".into())),
                                    }),
                                    key_strindex: 0,
                                }],
                                time_unix_nano: timestamp,
                                value: Some(number_data_point::Value::AsDouble(value)),
                                ..Default::default()
                            }],
                            aggregation_temporality: temporality,
                            is_monotonic: monotonic,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn otlp_gauge_body() -> Vec<u8> {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "system.cpu.utilization".into(),
                        description: "CPU utilization ratio.".into(),
                        unit: "1".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "host.name".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue("api-1".into())),
                                    }),
                                    key_strindex: 0,
                                }],
                                time_unix_nano: 1_000_000,
                                value: Some(number_data_point::Value::AsDouble(0.5)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn otlp_resource_body() -> Vec<u8> {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("checkout".into())),
                        }),
                        key_strindex: 0,
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "system.cpu.utilization".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_000_000,
                                value: Some(number_data_point::Value::AsDouble(0.5)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn label(name: &str, value: &str) -> crate::wire::pb::v1::Label {
        crate::wire::pb::v1::Label {
            name: name.into(),
            value: value.into(),
        }
    }

    #[tokio::test]
    async fn push_v1_returns_204_and_appends() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::NO_CONTENT);
        assert_eq!(
            records,
            vec![WalRecord {
                tenant: "tenant-a".to_string(),
                labels: vec![("__name__".to_string(), "up".to_string())],
                payload: SamplePayload::Float {
                    timestamp_ms: 100,
                    value: 1.0,
                    start_timestamp_ms: None,
                },
                exemplars: Vec::new(),
            }]
        );
    }

    #[tokio::test]
    async fn push_v1_accepts_listed_snappy_content_encoding() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "identity, snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_v1_accepts_prometheus_remote_write_receiver_path() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/write")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_keys_wal_append_by_tenant_and_series_fingerprint() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("job", "api"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        let records = sink.records();
        assert!(records.len() == 1);
        assert!(
            sink.append_keys()
                == vec![crate::wal::partition_key(
                    "tenant-a",
                    records[0].series_fingerprint()
                )]
        );
    }

    #[tokio::test]
    async fn push_v2_sets_written_headers() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v2_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(response.status() == StatusCode::NO_CONTENT);
        check!(
            response
                .headers()
                .get("X-Prometheus-Remote-Write-Samples-Written")
                .and_then(|value| value.to_str().ok())
                == Some("1")
        );
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_v2_preserves_sample_start_timestamp_in_wal() {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 5,
                }],
                ..Default::default()
            }],
        };
        let (state, sink) = test_state();

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(snappy(&req.encode_to_vec())))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        let records = sink.records();
        assert!(records.len() == 1);
        let SamplePayload::Float {
            timestamp_ms,
            value,
            start_timestamp_ms,
        } = records[0].payload
        else {
            panic!("expected float payload");
        };
        check!(timestamp_ms == 7);
        check!((value - 3.0).abs() < f64::EPSILON);
        check!(start_timestamp_ms == Some(5));
    }

    #[tokio::test]
    async fn push_v1_appends_metric_metadata_record() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_metadata()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(records.len() == 2);
        let metadata = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Metadata { .. }))
            .expect("metadata wal record");
        assert!(
            metadata.payload
                == SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: "counter".to_string(),
                    help: "Total HTTP requests.".to_string(),
                    unit: "requests".to_string(),
                }
        );
    }

    #[tokio::test]
    async fn push_v2_appends_metric_metadata_record() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v2_body_with_metadata()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(records.len() == 2);
        let metadata = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Metadata { .. }))
            .expect("metadata wal record");
        assert!(
            metadata.payload
                == SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: "counter".to_string(),
                    help: "Total HTTP requests.".to_string(),
                    unit: "requests".to_string(),
                }
        );
    }

    #[tokio::test]
    async fn oversized_exemplar_labels_are_rejected() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_exemplar_label_value(
                        &"x".repeat(129),
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn oversized_label_names_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                max_label_name_len: bytes(7),
                ..TenantLimits::default()
            }),
        );
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn runtime_overrides_apply_label_limits_per_tenant() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_overrides(
                crate::OverridesProvider::from_yaml(
                    r#"
overrides:
  tenant-tight:
    max_label_value_length: "2B"
"#,
                )
                .unwrap(),
            ),
        );
        let app = router(state);

        let tight_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-tight")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("job", "api"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();
        let loose_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-loose")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("job", "api"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(tight_response.status() == StatusCode::BAD_REQUEST);
        check!(loose_response.status() == StatusCode::NO_CONTENT);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn oversized_sample_sets_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                max_samples_per_series: 1,
                ..TenantLimits::default()
            }),
        );
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_samples(2)))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[test]
    fn validation_counts_exemplars_toward_samples_per_series_limit() {
        let mut labels = Labels::new();
        labels.insert("__name__", "http_requests_total");
        let mut exemplar_labels = Labels::new();
        exemplar_labels.insert("trace_id", "abc");
        let series = [DecodedSeries {
            labels,
            samples: Vec::new(),
            histograms: Vec::new(),
            exemplars: vec![
                DecodedExemplar {
                    labels: exemplar_labels.clone(),
                    timestamp_ms: 1000,
                    value: 1.0,
                },
                DecodedExemplar {
                    labels: exemplar_labels,
                    timestamp_ms: 2000,
                    value: 2.0,
                },
            ],
            metadata: None,
        }];

        let err = validate(
            &series,
            &TenantLimits {
                max_samples_per_series: 1,
                ..TenantLimits::default()
            },
        )
        .unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("samples per series 2 exceeds limit 1"));
    }

    #[test]
    fn validation_rejects_invalid_label_names() {
        for label_name in ["", "9bad", "bad-label"] {
            let mut labels = Labels::new();
            labels.insert("__name__", "up");
            labels.insert(label_name, "value");
            let series = [DecodedSeries {
                labels,
                samples: vec![DecodedSample::new(1000, 1.0)],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: None,
            }];

            let err = validate(&series, &TenantLimits::default()).unwrap_err();

            assert!(matches!(err, WireError::Invalid(_)));
            assert!(format!("{err}").contains("invalid label name"));
        }
    }

    #[test]
    fn validation_rejects_invalid_exemplar_label_names() {
        for label_name in ["", "9bad", "bad-label"] {
            let mut labels = Labels::new();
            labels.insert("__name__", "up");
            let mut exemplar_labels = Labels::new();
            exemplar_labels.insert(label_name, "value");
            let series = [DecodedSeries {
                labels,
                samples: vec![DecodedSample::new(1000, 1.0)],
                histograms: Vec::new(),
                exemplars: vec![DecodedExemplar {
                    labels: exemplar_labels,
                    timestamp_ms: 1000,
                    value: 1.0,
                }],
                metadata: None,
            }];

            let err = validate(&series, &TenantLimits::default()).unwrap_err();

            assert!(matches!(err, WireError::Invalid(_)));
            assert!(format!("{err}").contains("invalid exemplar label name"));
        }
    }

    #[tokio::test]
    async fn ingestion_rate_limit_returns_429_without_append() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                ingestion_rate: per_sec(1),
                ingestion_burst_size: 1,
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let first_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(first_response.status() == StatusCode::NO_CONTENT);
        check!(second_response.status() == StatusCode::TOO_MANY_REQUESTS);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn concurrent_pushes_cannot_overshoot_active_series_limit() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_overrides(
                crate::OverridesProvider::from_yaml(
                    r"
defaults:
  max_global_series_per_user: 1
",
                )
                .unwrap(),
            ),
        );
        let app = router(state);

        // Two distinct series pushed concurrently; the check-and-insert is a
        // single locked critical section, so exactly one is admitted and the
        // other is rejected rather than both passing the pre-insert count.
        let request = |name: &str| {
            app.clone().oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", name)])))
                    .unwrap(),
            )
        };
        let (first, second) = tokio::join!(request("series_a"), request("series_b"));
        let statuses = [first.unwrap().status(), second.unwrap().status()];

        let admitted = statuses
            .iter()
            .filter(|status| **status == StatusCode::NO_CONTENT)
            .count();
        let rejected = statuses
            .iter()
            .filter(|status| **status == StatusCode::BAD_REQUEST)
            .count();
        check!(admitted == 1);
        check!(rejected == 1);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn ingestion_rate_limit_counts_exemplar_only_writes() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                ingestion_rate: per_sec(1),
                ingestion_burst_size: 1,
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let exemplar_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_exemplar_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let sample_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(1_001)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(exemplar_response.status() == StatusCode::NO_CONTENT);
        check!(sample_response.status() == StatusCode::TOO_MANY_REQUESTS);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn too_old_samples_beyond_out_of_order_window_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                out_of_order_time_window: millis(100),
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let newest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let within_window_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(950)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let too_old_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(899)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(newest_response.status() == StatusCode::NO_CONTENT);
        check!(within_window_response.status() == StatusCode::NO_CONTENT);
        check!(too_old_response.status() == StatusCode::BAD_REQUEST);
        check!(sink.records().len() == 2);
    }

    #[tokio::test]
    async fn runtime_overrides_apply_out_of_order_window_per_tenant() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_overrides(
                crate::OverridesProvider::from_yaml(
                    r#"
defaults:
  out_of_order_time_window: "0ms"
overrides:
  tenant-loose:
    out_of_order_time_window: "100ms"
"#,
                )
                .unwrap(),
            ),
        );
        let app = router(state);

        let newest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-loose")
                    .body(Body::from(v1_body_with_sample_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let overridden_window_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-loose")
                    .body(Body::from(v1_body_with_sample_timestamp(950)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(newest_response.status() == StatusCode::NO_CONTENT);
        check!(overridden_window_response.status() == StatusCode::NO_CONTENT);
        check!(sink.records().len() == 2);
    }

    #[tokio::test]
    async fn too_old_exemplar_only_series_beyond_out_of_order_window_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                out_of_order_time_window: millis(100),
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let newest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let too_old_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_exemplar_timestamp(899)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(newest_response.status() == StatusCode::NO_CONTENT);
        check!(too_old_response.status() == StatusCode::BAD_REQUEST);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_rejects_invalid_tenant_with_400() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "bad tenant")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn push_requires_snappy_content_encoding() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn push_rejects_non_snappy_content_encoding() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "gzip")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn unsupported_content_type_is_415() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/json")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(vec![1, 2, 3]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn otlp_metrics_returns_200_and_appends() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::OK);
        assert!(records.len() == 2);
        let sample = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .expect("float wal record");
        check!(sample.tenant == "tenant-a");
        check!(
            sample.labels
                == vec![
                    ("__name__".to_string(), "system_cpu_utilization".to_string()),
                    ("host_name".to_string(), "api-1".to_string())
                ]
        );
        assert!(matches!(
            sample.payload,
            SamplePayload::Float {
                timestamp_ms: 1,
                value: 0.5,
                ..
            }
        ));
        let metadata = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Metadata { .. }))
            .expect("metadata wal record");
        assert!(
            metadata.payload
                == SamplePayload::Metadata {
                    metric_family_name: "system_cpu_utilization".to_string(),
                    metric_type: "gauge".to_string(),
                    help: "CPU utilization ratio.".to_string(),
                    unit: "1".to_string(),
                }
        );
    }

    #[tokio::test]
    async fn otlp_grpc_metrics_export_appends() {
        let (state, sink) = test_state();
        let data = MetricsData::decode(otlp_body().as_slice()).expect("otlp metrics data");
        let service = otlp_metrics_service(state);
        let mut request = tonic::Request::new(ExportMetricsServiceRequest {
            resource_metrics: data.resource_metrics,
        });
        request
            .metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let response = service.export(request).await.expect("otlp grpc export");

        let records = sink.records();
        assert!(response.into_inner().partial_success.is_none());
        assert!(records.len() == 2);
        let sample = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .expect("float wal record");
        assert!(sample.tenant == "tenant-a");
        assert!(
            sample.labels
                == vec![
                    ("__name__".to_string(), "system_cpu_utilization".to_string()),
                    ("host_name".to_string(), "api-1".to_string())
                ]
        );
    }

    #[tokio::test]
    async fn otlp_grpc_metrics_export_round_trips_over_bound_server() {
        let (state, sink) = test_state();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async {
            let _ = shutdown_rx.await;
        })
        .await
        .expect("serve distributor");
        let data = MetricsData::decode(otlp_body().as_slice()).expect("otlp metrics data");
        let mut client = MetricsServiceClient::connect(format!("http://{bound}"))
            .await
            .expect("connect otlp grpc client");
        let mut request = tonic::Request::new(ExportMetricsServiceRequest {
            resource_metrics: data.resource_metrics,
        });
        request
            .metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let response = client.export(request).await.expect("otlp grpc export");
        let _ = shutdown_tx.send(());

        let records = sink.records();
        check!(response.into_inner().partial_success.is_none());
        check!(records.len() == 2);
        check!(records.iter().any(|record| record.tenant == "tenant-a"));
    }

    #[tokio::test]
    async fn otlp_metrics_rejects_non_protobuf_content_type() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/json")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn otlp_delta_sum_accumulates_across_pushes() {
        let (state, sink) = test_state();
        let app = router(state);

        let first_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_sum_body(
                        7.0,
                        2_000_000,
                        true,
                        AggregationTemporality::Delta as i32,
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_sum_body(
                        5.0,
                        3_000_000,
                        true,
                        AggregationTemporality::Delta as i32,
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(first_response.status() == StatusCode::OK);
        assert!(second_response.status() == StatusCode::OK);
        let float_records = records
            .iter()
            .filter(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .collect::<Vec<_>>();
        assert!(float_records.len() == 2);
        assert!(matches!(
            float_records[0].payload,
            SamplePayload::Float {
                timestamp_ms: 2,
                value: 7.0,
                ..
            }
        ));
        assert!(matches!(
            float_records[1].payload,
            SamplePayload::Float {
                timestamp_ms: 3,
                value: 12.0,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn otlp_resource_attributes_append_target_info() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_resource_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::OK);
        let float_records = records
            .iter()
            .filter(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .collect::<Vec<_>>();
        assert!(float_records.len() == 2);
        let target = records
            .iter()
            .find(|record| {
                matches!(record.payload, SamplePayload::Float { .. })
                    && record
                        .labels
                        .iter()
                        .any(|(name, value)| name == "__name__" && value == "target_info")
            })
            .expect("target_info wal record");
        assert!(
            target.labels
                == vec![
                    ("__name__".to_string(), "target_info".to_string()),
                    ("service_name".to_string(), "checkout".to_string()),
                ]
        );
        assert!(matches!(
            target.payload,
            SamplePayload::Float {
                timestamp_ms: 1,
                value: 1.0,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn non_elected_replica_returns_202_without_append() {
        let (state, sink) = test_state();
        state.tracker().set_elected("tenant-a", "c1", "r1");
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("cluster", "c1"),
                        label("__replica__", "r2"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::ACCEPTED);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn non_elected_v2_replica_returns_zero_written_headers() {
        let (state, sink) = test_state();
        state.tracker().set_elected("tenant-a", "c1", "r1");
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v2_body_with_ha_replica("r2")))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(response.status() == StatusCode::ACCEPTED);
        for header in [
            "X-Prometheus-Remote-Write-Samples-Written",
            "X-Prometheus-Remote-Write-Histograms-Written",
            "X-Prometheus-Remote-Write-Exemplars-Written",
        ] {
            check!(
                response
                    .headers()
                    .get(header)
                    .and_then(|value| value.to_str().ok())
                    == Some("0"),
                "header {header}",
            );
        }
        check!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn first_seen_ha_replica_persists_election_before_append() {
        let sink = Arc::new(RecordingSink::default());
        let election_sink = Arc::new(RecordingHaElectionSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_ha_election_sink(election_sink.clone()),
        );

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("cluster", "c1"),
                        label("__replica__", "r1"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(sink.records().len() == 1);
        let elections = election_sink.elections();
        assert!(elections.len() == 1);
        check!(elections[0].tenant == "tenant-a");
        check!(elections[0].cluster == "c1");
        check!(elections[0].replica == "r1");
        check!(elections[0].lease_timestamp_ms > 0);
    }

    #[tokio::test]
    async fn first_seen_ha_replica_persistence_failure_prevents_append() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone())
                .with_ha_election_sink(Arc::new(FailingHaElectionSink)),
        );

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("cluster", "c1"),
                        label("__replica__", "r1"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
        assert!(sink.records().is_empty());
    }

    #[test]
    fn ha_election_records_round_trip_with_compacted_key() {
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };

        let encoded = record.encode().unwrap();

        assert!(HaElectionRecord::decode(&encoded).unwrap() == record);
        assert!(ha_election_compaction_key(&record) == Bytes::from_static(b"tenant-a\0c1"));
    }

    #[test]
    fn replay_ha_election_records_applies_tracker_and_reports_commit_offsets() {
        let tracker = HaTracker::default();
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };
        let records = vec![
            HaElectionConsumerRecord {
                topic: "ignored".to_string(),
                partition: PartitionIndex(0),
                offset: Offset(10),
                value: Some(record.encode().unwrap()),
            },
            HaElectionConsumerRecord {
                topic: HA_TRACKER_TOPIC.to_string(),
                partition: PartitionIndex(2),
                offset: Offset(20),
                value: Some(record.encode().unwrap()),
            },
        ];

        let result = replay_ha_election_records(&tracker, HA_TRACKER_TOPIC, &records).unwrap();

        assert!(
            result
                == HaElectionReplayResult {
                    polled_records: 2,
                    replayed_records: 1,
                    committed_offsets: vec![HaElectionPartitionOffset {
                        partition: PartitionIndex(2),
                        offset: Offset(21),
                    }],
                }
        );
        assert!(tracker.elected_replica("tenant-a", "c1") == Some("r1".to_string()));
    }

    /// A poll that replays nothing must not commit. Committing on an empty
    /// batch would advance the group past records it never applied, and the
    /// elections they carry would be lost on the next restart.
    #[tokio::test]
    async fn poll_ha_election_consumer_once_does_not_commit_without_progress() {
        let tracker = HaTracker::default();

        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![vec![]],
            commit_calls: 0,
        };
        let result =
            poll_ha_election_consumer_once(&mut consumer, &tracker, HA_TRACKER_TOPIC, millis(1))
                .await
                .unwrap();
        assert!(
            result
                == HaElectionReplayResult {
                    polled_records: 0,
                    replayed_records: 0,
                    committed_offsets: vec![],
                }
        );
        assert!(consumer.commit_calls == 0, "an empty poll commits nothing");

        // Polled but not replayed: a record from another topic is seen and
        // applied to nothing. Committing here would advance this group's
        // offsets on the strength of someone else's records.
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };
        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![vec![consumer_record(
                "some-other-topic",
                1,
                7,
                Some(record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };
        let result =
            poll_ha_election_consumer_once(&mut consumer, &tracker, HA_TRACKER_TOPIC, millis(1))
                .await
                .unwrap();
        assert!(result.polled_records == 1, "the record was seen");
        assert!(result.replayed_records == 0, "but it was not ours to apply");
        assert!(consumer.commit_calls == 0, "a poll that applies nothing commits nothing");
    }

    /// The election consumer loop polls until told to stop, accumulating
    /// what each poll saw. A caller watching the summary to decide when it
    /// has caught up depends on every field advancing on every poll.
    #[tokio::test]
    async fn the_ha_election_loop_accumulates_every_polls_result() {
        let tracker = HaTracker::default();
        let record = |cluster: &str| {
            HaElectionRecord {
                tenant: "tenant-a".to_string(),
                cluster: cluster.to_string(),
                replica: "r1".to_string(),
                lease_timestamp_ms: 42_000,
            }
            .encode()
            .unwrap()
        };

        // Two records, then one, then none, so a loop that reused a single
        // poll's result would not add up.
        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![
                vec![
                    consumer_record(HA_TRACKER_TOPIC, 0, 1, Some(record("c1"))),
                    consumer_record(HA_TRACKER_TOPIC, 1, 5, Some(record("c2"))),
                ],
                vec![consumer_record(HA_TRACKER_TOPIC, 2, 9, Some(record("c3")))],
                vec![],
            ],
            commit_calls: 0,
        };

        let summary = run_ha_election_consumer_loop(
            &mut consumer,
            &tracker,
            HA_TRACKER_TOPIC,
            millis(1),
            |summary| summary.polls >= 3,
        )
        .await
        .unwrap();

        assert!(summary.polls == 3, "one count per poll, including the empty one");
        assert!(summary.polled_records == 3, "2 + 1 + 0");
        assert!(summary.replayed_records == 3);
        assert!(
            summary.committed_offsets
                == vec![
                    HaElectionPartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(2),
                    },
                    HaElectionPartitionOffset {
                        partition: PartitionIndex(1),
                        offset: Offset(6),
                    },
                    HaElectionPartitionOffset {
                        partition: PartitionIndex(2),
                        offset: Offset(10),
                    },
                ],
            "offsets from every poll, in order"
        );
        assert!(consumer.commit_calls == 2, "the empty poll committed nothing");
    }

    /// The stop predicate is consulted after each poll, so a loop told to
    /// stop immediately still does exactly one poll's worth of work.
    #[tokio::test]
    async fn the_ha_election_loop_stops_after_the_poll_that_satisfies_it() {
        let tracker = HaTracker::default();
        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![vec![], vec![]],
            commit_calls: 0,
        };

        let summary = run_ha_election_consumer_loop(
            &mut consumer,
            &tracker,
            HA_TRACKER_TOPIC,
            millis(1),
            |_| true,
        )
        .await
        .unwrap();

        assert!(summary.polls == 1, "stopping at once still polls once");
        assert!(consumer.batches.len() == 1, "and consumes exactly one batch");
    }

    #[tokio::test]
    async fn poll_ha_election_consumer_once_replays_records_and_commits_on_progress() {
        let tracker = HaTracker::default();
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };
        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![vec![consumer_record(
                HA_TRACKER_TOPIC,
                1,
                7,
                Some(record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };

        let result =
            poll_ha_election_consumer_once(&mut consumer, &tracker, HA_TRACKER_TOPIC, millis(1))
                .await
                .unwrap();

        assert!(
            result
                == HaElectionReplayResult {
                    polled_records: 1,
                    replayed_records: 1,
                    committed_offsets: vec![HaElectionPartitionOffset {
                        partition: PartitionIndex(1),
                        offset: Offset(8),
                    }],
                }
        );
        check!(consumer.commit_calls == 1);
        check!(tracker.elected_replica("tenant-a", "c1") == Some("r1".to_string()));
    }

    #[test]
    fn wal_records_from_series_fans_out_float_samples() {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let series = [DecodedSeries {
            labels,
            samples: vec![DecodedSample::new(10, 1.0), DecodedSample::new(20, 2.0)],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: None,
        }];

        let records = wal_records_from_series("tenant-a", &series);

        assert!(records.len() == 2);
        check!(records[0].tenant == "tenant-a");
        check!(records[0].labels == records[1].labels);
        assert!(matches!(
            records[0].payload,
            SamplePayload::Float {
                timestamp_ms: 10,
                value: 1.0,
                ..
            }
        ));
        assert!(matches!(
            records[1].payload,
            SamplePayload::Float {
                timestamp_ms: 20,
                value: 2.0,
                ..
            }
        ));
    }
}
