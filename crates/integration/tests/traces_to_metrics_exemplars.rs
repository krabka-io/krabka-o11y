//! traces -> metrics: a span-metrics exemplar keeps its trace id across the
//! `remote_write` boundary.
//!
//! `krabka-traces`' metrics-generator writes one exemplar per observed span,
//! labelled with the span's `trace_id` and `span_id`, and ships it to a
//! Prometheus `remote_write` endpoint. `krabka-metrics`' distributor decodes
//! that endpoint's requests, and `krabka-promql` answers
//! `/api/v1/query_exemplars` over what the distributor wrote. Each half has its
//! own tests. Nothing joined them: the traces end-to-end suite remote-writes
//! into an upstream Prometheus container instead of into Krabka's own ingest,
//! and the metrics exemplar-query suite seeds no exemplars, so it asserts only
//! that the status is `success`.
//!
//! This suite closes that. It runs the real encoder and the real decoder
//! against each other, over a real HTTP hop, and then asks the query API for
//! the trace id back:
//!
//! ```text
//! SpanRecord -> SpanMetricsRegistry -> Series (krabka-traces)
//!            -> to_timeseries + snappy protobuf remote_write     [encoder]
//!            -> HTTP POST /api/v1/push over a loopback listener
//!            -> distributor decode_v1 -> WalRecord               [decoder]
//!            -> WalHead -> /api/v1/query_exemplars               (krabka-promql)
//! ```
//!
//! The only test double is the distributor's `WalSink`, which stands in for the
//! Kafka producer so no broker has to boot. Everything on either side of the
//! wire is production code.

use std::sync::{Arc, Mutex};

use assert2::{assert, check};
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use krabka_metrics::{
    WalRecord,
    distributor::{DistributorState, ProduceError, WalSink, router},
};
use krabka_observability::server_security::{
    InternalClient, ServerSecurity, authenticate_requests,
};
use krabka_promql::{EngineOpts, PrometheusApiState, WalHead, prometheus_router};
use krabka_traces::metricsgen::{
    MetricsGenConfig, PrometheusRemoteWriteSink, RemoteWriteSink as _, SeriesPayload, SpanKind,
    SpanMetricsRegistry, SpanRecord, StatusCode as SpanStatus,
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use tower::ServiceExt as _;

/// The tenant both halves are driven under. The distributor takes it from the
/// `X-Scope-OrgID` header the traces sink sets from `SeriesPayload::tenant`,
/// and the query API takes it from the same header on the way out.
const TENANT: &str = "tenant-a";

/// The id under test. It is the whole point of the suite: it is written by
/// `krabka-traces` and has to be readable, unchanged, out of `krabka-promql`.
const TRACE_ID: [u8; 16] = [
    0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36,
];
const SPAN_ID: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];

/// A wall-clock-free base for every timestamp in the suite. The distributor
/// rejects a sample that goes backwards within a series, and the query API
/// takes an explicit `[start, end]`, so a fixed instant is both sufficient and
/// reproducible.
const BASE_MS: i64 = 1_700_000_000_000;

/// Stands in for the distributor's Kafka producer.
///
/// `WalSink` is the seam the distributor already publishes for exactly this —
/// it is what keeps the suite from having to boot a broker to read back what
/// the decoder produced. The records land here in append order.
#[derive(Default)]
struct CapturingWalSink {
    records: Mutex<Vec<WalRecord>>,
}

impl CapturingWalSink {
    fn records(&self) -> Vec<WalRecord> {
        self.records.lock().expect("wal sink poisoned").clone()
    }
}

#[async_trait::async_trait]
impl WalSink for CapturingWalSink {
    async fn append(&self, _key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.records.lock().expect("wal sink poisoned").push(record);
        Ok(())
    }
}

/// One span, the shape the metrics-generator projects out of the traces WAL.
fn span() -> SpanRecord {
    SpanRecord {
        tenant: TENANT.to_string(),
        trace_id: TRACE_ID,
        span_id: SPAN_ID,
        parent_span_id: [0; 8],
        name: "GET /orders".to_string(),
        kind: SpanKind::Server,
        start_ns: BASE_MS * 1_000_000,
        // 5 ms, which lands in a bucket well below `+Inf` so the assertion that
        // the exemplar is attached to a *specific* bucket has something to say.
        duration_ns: 5_000_000,
        status: SpanStatus::Ok,
        status_message: String::new(),
        service_name: "orders".to_string(),
        attributes: Vec::new(),
        resource_attributes: Vec::new(),
        size: ByteSize::from_bytes(512),
    }
}

/// Binds `krabka-metrics`' distributor on a loopback port and returns its
/// `remote_write` URL alongside the sink holding whatever it decodes.
///
/// A real listener, not a `oneshot` against the router: the traces side of this
/// link is an HTTP client, and running it against anything but a socket would
/// skip the half of the encoder that is `reqwest` and its headers.
async fn serve_distributor() -> (String, Arc<CapturingWalSink>) {
    let sink = Arc::new(CapturingWalSink::default());
    let state = Arc::new(DistributorState::new(Arc::clone(&sink) as Arc<dyn WalSink>));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind distributor listener");
    let addr = listener.local_addr().expect("distributor local addr");
    tokio::spawn(async move {
        // The push handlers read the principal that the authentication layer
        // attaches, so the router is served through that layer, unconfigured.
        let app = authenticate_requests(router(state), &ServerSecurity::default());
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://{addr}/api/v1/push"), sink)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn span_metrics_exemplar_survives_remote_write_with_its_trace_id() {
    let (url, wal_sink) = serve_distributor().await;

    // --- traces: derive span metrics, exemplars and all -------------------
    let config = MetricsGenConfig {
        max_exemplars_per_series: 4,
        ..MetricsGenConfig::default()
    };
    let mut registry = SpanMetricsRegistry::new(&config);
    registry.record_span(&span());
    let series = registry.drain(BASE_MS);

    // The generator has to have produced an exemplar carrying the ids before
    // the wire hop can be said to preserve them.
    let latency = series
        .iter()
        .find(|series| series.name == "traces_spanmetrics_latency")
        .expect("latency histogram series");
    assert!(latency.exemplars.len() == 1);
    check!(
        latency.exemplars[0].labels
            == vec![
                ("span_id".to_string(), hex::encode(SPAN_ID)),
                ("trace_id".to_string(), hex::encode(TRACE_ID)),
            ]
    );

    // --- the wire hop: krabka-traces' encoder, krabka-metrics' decoder -----
    PrometheusRemoteWriteSink::new(&url, &InternalClient::default())
        .expect("the remote-write client builds")
        .write(&SeriesPayload {
            tenant: TENANT.to_string(),
            series,
        })
        .await
        .expect("remote_write into krabka-metrics");

    let records = wal_sink.records();
    assert!(!records.is_empty());

    // Exemplars ride the classic histogram's `_bucket` series, placed on the
    // lowest bucket whose `le` covers the observed value.
    let with_exemplars: Vec<&WalRecord> = records
        .iter()
        .filter(|record| !record.exemplars.is_empty())
        .collect();
    assert!(with_exemplars.len() == 1);
    let decoded = with_exemplars[0];

    check!(decoded.tenant == TENANT);
    check!(
        decoded
            .labels
            .iter()
            .any(|(name, value)| name == "__name__"
                && value == "traces_spanmetrics_latency_bucket")
    );
    // The decoder rebuilt the exemplar the encoder wrote, label set and all.
    check!(
        decoded.exemplars[0].labels
            == vec![
                ("span_id".to_string(), hex::encode(SPAN_ID)),
                ("trace_id".to_string(), hex::encode(TRACE_ID)),
            ]
    );
    check!((decoded.exemplars[0].value - 0.005).abs() < 1e-9);
    check!(decoded.exemplars[0].timestamp_ms == BASE_MS);

    // --- metrics: query the exemplar back through the Prometheus API ------
    let head = WalHead::new();
    head.apply_wal_records(records.iter());
    let api = Arc::new(PrometheusApiState::new(
        Arc::new(head),
        EngineOpts::default(),
    ));

    let response = authenticate_requests(prometheus_router(api), &ServerSecurity::default())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/api/v1/query_exemplars?query={}&start={}&end={}",
                    "traces_spanmetrics_latency_bucket",
                    (BASE_MS / 1_000) - 60,
                    (BASE_MS / 1_000) + 60,
                ))
                .header("X-Scope-OrgID", TENANT)
                .body(axum::body::Body::empty())
                .expect("query_exemplars request"),
        )
        .await
        .expect("query_exemplars response");

    assert!(response.status() == StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("query_exemplars body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("query_exemplars json");

    check!(json["status"] == "success");
    let groups = json["data"].as_array().expect("exemplar groups");
    assert!(!groups.is_empty());

    // The trace id came back out of the metrics store, on an exemplar attached
    // to the span-metrics series the traces side named. That is the link.
    let expected_trace_id = hex::encode(TRACE_ID);
    let trace_ids: Vec<&str> = groups
        .iter()
        .flat_map(|group| {
            group["exemplars"]
                .as_array()
                .expect("exemplars array")
                .iter()
        })
        .filter_map(|exemplar| exemplar["labels"]["trace_id"].as_str())
        .collect();
    check!(trace_ids == vec![expected_trace_id.as_str()]);

    let carrying = groups
        .iter()
        .find(|group| {
            group["exemplars"]
                .as_array()
                .is_some_and(|exemplars| !exemplars.is_empty())
        })
        .expect("a group carrying exemplars");
    check!(carrying["seriesLabels"]["__name__"] == "traces_spanmetrics_latency_bucket");
    check!(carrying["seriesLabels"]["service"] == "orders");
    check!(carrying["seriesLabels"]["span_name"] == "GET /orders");
    check!(carrying["exemplars"][0]["labels"]["span_id"] == hex::encode(SPAN_ID));
}
