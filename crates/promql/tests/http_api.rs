#![recursion_limit = "512"]

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_blockstore::Labels;
use krabka_metrics::{
    BucketSpan, Limits, NativeHistogram, OverridesProvider, ResetHint, SamplePayload, WalRecord,
    wire::pb,
};
use krabka_observability::{
    RoleReadiness,
    server_security::{ServerSecurity, authenticate_requests},
};
use krabka_promql::{
    EngineOpts, InMemoryMetricStore, MetricStore, Offset, PartitionIndex, PrometheusApiState,
    QueryFrontendOptions, RulerAlertStateRecord, RulerGroupStateRecord, WalHead,
};
use krabka_units::prelude::*;
use prost::Message;
use serde_json::Value;
use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
use tower::ServiceExt;

// Every request reaches the handlers through the authentication layer, as it
// does on a served listener. With no credentials file, the layer marks each
// request unauthenticated and lets it through.
fn prometheus_router<S: MetricStore + 'static>(state: Arc<PrometheusApiState<S>>) -> axum::Router {
    authenticate_requests(
        krabka_promql::prometheus_router(state),
        &ServerSecurity::default(),
    )
}

const TENANT_HEADER: &str = "X-Scope-OrgID";

/// A native histogram's observation count and sum.
#[derive(Clone, Copy)]
struct HistogramTotals {
    count: f64,
    sum: f64,
}

/// A schema-0 float native histogram with `totals`, no buckets, no zero
/// bucket, and no counter-reset hint.
fn float_histogram(totals: HistogramTotals) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: totals.count,
        sum: totals.sum,
        positive_spans: Vec::new(),
        positive_counts: Vec::new(),
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    }
}

fn api_state<S: MetricStore + 'static>(store: S) -> Arc<PrometheusApiState<S>> {
    Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ))
}

/// `up{job="api"}` at 60s (value 1) and 120s (value 2).
fn up_api_at_60_and_120() -> TenantFloats {
    TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 60_000, 1.0)
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 120_000, 2.0)
}

/// Checks the matrix a 60s-step range query returns over [`up_api_at_60_and_120`].
fn assert_up_api_matrix(body: &Value) {
    assert_result_type(body, "matrix");
    assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
    assert2::assert!(
        body["data"]["result"][0]["values"].clone() == serde_json::json!([[60, "1"], [120, "2"]])
    );
}

/// `up{job="api",instance="a"}` and `up{job="web",instance="b"}`, both 1 at 10s.
fn up_api_a_and_web_b() -> TenantFloats {
    TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
            10_000,
            1.0,
        )
}

/// Checks a `/series` body that holds only the `job="api"` series of [`up_api_a_and_web_b`].
fn assert_only_up_api_a_series(body: &Value) {
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"].as_array().expect("data array").len() == 1);
    assert2::assert!(body["data"][0]["__name__"] == "up");
    assert2::assert!(body["data"][0]["job"] == "api");
    assert2::assert!(body["data"][0]["instance"] == "a");
}

/// `tenant-a` float samples for a test store, added one sample at a time.
struct TenantFloats {
    store: InMemoryMetricStore,
}

impl TenantFloats {
    fn new() -> Self {
        Self {
            store: InMemoryMetricStore::new(),
        }
    }

    /// Adds the float `sample_value` of the series `series` at `ts_ms`.
    fn sample(mut self, series: Labels, ts_ms: i64, sample_value: f64) -> Self {
        self.store
            .push_float("tenant-a", series, ts_ms, sample_value);
        self
    }

    fn store(self) -> InMemoryMetricStore {
        self.store
    }

    /// Routes the store with default engine options.
    fn app(self) -> axum::Router {
        prometheus_router(api_state(self.store))
    }

    /// Routes the store under the given per-tenant query limits.
    fn limited_app(self, limits: Limits) -> axum::Router {
        let state = Arc::new(
            PrometheusApiState::new(Arc::new(self.store), EngineOpts::default())
                .with_query_limits(OverridesProvider::new(limits)),
        );
        prometheus_router(state)
    }
}

async fn send(app: &axum::Router, request: Request<Body>) -> axum::response::Response {
    app.clone().oneshot(request).await.expect("router response")
}

/// Sends a `tenant-a` GET request.
async fn get(app: &axum::Router, uri: impl AsRef<str>) -> axum::response::Response {
    let request = Request::builder()
        .uri(uri.as_ref())
        .header(TENANT_HEADER, "tenant-a")
        .body(Body::empty())
        .expect("GET request");
    send(app, request).await
}

/// The body formats the tests POST.
#[derive(Clone, Copy)]
enum BodyFormat {
    Form,
    Yaml,
}

impl BodyFormat {
    fn content_type(self) -> &'static str {
        match self {
            Self::Form => "application/x-www-form-urlencoded",
            Self::Yaml => "application/yaml",
        }
    }
}

/// A `tenant-a` POST request builder for a `body_format` body.
fn tenant_post(uri: &str, body_format: BodyFormat) -> axum::http::request::Builder {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(TENANT_HEADER, "tenant-a")
        .header("Content-Type", body_format.content_type())
}

async fn post_form(
    app: &axum::Router,
    uri: &str,
    form: impl Into<Body>,
) -> axum::response::Response {
    let request = tenant_post(uri, BodyFormat::Form)
        .body(form.into())
        .expect("form POST request");
    send(app, request).await
}

async fn post_yaml(
    app: &axum::Router,
    uri: &str,
    yaml: impl Into<Body>,
) -> axum::response::Response {
    let request = tenant_post(uri, BodyFormat::Yaml)
        .body(yaml.into())
        .expect("YAML POST request");
    send(app, request).await
}

/// Stores a rule group in the `team-a` namespace and checks it was accepted.
async fn configure_team_a_rules(app: &axum::Router, yaml: &'static str) {
    let response = post_yaml(app, "/prometheus/config/v1/rules/team-a", yaml).await;
    assert2::assert!(response.status() == StatusCode::ACCEPTED);
}

/// Sends `request` to `/api/v1/read` as a snappy-compressed `tenant-a` protobuf body.
async fn remote_read(
    app: &axum::Router,
    request: &pb::v1::ReadRequest,
) -> axum::response::Response {
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/read")
        .header(TENANT_HEADER, "tenant-a")
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(Body::from(compressed))
        .expect("remote read request");
    send(app, request).await
}

fn eq_matcher(name: &str, value: &str) -> pb::v1::LabelMatcher {
    pb::v1::LabelMatcher {
        r#type: pb::v1::label_matcher::Type::Eq as i32,
        name: name.into(),
        value: value.into(),
    }
}

/// A closed window of epoch milliseconds.
#[derive(Clone, Copy)]
struct MillisRange {
    start_ms: i64,
    end_ms: i64,
}

/// A remote-read query over `window`, without hints.
fn read_query(window: MillisRange, matchers: Vec<pb::v1::LabelMatcher>) -> pb::v1::Query {
    pb::v1::Query {
        start_timestamp_ms: window.start_ms,
        end_timestamp_ms: window.end_ms,
        matchers,
        hints: None,
    }
}

/// A samples-typed remote read of `up{job="api"}` over `[10s, end_ms]`.
fn up_api_samples_request(end_ms: i64) -> pb::v1::ReadRequest {
    pb::v1::ReadRequest {
        queries: vec![read_query(
            MillisRange {
                start_ms: 10_000,
                end_ms,
            },
            vec![eq_matcher("__name__", "up"), eq_matcher("job", "api")],
        )],
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    }
}

async fn assert_execution_error_containing(response: axum::response::Response, fragment: &str) {
    let body = json_with_status(response, StatusCode::UNPROCESSABLE_ENTITY).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("execution"));
    assert2::assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains(fragment))
    );
}

async fn decode_read_response(response: axum::response::Response) -> pb::v1::ReadResponse {
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let decoded = SnappyDecoder::new()
        .decompress_vec(&bytes)
        .expect("snappy response");
    pb::v1::ReadResponse::decode(decoded.as_slice()).expect("remote read response")
}

async fn json_with_status(response: axum::response::Response, status: StatusCode) -> Value {
    assert2::assert!(response.status() == status);
    response_json(response).await
}

/// Checks a `parse_query` body that describes the `up` vector selector.
fn assert_up_vector_selector(body: &Value, matchers: &Value) {
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["type"].as_str() == Some("vectorSelector"));
    assert2::assert!(body["data"]["name"].as_str() == Some("up"));
    assert2::assert!(body["data"]["matchers"] == *matchers);
}

/// Serves an unauthenticated `/api/v1/status/walreplay` from a WAL-head-backed state.
async fn walreplay_status(head: WalHead, wal_tail: krabka_observability::ReadinessGate) -> Value {
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(head.clone()), EngineOpts::default())
            .with_wal_head_status(head, wal_tail),
    );
    let app = prometheus_router(state);
    let request = Request::builder()
        .uri("/api/v1/status/walreplay")
        .body(Body::empty())
        .expect("walreplay request");
    json_with_status(send(&app, request).await, StatusCode::OK).await
}

/// A Prometheus API error envelope's `errorType` and `error` message.
#[derive(Clone, Copy)]
struct ApiError<'a> {
    error_type: &'a str,
    message: &'a str,
}

fn assert_error_envelope(body: &Value, expected: ApiError<'_>) {
    assert2::assert!(body["status"] == "error");
    assert2::assert!(body["errorType"] == expected.error_type);
    assert2::assert!(body["error"] == expected.message);
}

fn assert_result_type(body: &Value, result_type: &str) {
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"]["resultType"] == result_type);
}

const INSTANCE_DOWN_PAGE_YAML: &str = "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    labels:
      severity: page
    annotations:
      summary: instance down
";

/// Checks the alert [`INSTANCE_DOWN_PAGE_YAML`] raises for `up{job="api",instance="a"} 1` at 0s.
fn assert_firing_instance_down_alert(alert: &Value) {
    assert2::assert!(alert["labels"]["alertname"].as_str() == Some("InstanceDown"));
    assert2::assert!(alert["labels"]["job"].as_str() == Some("api"));
    assert2::assert!(alert["labels"]["instance"].as_str() == Some("a"));
    assert2::assert!(alert["labels"]["severity"].as_str() == Some("page"));
    assert2::assert!(alert["annotations"]["summary"].as_str() == Some("instance down"));
    assert2::assert!(alert["state"].as_str() == Some("firing"));
    assert2::assert!(alert["activeAt"].as_str() == Some("1970-01-01T00:00:00Z"));
    assert2::assert!(alert["value"].as_str() == Some("1"));
}

const RULE_GROUP_YAML: &str = "
name: latency
interval: 30s
rules:
  - record: job:http_request_duration_seconds:p99
    expr: histogram_quantile(0.99, sum by (le, job) (rate(http_request_duration_seconds_bucket[5m])))
  - alert: HighLatency
    expr: job:http_request_duration_seconds:p99 > 1
    for: 5m
    labels:
      severity: page
    annotations:
      summary: high latency
";

fn labels(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in pairs {
        labels.insert(*name, *value);
    }
    labels
}

fn first_streamed_payload(body: &[u8]) -> &[u8] {
    let mut length = 0_usize;
    let mut header = 0_usize;
    for (index, byte) in body.iter().copied().enumerate() {
        length |= usize::from(byte & 0x7f) << (index * 7);
        header = index + 1;
        if byte & 0x80 == 0 {
            break;
        }
    }
    let payload_start = header + 4;
    &body[payload_start..payload_start + length]
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("json response")
}

async fn response_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    String::from_utf8(bytes.to_vec()).expect("utf8 response")
}

#[tokio::test]
async fn query_endpoint_returns_prometheus_vector_envelope() {
    let app = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 10_000, 1.0)
        .app();

    let response = get(&app, "/api/v1/query?query=up&time=10").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "vector");
    assert2::assert!(body["data"]["result"][0]["metric"]["__name__"].as_str() == Some("up"));
    assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
    assert2::assert!(
        // Prometheus MarshalTimestamp emits whole seconds as a bare JSON integer.
        body["data"]["result"][0]["value"][0].as_i64() == Some(10)
    );
    assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
}

#[tokio::test]
async fn query_endpoint_accepts_rfc3339_time_parameter() {
    let app = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 10_000, 1.0)
        .app();

    let response = get(&app, "/api/v1/query?query=up&time=1970-01-01T00%3A00%3A10Z").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "vector");
    assert2::assert!(body["data"]["result"][0]["value"][0].as_i64() == Some(10));
    assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
}

#[tokio::test]
async fn query_endpoint_returns_native_histogram_envelope() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        {
            let mut histogram = float_histogram(HistogramTotals {
                count: 4.0,
                sum: 10.0,
            });
            histogram.zero_count = -1.0;
            histogram
        },
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/query?query=request_duration_seconds&time=10").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "vector");
    assert2::assert!(body["data"]["result"][0].get("value").is_none());
    assert2::assert!(
        body["data"]["result"][0]["metric"]["__name__"].as_str()
            == Some("request_duration_seconds")
    );
    assert2::assert!(body["data"]["result"][0]["histogram"][0].as_i64() == Some(10));
    assert2::assert!(body["data"]["result"][0]["histogram"][1]["count"].as_str() == Some("4"));
    assert2::assert!(body["data"]["result"][0]["histogram"][1]["sum"].as_str() == Some("10"));
    assert2::assert!(
        body["data"]["result"][0]["histogram"][1]
            .get("buckets")
            .is_none()
    );
}

#[tokio::test]
async fn query_endpoint_returns_native_histogram_buckets() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        {
            let mut histogram = float_histogram(HistogramTotals {
                count: 10.0,
                sum: 7.0,
            });
            histogram.zero_threshold = 0.25;
            histogram.zero_count = 3.0;
            histogram.positive_spans = vec![BucketSpan {
                offset: 0,
                length: 4,
            }];
            histogram.positive_counts = vec![2.0, 0.0, -1.0, 4.0];
            histogram.negative_spans = vec![BucketSpan {
                offset: 0,
                length: 1,
            }];
            histogram.negative_counts = vec![1.0];
            histogram
        },
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/query?query=request_duration_seconds&time=10").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(
        body["data"]["result"][0]["histogram"][1]["buckets"]
            == serde_json::json!([
                [1, "-1", "-0.5", "1"],
                [3, "-0.25", "0.25", "3"],
                [0, "0.5", "1", "2"],
                [0, "2", "4", "-1"],
                [0, "4", "8", "4"],
            ])
    );
}

#[tokio::test]
async fn query_endpoint_returns_native_histogram_custom_buckets() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        {
            let mut histogram = float_histogram(HistogramTotals {
                count: 6.0,
                sum: 2.3,
            });
            histogram.schema = -53;
            histogram.positive_spans = vec![BucketSpan {
                offset: 0,
                length: 3,
            }];
            histogram.positive_counts = vec![1.0, 2.0, 3.0];
            histogram.custom_values = Some(vec![0.1, 0.5]);
            histogram
        },
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/query?query=request_duration_seconds&time=10").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(
        body["data"]["result"][0]["histogram"][1]["buckets"]
            == serde_json::json!([
                [3, "-Inf", "0.1", "1"],
                [0, "0.1", "0.5", "2"],
                [0, "0.5", "+Inf", "3"],
            ])
    );
}

#[tokio::test]
async fn query_endpoint_accepts_post_form_body() {
    let app = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 10_000, 1.0)
        .app();

    let response = post_form(&app, "/api/v1/query", "query=up&time=10").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "vector");
    assert2::assert!(body["data"]["result"][0]["metric"]["__name__"].as_str() == Some("up"));
    assert2::assert!(body["data"]["result"][0]["value"][0].as_i64() == Some(10));
    assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
}

#[tokio::test]
async fn query_endpoint_honors_limit_parameter_for_vectors() {
    let app = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 10_000, 1.0)
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 10_000, 2.0)
        .app();

    let response = get(&app, "/api/v1/query?query=up&time=10&limit=1").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "vector");
    assert2::assert!(body["data"]["result"].as_array().unwrap().len() == 1);
}

#[tokio::test]
async fn query_endpoint_treats_zero_limit_as_disabled() {
    let app = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 10_000, 1.0)
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 10_000, 2.0)
        .app();

    let response = get(&app, "/api/v1/query?query=up&time=10&limit=0").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "vector");
    assert2::assert!(body["data"]["result"].as_array().unwrap().len() == 2);
}

#[tokio::test]
async fn query_endpoint_rejects_invalid_limit_parameter() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/query?query=up&time=10&limit=abc").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert_error_envelope(
        &body,
        ApiError {
            error_type: "bad_data",
            message: "invalid limit parameter",
        },
    );
}

#[tokio::test]
async fn query_range_endpoint_is_available_under_mimir_prefix() {
    let app = up_api_at_60_and_120().app();

    let response = get(
        &app,
        "/prometheus/api/v1/query_range?query=up&start=60&end=120&step=60",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_up_api_matrix(&body);
}

#[tokio::test]
async fn matrix_integral_second_ts_is_bare_integer() {
    let app = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 60_000, 1.0)
        .app();

    let response = get(&app, "/api/v1/query_range?query=up&start=60&end=60&step=60").await;

    assert2::assert!(response.status() == StatusCode::OK);
    let text = response_text(response).await;
    // Byte-exact: a whole-second timestamp is a bare integer, never `60.0`.
    assert2::assert!(text.contains("[60,\"1\"]"));
    assert2::assert!(!text.contains("60.0"));

    let body: Value = serde_json::from_str(&text).expect("json response");
    let ts = &body["data"]["result"][0]["values"][0][0];
    assert2::assert!(ts == 60);
}

#[tokio::test]
async fn query_range_endpoint_can_use_query_frontend_split_and_merge() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0, 1.0), (60_000, 2.0), (120_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            ts_ms,
            value,
        );
    }
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval: millis(60_000),
                shard_count: 1,
            },
        ),
    );
    let app = prometheus_router(state);

    let response = get(&app, "/api/v1/query_range?query=up&start=0&end=120&step=60").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "matrix");
    assert2::assert!(body["data"]["result"][0]["metric"]["job"] == "api");
    assert2::assert!(body["data"]["result"][0]["values"][0][1] == "1");
    assert2::assert!(body["data"]["result"][0]["values"][1][1] == "2");
    assert2::assert!(body["data"]["result"][0]["values"][2][1] == "3");
}

fn labels_on_two_query_shards() -> (Labels, Labels) {
    let mut shard_one = None;
    let mut shard_two = None;
    for id in 0..100 {
        let candidate = labels(&[("__name__", "up"), ("series", &id.to_string())]);
        if candidate.fingerprint().is_multiple_of(2) && shard_one.is_none() {
            shard_one = Some(candidate);
        } else if candidate.fingerprint() % 2 == 1 && shard_two.is_none() {
            shard_two = Some(candidate);
        }
        if shard_one.is_some() && shard_two.is_some() {
            break;
        }
    }
    (
        shard_one.expect("series for first query shard"),
        shard_two.expect("series for second query shard"),
    )
}

fn labels_on_uneven_query_shards() -> (Labels, Labels, Labels) {
    let mut first_even = None;
    let mut second_even = None;
    let mut odd = None;
    for id in 0..100 {
        let candidate = labels(&[("__name__", "up"), ("series", &id.to_string())]);
        if candidate.fingerprint().is_multiple_of(2) {
            if first_even.is_none() {
                first_even = Some(candidate);
            } else if second_even.is_none() {
                second_even = Some(candidate);
            }
        } else if odd.is_none() {
            odd = Some(candidate);
        }
        if first_even.is_some() && second_even.is_some() && odd.is_some() {
            break;
        }
    }
    (
        first_even.expect("first series for first query shard"),
        second_even.expect("second series for first query shard"),
        odd.expect("series for second query shard"),
    )
}

/// Runs an instant-width `query_range` at `t=0` through a two-shard query
/// frontend over `samples` (taken at `t=0`), and returns the matrix body.
async fn sharded_query_range(samples: TenantFloats, encoded_query: &str) -> Value {
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(samples.store()), EngineOpts::default())
            .with_query_frontend(QueryFrontendOptions {
                split_interval: millis(60_000),
                shard_count: 2,
            }),
    );
    let app = prometheus_router(state);
    let response = get(
        &app,
        format!("/api/v1/query_range?query={encoded_query}&start=0&end=0&step=60"),
    )
    .await;
    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "matrix");
    body
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_aggregations() {
    let (shard_one, shard_two) = labels_on_two_query_shards();
    let (first_even, second_even, odd) = labels_on_uneven_query_shards();
    let two_shards = |one_value, two_value| {
        TenantFloats::new()
            .sample(shard_one.clone(), 0, one_value)
            .sample(shard_two.clone(), 0, two_value)
    };
    let cases = [
        ("sum%28up%29", two_shards(1.0, 2.0), "3"),
        (
            "avg%28up%29",
            TenantFloats::new()
                .sample(first_even.clone(), 0, 2.0)
                .sample(second_even.clone(), 0, 10.0)
                .sample(odd.clone(), 0, 3.0),
            "5",
        ),
        ("min%28up%29", two_shards(5.0, 2.0), "2"),
        ("max%28up%29", two_shards(5.0, 2.0), "5"),
        ("group%28up%29", two_shards(5.0, 2.0), "1"),
    ];
    for (encoded_query, series, expected) in cases {
        let body = sharded_query_range(series, encoded_query).await;
        assert2::assert!(
            body["data"]["result"][0]["metric"] == serde_json::json!({}),
            "{encoded_query}"
        );
        assert2::assert!(
            body["data"]["result"][0]["values"][0][0] == 0,
            "{encoded_query}"
        );
        assert2::assert!(
            body["data"]["result"][0]["values"][0][1] == expected,
            "{encoded_query}"
        );
    }
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_stdvar() {
    let (first_even, second_even, odd) = labels_on_uneven_query_shards();

    let body = sharded_query_range(
        TenantFloats::new()
            .sample(first_even, 0, 2.0)
            .sample(second_even, 0, 10.0)
            .sample(odd, 0, 3.0),
        "stdvar%28up%29",
    )
    .await;

    assert2::assert!(body["data"]["result"][0]["metric"] == serde_json::json!({}));
    assert2::assert!(body["data"]["result"][0]["values"][0][0] == 0);
    let value = body["data"]["result"][0]["values"][0][1]
        .as_str()
        .expect("stdvar sample value")
        .parse::<f64>()
        .expect("stdvar sample parses as float");
    assert2::assert!((value - (38.0 / 3.0)).abs() < 1e-9);
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_topk() {
    let (first_even, second_even, odd) = labels_on_uneven_query_shards();
    let expected_high = second_even
        .get("series")
        .expect("high value series label")
        .to_string();
    let expected_mid = odd
        .get("series")
        .expect("mid value series label")
        .to_string();

    let body = sharded_query_range(
        TenantFloats::new()
            .sample(first_even, 0, 2.0)
            .sample(second_even, 0, 10.0)
            .sample(odd, 0, 3.0),
        "topk%282%2C%20up%29",
    )
    .await;

    let mut selected = body["data"]["result"]
        .as_array()
        .expect("topk result array")
        .iter()
        .map(|series| {
            (
                series["metric"]["series"]
                    .as_str()
                    .expect("series label")
                    .to_string(),
                series["values"][0][1]
                    .as_str()
                    .expect("sample value")
                    .parse::<f64>()
                    .expect("sample value parses"),
            )
        })
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| left.0.cmp(&right.0));
    let mut expected = vec![(expected_high, 10.0), (expected_mid, 3.0)];
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    assert2::assert!(selected == expected);
}

#[tokio::test]
async fn query_range_endpoint_honors_limit_parameter_for_matrices() {
    let app = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 60_000, 1.0)
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 60_000, 2.0)
        .app();

    let response = get(
        &app,
        "/api/v1/query_range?query=up&start=60&end=60&step=60&limit=1",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "matrix");
    assert2::assert!(body["data"]["result"].as_array().unwrap().len() == 1);
}

#[tokio::test]
async fn query_range_endpoint_accepts_post_form_body() {
    let app = up_api_at_60_and_120().app();

    let response = post_form(
        &app,
        "/api/v1/query_range",
        "query=up&start=60&end=120&step=60",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_up_api_matrix(&body);
}

#[tokio::test]
async fn query_range_endpoint_accepts_duration_literal_step() {
    let app = up_api_at_60_and_120().app();

    let response = get(
        &app,
        "/api/v1/query_range?query=up&start=60&end=120&step=1m",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_result_type(&body, "matrix");
    assert2::assert!(body["data"]["result"][0]["values"][0][0] == 60);
    assert2::assert!(body["data"]["result"][0]["values"][0][1] == "1");
    assert2::assert!(body["data"]["result"][0]["values"][1][0] == 120);
    assert2::assert!(body["data"]["result"][0]["values"][1][1] == "2");
}

#[tokio::test]
async fn query_range_endpoint_rejects_end_before_start() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(
        &app,
        "/api/v1/query_range?query=up&start=120&end=60&step=60",
    )
    .await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert_error_envelope(
        &body,
        ApiError {
            error_type: "bad_data",
            message: "end timestamp must not be before start time",
        },
    );
}

#[tokio::test]
async fn query_range_endpoint_rejects_invalid_limit_parameter() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(
        &app,
        "/api/v1/query_range?query=up&start=60&end=120&step=60&limit=abc",
    )
    .await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert_error_envelope(
        &body,
        ApiError {
            error_type: "bad_data",
            message: "invalid limit parameter",
        },
    );
}

#[tokio::test]
async fn query_range_endpoint_returns_prometheus_error_for_missing_step() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/query_range?query=up&start=60&end=120").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert_error_envelope(
        &body,
        ApiError {
            error_type: "bad_data",
            message: "missing step parameter",
        },
    );
}

/// Grafana Mimir answers a query without `X-Scope-OrgID` with `401` and the
/// plain-text body `no org id`, before its Prometheus API handler runs.
#[tokio::test]
async fn query_endpoint_requires_scope_org_id() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = send(
        &app,
        Request::builder()
            .uri("/api/v1/query?query=up&time=10")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert2::assert!(response.status() == StatusCode::UNAUTHORIZED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert2::assert!(body.as_ref() == b"no org id\n");
}

#[tokio::test]
async fn query_endpoint_returns_422_when_max_samples_is_exceeded() {
    let store = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 10_000, 1.0)
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 10_000, 1.0)
        .store();
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts {
            max_samples: 1,
            ..EngineOpts::default()
        },
    ));
    let app = prometheus_router(state);

    let response = get(&app, "/api/v1/query?query=up&time=10").await;

    let body = json_with_status(response, StatusCode::UNPROCESSABLE_ENTITY).await;
    assert_error_envelope(
        &body,
        ApiError {
            error_type: "execution",
            message: "samples per query exceeded: observed 2 above limit 1",
        },
    );
}

#[tokio::test]
async fn query_endpoint_applies_runtime_max_samples_per_query() {
    let app = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 10_000, 1.0)
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 10_000, 1.0)
        .limited_app(Limits {
            max_samples_per_query: 1,
            ..Limits::default()
        });

    let response = get(&app, "/api/v1/query?query=up&time=10").await;

    let body = json_with_status(response, StatusCode::UNPROCESSABLE_ENTITY).await;
    assert_error_envelope(
        &body,
        ApiError {
            error_type: "execution",
            message: "samples per query exceeded: observed 2 above limit 1",
        },
    );
}

#[tokio::test]
async fn series_endpoint_returns_matching_label_sets() {
    let app = up_api_a_and_web_b().app();

    let response = get(
        &app,
        "/api/v1/series?match%5B%5D=up%7Bjob%3D%22api%22%7D&start=10&end=10",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_only_up_api_a_series(&body);
}

#[tokio::test]
async fn series_endpoint_accepts_or_label_matchers() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "db"), ("instance", "c")]),
            10_000,
            1.0,
        )
        .app();

    let response = get(
        &app,
        "/api/v1/series?match%5B%5D=up%7Bjob%3D%22api%22%20or%20job%3D%22web%22%7D&start=10&end=10",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    let data = body["data"].as_array().expect("data array");
    assert2::assert!(data.len() == 2);
    let jobs = data
        .iter()
        .map(|series| series["job"].as_str().expect("job"))
        .collect::<std::collections::BTreeSet<_>>();
    assert2::assert!(jobs == std::collections::BTreeSet::from(["api", "web"]));
}

#[tokio::test]
async fn series_endpoint_accepts_post_form_body() {
    let app = up_api_a_and_web_b().app();

    let response = post_form(
        &app,
        "/api/v1/series",
        "match%5B%5D=up%7Bjob%3D%22api%22%7D&start=10&end=10",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_only_up_api_a_series(&body);
}

#[tokio::test]
async fn series_endpoint_honors_limit_parameter() {
    let app = up_api_a_and_web_b().app();

    let response = get(
        &app,
        "/api/v1/series?match%5B%5D=up&start=10&end=10&limit=1",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"].as_array().expect("data array").len() == 1);
}

#[tokio::test]
async fn series_endpoint_rejects_invalid_limit_parameter() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/series?limit=abc").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert_error_envelope(
        &body,
        ApiError {
            error_type: "bad_data",
            message: "invalid limit parameter",
        },
    );
}

#[tokio::test]
async fn series_endpoint_rejects_end_before_start() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/series?match%5B%5D=up&start=20&end=10").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert_error_envelope(
        &body,
        ApiError {
            error_type: "bad_data",
            message: "end timestamp must not be before start time",
        },
    );
}

#[tokio::test]
async fn labels_endpoint_returns_label_names_for_matchers() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "errors_total"), ("job", "api")]),
            10_000,
            1.0,
        )
        .app();

    let response = get(&app, "/api/v1/labels?match%5B%5D=up&start=10&end=10").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"] == serde_json::json!(["__name__", "instance", "job"]));
}

#[tokio::test]
async fn labels_endpoint_accepts_post_form_body() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "errors_total"), ("job", "api")]),
            10_000,
            1.0,
        )
        .app();

    let response = post_form(&app, "/api/v1/labels", "match%5B%5D=up&start=10&end=10").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"] == serde_json::json!(["__name__", "instance", "job"]));
}

#[tokio::test]
async fn labels_endpoint_honors_limit_parameter() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .app();

    let response = get(
        &app,
        "/api/v1/labels?match%5B%5D=up&start=10&end=10&limit=2",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"] == serde_json::json!(["__name__", "instance"]));
}

#[tokio::test]
async fn label_values_endpoint_is_available_under_mimir_prefix() {
    let mut store = InMemoryMetricStore::new();
    for job in ["api", "web"] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", job)]),
            10_000,
            1.0,
        );
    }
    let app = prometheus_router(api_state(store));

    let response = get(
        &app,
        "/prometheus/api/v1/label/job/values?match%5B%5D=up&start=10&end=10",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"] == serde_json::json!(["api", "web"]));
}

#[tokio::test]
async fn label_values_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    for job in ["api", "web"] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", job)]),
            10_000,
            1.0,
        );
    }
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "errors_total"), ("job", "ignored")]),
        10_000,
        1.0,
    );
    let app = prometheus_router(api_state(store));

    let response = post_form(
        &app,
        "/api/v1/label/job/values",
        "match%5B%5D=up&start=10&end=10",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"] == serde_json::json!(["api", "web"]));
}

#[tokio::test]
async fn label_values_endpoint_honors_limit_parameter() {
    let mut store = InMemoryMetricStore::new();
    for job in ["api", "web"] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", job)]),
            10_000,
            1.0,
        );
    }
    let app = prometheus_router(api_state(store));

    let response = get(
        &app,
        "/api/v1/label/job/values?match%5B%5D=up&start=10&end=10&limit=1",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"] == serde_json::json!(["api"]));
}

#[tokio::test]
async fn metadata_endpoint_is_available_under_mimir_prefix() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/prometheus/api/v1/metadata").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"] == serde_json::json!({}));
}

#[tokio::test]
async fn metadata_endpoint_returns_metric_metadata() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata(
        "tenant-b",
        "http_requests_total",
        "gauge",
        "Wrong tenant.",
        "",
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/metadata?metric=http_requests_total").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(
        body["data"]["http_requests_total"]
            .as_array()
            .unwrap()
            .len()
            == 1
    );
    assert2::assert!(body["data"]["http_requests_total"][0]["type"] == "counter");
    assert2::assert!(body["data"]["http_requests_total"][0]["help"] == "Total HTTP requests.");
    assert2::assert!(body["data"]["http_requests_total"][0]["unit"] == "requests");
}

#[tokio::test]
async fn metadata_endpoint_honors_limit_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/metadata?limit=1").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"].as_object().unwrap().len() == 1);
    assert2::assert!(body["data"]["http_requests_total"][0]["type"] == "counter");
}

#[tokio::test]
async fn metadata_endpoint_treats_zero_limit_as_disabled() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/metadata?limit=0").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"].as_object().unwrap().len() == 2);
}

#[tokio::test]
async fn metadata_endpoint_rejects_invalid_limit_parameter_with_prometheus_error() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/metadata?limit=abc").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert_error_envelope(
        &body,
        ApiError {
            error_type: "bad_data",
            message: "invalid limit parameter",
        },
    );
}

#[tokio::test]
async fn metadata_endpoint_honors_limit_per_metric_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "HTTP requests from another target.",
        "requests",
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/metadata?limit_per_metric=1").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"].as_object().unwrap().len() == 2);
    assert2::assert!(
        body["data"]["http_requests_total"]
            .as_array()
            .unwrap()
            .len()
            == 1
    );
    assert2::assert!(body["data"]["up"].as_array().unwrap().len() == 1);
}

#[tokio::test]
async fn rules_endpoint_returns_empty_groups() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/rules").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["groups"].clone() == serde_json::json!([]));
}

#[tokio::test]
async fn rules_endpoint_rejects_invalid_exclude_alerts_parameter_with_prometheus_error() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/rules?exclude_alerts=maybe").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(body["error"].as_str() == Some("invalid exclude_alerts parameter"));
}

#[tokio::test]
async fn rules_endpoint_returns_loaded_recording_rules() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    configure_team_a_rules(&app, RULE_GROUP_YAML).await;

    let response = get(&app, "/api/v1/rules").await;

    let body = json_with_status(response, StatusCode::OK).await;
    let group = &body["data"]["groups"][0];
    let rule = &group["rules"][0];
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(group["file"].as_str() == Some("team-a"));
    assert2::assert!(group["interval"].as_i64() == Some(30));
    assert2::assert!(group["lastEvaluation"].as_str() == Some("0001-01-01T00:00:00Z"));
    assert2::assert!(group["evaluationTime"].as_f64() == Some(0.0));
    assert2::assert!(group["sourceTenants"] == serde_json::json!([]));
    assert2::assert!(group.get("lastError").is_none());
    assert2::assert!(group.get("limit").is_none());
    assert2::assert!(group["name"].as_str() == Some("latency"));
    assert2::assert!(rule["lastEvaluation"].as_str() == Some("0001-01-01T00:00:00Z"));
    assert2::assert!(rule["evaluationTime"].as_f64() == Some(0.0));
    assert2::assert!(rule["lastError"].as_str() == Some(""));
    assert2::assert!(rule["health"].as_str() == Some("ok"));
    assert2::assert!(rule["name"].as_str() == Some("job:http_request_duration_seconds:p99"));
    assert2::assert!(
        rule["query"].as_str()
            == Some(
                "histogram_quantile(0.99, sum by (le, job) (rate(http_request_duration_seconds_bucket[5m])))"
            )
    );
    assert2::assert!(rule["type"].as_str() == Some("recording"));
}

#[tokio::test]
async fn rules_endpoint_reports_group_last_evaluation_from_ruler_state() {
    let state = api_state(InMemoryMetricStore::new());
    let app = prometheus_router(Arc::clone(&state));

    configure_team_a_rules(&app, RULE_GROUP_YAML).await;

    state.apply_ruler_group_state(RulerGroupStateRecord {
        tenant: "tenant-a".to_string(),
        namespace: "team-a".to_string(),
        group: "latency".to_string(),
        last_eval_ms: 90_000,
    });

    let response = get(&app, "/api/v1/rules").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["data"]["groups"][0]["lastEvaluation"] == "1970-01-01T00:01:30Z");
}

#[tokio::test]
async fn rules_endpoint_filters_by_rule_type() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    configure_team_a_rules(&app, RULE_GROUP_YAML).await;

    let response = get(&app, "/api/v1/rules?type=alert").await;
    let body = json_with_status(response, StatusCode::OK).await;
    let alert_rules = body["data"]["groups"][0]["rules"].as_array().unwrap();
    assert2::assert!(alert_rules.len() == 1);
    assert2::assert!(alert_rules[0]["type"].as_str() == Some("alerting"));
    assert2::assert!(alert_rules[0]["name"].as_str() == Some("HighLatency"));
    assert2::assert!(alert_rules[0]["duration"].as_i64() == Some(300));
    assert2::assert!(alert_rules[0]["labels"]["severity"].as_str() == Some("page"));
    assert2::assert!(alert_rules[0]["annotations"]["summary"].as_str() == Some("high latency"));

    let response = get(&app, "/api/v1/rules?type=record").await;
    let body = json_with_status(response, StatusCode::OK).await;
    let recording_rules = body["data"]["groups"][0]["rules"].as_array().unwrap();
    assert2::assert!(recording_rules.len() == 1);
    assert2::assert!(recording_rules[0]["type"].as_str() == Some("recording"));
    assert2::assert!(
        recording_rules[0]["name"].as_str() == Some("job:http_request_duration_seconds:p99")
    );
}

#[tokio::test]
async fn rules_endpoint_rejects_invalid_type_parameter() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/rules?type=notify").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(body["error"].as_str() == Some("not supported value \"notify\""));
}

#[tokio::test]
async fn rules_endpoint_can_exclude_alert_payloads() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    configure_team_a_rules(&app, RULE_GROUP_YAML).await;

    let response = get(&app, "/api/v1/rules?type=alert").await;
    let body = json_with_status(response, StatusCode::OK).await;
    let alert_rule = &body["data"]["groups"][0]["rules"][0];
    assert2::assert!(alert_rule.get("alerts").is_some());

    let response = get(&app, "/api/v1/rules?type=alert&exclude_alerts=true").await;
    let body = json_with_status(response, StatusCode::OK).await;
    let alert_rule = &body["data"]["groups"][0]["rules"][0];
    assert2::assert!(alert_rule["type"].as_str() == Some("alerting"));
    assert2::assert!(alert_rule["name"].as_str() == Some("HighLatency"));
    assert2::assert!(alert_rule.get("alerts").is_none());
}

#[tokio::test]
async fn rules_endpoint_embeds_evaluated_alerts() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            0,
            1.0,
        )
        .app();

    configure_team_a_rules(&app, INSTANCE_DOWN_PAGE_YAML).await;

    let response = get(&app, "/api/v1/rules?type=alert").await;

    let body = json_with_status(response, StatusCode::OK).await;
    let rule = &body["data"]["groups"][0]["rules"][0];
    let alerts = rule["alerts"].as_array().unwrap();
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(rule["name"].as_str() == Some("InstanceDown"));
    assert2::assert!(rule["lastEvaluation"].as_str() == Some("1970-01-01T00:00:00Z"));
    assert2::assert!(alerts.len() == 1);
    assert_firing_instance_down_alert(&alerts[0]);
}

#[tokio::test]
async fn rules_endpoint_expands_value_and_labels_in_alert_templates() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            0,
            2.0,
        )
        .app();

    configure_team_a_rules(
        &app,
        "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    labels:
      detail: 'v={{ $value }}'
    annotations:
      summary: '{{ $labels.job }} is {{ $value }}'
      passthrough: '{{ humanize $value }}'
",
    )
    .await;

    let response = get(&app, "/api/v1/rules?type=alert").await;

    let body = json_with_status(response, StatusCode::OK).await;
    let alerts = body["data"]["groups"][0]["rules"][0]["alerts"]
        .as_array()
        .unwrap();
    // Variables and Prometheus helper functions use the shared Go-template runtime.
    assert2::assert!(alerts.len() == 1);
    assert2::assert!(alerts[0]["annotations"]["summary"].as_str() == Some("api is 2"));
    assert2::assert!(alerts[0]["annotations"]["passthrough"].as_str() == Some("2"));
    assert2::assert!(alerts[0]["labels"]["detail"].as_str() == Some("v=2"));
}

#[tokio::test]
async fn rules_endpoint_reports_alert_evaluation_errors_per_rule() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    configure_team_a_rules(
        &app,
        "
name: unsupported
rules:
  - alert: UnsupportedAlert
    expr: label_replace(up, \"dst\", \"$1\", \"src\", \"(\")
",
    )
    .await;

    let response = get(&app, "/api/v1/rules?type=alert").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    let rule = &body["data"]["groups"][0]["rules"][0];
    assert2::assert!(rule["name"].as_str() == Some("UnsupportedAlert"));
    assert2::assert!(rule["health"].as_str() == Some("err"));
    assert2::assert!(
        rule["lastError"]
            .as_str()
            .is_some_and(|error| !error.is_empty())
    );
    assert2::assert!(rule["alerts"].clone() == serde_json::json!([]));
}

#[tokio::test]
async fn alerts_endpoint_returns_empty_alerts_under_mimir_prefix() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/prometheus/api/v1/alerts").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["alerts"].clone() == serde_json::json!([]));
}

#[tokio::test]
async fn alerts_endpoint_omits_inactive_configured_alerting_rules() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    configure_team_a_rules(&app, RULE_GROUP_YAML).await;

    let response = get(&app, "/prometheus/api/v1/alerts").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert2::assert!(alerts.is_empty());
}

#[tokio::test]
async fn alerts_endpoint_evaluates_alerting_rules() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            0,
            1.0,
        )
        .app();

    configure_team_a_rules(&app, INSTANCE_DOWN_PAGE_YAML).await;

    let response = get(&app, "/prometheus/api/v1/alerts").await;

    let body = json_with_status(response, StatusCode::OK).await;
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(alerts.len() == 1);
    assert2::assert!(alerts[0]["name"].as_str() == Some("InstanceDown"));
    assert2::assert!(alerts[0]["query"].as_str() == Some("up > 0"));
    assert2::assert!(alerts[0]["duration"].as_i64() == Some(0));
    assert_firing_instance_down_alert(&alerts[0]);
}

#[tokio::test]
async fn alerts_endpoint_marks_for_duration_alerts_pending() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            0,
            1.0,
        )
        .app();

    configure_team_a_rules(
        &app,
        "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    for: 5m
",
    )
    .await;

    let response = get(&app, "/prometheus/api/v1/alerts").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert2::assert!(alerts.len() == 1);
    assert2::assert!(alerts[0]["duration"].as_i64() == Some(300));
    assert2::assert!(alerts[0]["state"].as_str() == Some("pending"));
    assert2::assert!(alerts[0]["activeAt"].as_str() == Some("1970-01-01T00:00:00Z"));
    assert2::assert!(alerts[0]["value"].as_str() == Some("1"));
}

#[tokio::test]
async fn alerts_endpoint_fires_for_duration_alerts_after_active_duration() {
    let store = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            0,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            300_000,
            1.0,
        )
        .store();
    let state = api_state(store);
    let app = prometheus_router(Arc::clone(&state));

    configure_team_a_rules(
        &app,
        "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    for: 5m
",
    )
    .await;

    let response = get(&app, "/prometheus/api/v1/alerts").await;
    let body = json_with_status(response, StatusCode::OK).await;
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert2::assert!(alerts.len() == 1);
    assert2::assert!(alerts[0]["state"].as_str() == Some("pending"));
    assert2::assert!(alerts[0]["activeAt"].as_str() == Some("1970-01-01T00:00:00Z"));

    state.set_ruler_evaluation_time_ms(300_000);
    let response = get(&app, "/prometheus/api/v1/alerts").await;
    let body = json_with_status(response, StatusCode::OK).await;
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert2::assert!(alerts.len() == 1);
    assert2::assert!(alerts[0]["duration"].as_i64() == Some(300));
    assert2::assert!(alerts[0]["state"].as_str() == Some("firing"));
    assert2::assert!(alerts[0]["activeAt"].as_str() == Some("1970-01-01T00:00:00Z"));
    assert2::assert!(alerts[0]["value"].as_str() == Some("1"));
}

#[tokio::test]
async fn alerts_endpoint_replays_compacted_alert_state() {
    let store = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            300_000,
            1.0,
        )
        .store();
    let state = api_state(store);
    state.set_ruler_evaluation_time_ms(300_000);
    let app = prometheus_router(Arc::clone(&state));

    configure_team_a_rules(
        &app,
        "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    for: 5m
",
    )
    .await;

    let alert_labels = BTreeMap::from([
        ("alertname".to_string(), "InstanceDown".to_string()),
        ("instance".to_string(), "a".to_string()),
        ("job".to_string(), "api".to_string()),
    ]);
    state.apply_ruler_alert_state(RulerAlertStateRecord {
        tenant: "tenant-a".to_string(),
        rule_id: "InstanceDown\nup > 0".to_string(),
        labels: alert_labels.into(),
        active_since_ms: Some(0),
        keep_firing_until_ms: None,
    });

    let response = get(&app, "/prometheus/api/v1/alerts").await;

    let body = json_with_status(response, StatusCode::OK).await;
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert2::assert!(alerts.len() == 1);
    assert2::assert!(alerts[0]["state"].as_str() == Some("firing"));
    assert2::assert!(alerts[0]["activeAt"].as_str() == Some("1970-01-01T00:00:00Z"));
}

#[tokio::test]
async fn alertmanagers_endpoint_returns_empty_discovery_lists() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = send(
        &app,
        Request::builder()
            .uri("/prometheus/api/v1/alertmanagers")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["activeAlertmanagers"].clone() == serde_json::json!([]));
    assert2::assert!(body["data"]["droppedAlertmanagers"].clone() == serde_json::json!([]));
}

#[tokio::test]
async fn targets_endpoint_returns_empty_discovery_lists() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = send(
        &app,
        Request::builder()
            .uri("/prometheus/api/v1/targets")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["activeTargets"].clone() == serde_json::json!([]));
    assert2::assert!(body["data"]["droppedTargets"].clone() == serde_json::json!([]));
    assert2::assert!(body["data"]["droppedTargetCounts"].clone() == serde_json::json!({}));
}

#[tokio::test]
async fn scrape_pools_endpoint_returns_empty_pool_list() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = send(
        &app,
        Request::builder()
            .uri("/prometheus/api/v1/scrape_pools")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"].clone() == serde_json::json!([]));
}

#[tokio::test]
async fn target_metadata_endpoint_returns_empty_metadata_list() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(
        &app,
        "/prometheus/api/v1/targets/metadata?metric=up&limit=1",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"].clone() == serde_json::json!([]));
}

#[tokio::test]
async fn target_metadata_endpoint_returns_metric_metadata() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    let app = prometheus_router(api_state(store));

    let response = get(
        &app,
        "/prometheus/api/v1/targets/metadata?metric=http_requests_total&limit=1",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"].as_array().unwrap().len() == 1);
    assert2::assert!(
        body["data"][0].clone()
            == serde_json::json!({
                "target": {},
                "metric": "http_requests_total",
                "type": "counter",
                "help": "Total HTTP requests.",
                "unit": "requests",
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_post_form_body() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/v1/format_query")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(Body::from("query=sum%28up%29"))
            .unwrap(),
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"].as_str() == Some("sum(up)"));
}

#[tokio::test]
async fn parse_query_endpoint_accepts_post_form_body() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/v1/parse_query")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(Body::from("query=up"))
            .unwrap(),
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_up_vector_selector(&body, &serde_json::json!([]));
}

#[tokio::test]
async fn ruler_config_rules_crud_round_trips_yaml_groups() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    configure_team_a_rules(&app, RULE_GROUP_YAML).await;

    let response = get(&app, "/prometheus/config/v1/rules").await;
    assert2::assert!(response.status() == StatusCode::OK);
    assert2::assert!(response.headers()["Content-Type"].to_str().unwrap() == "application/yaml");
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&response_text(response).await).expect("ruler yaml");
    assert2::assert!(yaml["team-a"][0]["name"].as_str() == Some("latency"));
    assert2::assert!(yaml["team-a"][0]["interval"].as_str() == Some("30s"));
    assert2::assert!(
        yaml["team-a"][0]["rules"][0]["record"].as_str()
            == Some("job:http_request_duration_seconds:p99")
    );

    let response = get(&app, "/prometheus/config/v1/rules/team-a/latency").await;
    assert2::assert!(response.status() == StatusCode::OK);
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&response_text(response).await).expect("group yaml");
    assert2::assert!(yaml["name"] == "latency");

    let response = send(
        &app,
        Request::builder()
            .uri("/prometheus/config/v1/rules")
            .header("X-Scope-OrgID", "tenant-b")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert2::assert!(response.status() == StatusCode::OK);
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&response_text(response).await).expect("tenant yaml");
    assert2::assert!(yaml == serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));

    let response = send(
        &app,
        Request::builder()
            .method("DELETE")
            .uri("/prometheus/config/v1/rules/team-a/latency")
            .header("X-Scope-OrgID", "tenant-a")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert2::assert!(response.status() == StatusCode::ACCEPTED);

    let response = get(&app, "/prometheus/config/v1/rules/team-a").await;
    assert2::assert!(response.status() == StatusCode::OK);
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&response_text(response).await).expect("namespace yaml");
    assert2::assert!(yaml == serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
}

#[tokio::test]
async fn ruler_config_rejects_invalid_rule_groups() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = post_yaml(
        &app,
        "/prometheus/config/v1/rules/team-a",
        "
name: broken
rules:
  - record: missing_expr
",
    )
    .await;
    assert2::assert!(response.status() == StatusCode::BAD_REQUEST);
    assert2::assert!(response_text(response).await.contains("expr"));

    let response = post_yaml(
        &app,
        "/prometheus/config/v1/rules/team-a",
        "
name: broken
rules:
  - record: bad_query
    expr: sum(
",
    )
    .await;
    assert2::assert!(response.status() == StatusCode::BAD_REQUEST);
    assert2::assert!(response_text(response).await.contains("PromQL"));

    let response = get(&app, "/prometheus/config/v1/rules").await;
    assert2::assert!(response.status() == StatusCode::OK);
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&response_text(response).await).expect("tenant yaml");
    assert2::assert!(yaml == serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
}

#[tokio::test]
async fn query_exemplars_endpoint_returns_empty_list() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/query_exemplars?query=up&start=10&end=20").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    assert2::assert!(body["data"] == serde_json::json!([]));
}

#[tokio::test]
async fn query_exemplars_endpoint_returns_matching_exemplars() {
    let mut store = TenantFloats::new()
        .sample(
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            10_000,
            1.0,
        )
        .store();
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "api")]),
        labels(&[("trace_id", "abc"), ("span_id", "def")]),
        10_500,
        7.0,
    );
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "web")]),
        labels(&[("trace_id", "ignored")]),
        10_500,
        9.0,
    );
    let app = prometheus_router(api_state(store));

    let response = get(
        &app,
        "/api/v1/query_exemplars?query=http_requests_total%7Bjob%3D%22api%22%7D&start=10&end=11",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"].as_array().expect("data array").len() == 1);
    assert2::assert!(
        body["data"][0]["seriesLabels"]["__name__"].as_str() == Some("http_requests_total")
    );
    assert2::assert!(body["data"][0]["seriesLabels"]["job"].as_str() == Some("api"));
    assert2::assert!(body["data"][0]["exemplars"][0]["labels"]["trace_id"].as_str() == Some("abc"));
    assert2::assert!(body["data"][0]["exemplars"][0]["labels"]["span_id"].as_str() == Some("def"));
    assert2::assert!(body["data"][0]["exemplars"][0]["value"].as_str() == Some("7"));
    assert2::assert!(body["data"][0]["exemplars"][0]["timestamp"].as_f64() == Some(10.5));
}

#[tokio::test]
async fn query_exemplars_endpoint_accepts_or_label_matchers() {
    let mut store = InMemoryMetricStore::new();
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "api")]),
        labels(&[("trace_id", "api")]),
        10_500,
        7.0,
    );
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "web")]),
        labels(&[("trace_id", "web")]),
        10_600,
        9.0,
    );
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "db")]),
        labels(&[("trace_id", "db")]),
        10_700,
        11.0,
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/query_exemplars?query=http_requests_total%7Bjob%3D%22api%22%20or%20job%3D%22web%22%7D&start=10&end=11").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"] == "success");
    let data = body["data"].as_array().expect("data array");
    assert2::assert!(data.len() == 2);
    let jobs = data
        .iter()
        .map(|series| series["seriesLabels"]["job"].as_str().expect("job"))
        .collect::<std::collections::BTreeSet<_>>();
    assert2::assert!(jobs == std::collections::BTreeSet::from(["api", "web"]));
}

#[tokio::test]
async fn query_exemplars_endpoint_accepts_post_form_body() {
    let mut store = TenantFloats::new()
        .sample(
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            10_000,
            1.0,
        )
        .store();
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "api")]),
        labels(&[("trace_id", "abc"), ("span_id", "def")]),
        10_500,
        7.0,
    );
    let app = prometheus_router(api_state(store));

    let response = post_form(
        &app,
        "/api/v1/query_exemplars",
        "query=http_requests_total%7Bjob%3D%22api%22%7D&start=10&end=11",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"].as_array().expect("data array").len() == 1);
    assert2::assert!(body["data"][0]["seriesLabels"]["job"].as_str() == Some("api"));
    assert2::assert!(body["data"][0]["exemplars"][0]["labels"]["trace_id"].as_str() == Some("abc"));
    assert2::assert!(body["data"][0]["exemplars"][0]["timestamp"].as_f64() == Some(10.5));
}

#[tokio::test]
async fn query_exemplars_endpoint_rejects_end_before_start() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/query_exemplars?query=up&start=20&end=10").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(body["error"].as_str() == Some("end timestamp must not be before start time"));
}

#[tokio::test]
async fn remote_read_endpoint_applies_configured_body_cap() {
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_remote_read_max_body(bytes(1)),
    );
    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .body(Body::from(vec![0_u8; 2]))
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn remote_read_endpoint_returns_snappy_protobuf_response() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));
    let request = pb::v1::ReadRequest {
        queries: Vec::new(),
        accepted_response_types: Vec::new(),
    };
    let response = remote_read(&app, &request).await;

    assert2::assert!(response.status() == StatusCode::OK);
    assert2::assert!(
        response.headers()["Content-Type"].to_str().unwrap() == "application/x-protobuf"
    );
    assert2::assert!(response.headers()["Content-Encoding"].to_str().unwrap() == "snappy");
    let read_response = decode_read_response(response).await;
    assert2::assert!(read_response.results.is_empty());
}

#[tokio::test]
async fn remote_read_endpoint_accepts_listed_snappy_content_encoding() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));
    let request = pb::v1::ReadRequest {
        queries: Vec::new(),
        accepted_response_types: Vec::new(),
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/v1/read")
            .header("X-Scope-OrgID", "tenant-a")
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "identity, snappy")
            .body(Body::from(compressed))
            .unwrap(),
    )
    .await;

    assert2::assert!(response.status() == StatusCode::OK);
}

#[tokio::test]
async fn remote_read_endpoint_rejects_end_before_start() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));
    let request = pb::v1::ReadRequest {
        queries: vec![read_query(
            MillisRange {
                start_ms: 20_000,
                end_ms: 10_000,
            },
            Vec::new(),
        )],
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    };
    let response = remote_read(&app, &request).await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(body["error"].as_str() == Some("end timestamp must not be before start time"));
}

#[tokio::test]
async fn remote_read_endpoint_rejects_invalid_or_oversized_hint_ranges() {
    let limits = Limits {
        max_query_length: krabka_units::secs(10),
        ..Limits::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );
    let app = prometheus_router(state);
    for (start_ms, end_ms, expected_status) in [
        (20_000, 10_000, StatusCode::BAD_REQUEST),
        (1, 30_000, StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        let request = pb::v1::ReadRequest {
            queries: vec![pb::v1::Query {
                start_timestamp_ms: 10_000,
                end_timestamp_ms: 20_000,
                matchers: Vec::new(),
                hints: Some(pb::v1::ReadHints {
                    start_ms,
                    end_ms,
                    ..Default::default()
                }),
            }],
            accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
        };
        let response = remote_read(&app, &request).await;
        assert2::assert!(response.status() == expected_status);
        let body = response_json(response).await;
        assert2::assert!(body["status"] == "error");
    }
}

#[tokio::test]
async fn remote_read_endpoint_returns_matching_float_samples() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            20_000,
            2.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            30_000,
            3.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
            20_000,
            9.0,
        )
        .app();
    let request = pb::v1::ReadRequest {
        queries: [
            None,
            Some((0, 0)),
            Some((20_000, 20_000)),
            Some((0, 10_000)),
            Some((20_000, 0)),
            Some((1, 30_000)),
        ]
        .into_iter()
        .map(|bounds| pb::v1::Query {
            start_timestamp_ms: 10_000,
            end_timestamp_ms: 20_000,
            matchers: vec![eq_matcher("__name__", "up"), eq_matcher("job", "api")],
            hints: bounds.map(|(start_ms, end_ms)| pb::v1::ReadHints {
                start_ms,
                end_ms,
                ..Default::default()
            }),
        })
        .collect(),
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    };
    let response = remote_read(&app, &request).await;

    assert2::assert!(response.status() == StatusCode::OK);
    let read_response = decode_read_response(response).await;
    let series = &read_response.results[0].timeseries[0];
    assert2::assert!(read_response.results.len() == 6);
    assert2::assert!(read_response.results[0].timeseries.len() == 1);
    assert2::assert!(
        series
            .labels
            .iter()
            .map(|label| (label.name.as_str(), label.value.as_str()))
            .collect::<Vec<_>>()
            == vec![("__name__", "up"), ("instance", "a"), ("job", "api")]
    );
    assert2::assert!(
        read_response
            .results
            .iter()
            .map(|result| {
                assert2::assert!(result.timeseries.len() == 1);
                result.timeseries[0]
                    .samples
                    .iter()
                    .map(|sample| (sample.timestamp, sample.value))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            == vec![
                vec![(10_000, 1.0), (20_000, 2.0)],
                vec![(10_000, 1.0), (20_000, 2.0)],
                vec![(20_000, 2.0)],
                vec![(10_000, 1.0)],
                vec![(20_000, 2.0)],
                vec![(10_000, 1.0), (20_000, 2.0), (30_000, 3.0)],
            ]
    );
}

#[tokio::test]
async fn remote_read_endpoint_rejects_selected_series_over_tenant_limit() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "b")]),
            10_000,
            1.0,
        )
        .limited_app(Limits {
            max_fetched_series_per_query: 1,
            ..Limits::default()
        });
    let request = up_api_samples_request(10_000);
    let response = remote_read(&app, &request).await;

    assert_execution_error_containing(response, "series per query exceeded").await;
}

#[tokio::test]
async fn remote_read_endpoint_rejects_samples_over_tenant_limit() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            20_000,
            2.0,
        )
        .limited_app(Limits {
            max_samples_per_query: 1,
            ..Limits::default()
        });
    let request = up_api_samples_request(20_000);
    let response = remote_read(&app, &request).await;

    assert_execution_error_containing(response, "samples per query exceeded").await;
}

#[tokio::test]
async fn remote_read_endpoint_returns_matching_exemplars() {
    let mut store = TenantFloats::new()
        .sample(
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            1.0,
        )
        .store();
    store.push_exemplar(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        labels(&[("trace_id", "abc"), ("span_id", "def")]),
        10_500,
        7.0,
    );
    let app = prometheus_router(api_state(store));
    let request = pb::v1::ReadRequest {
        queries: vec![read_query(
            MillisRange {
                start_ms: 10_000,
                end_ms: 11_000,
            },
            vec![eq_matcher("__name__", "http_requests_total")],
        )],
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    };
    let response = remote_read(&app, &request).await;

    assert2::assert!(response.status() == StatusCode::OK);
    let read_response = decode_read_response(response).await;
    let series = &read_response.results[0].timeseries[0];
    assert2::assert!(series.exemplars.len() == 1);
    assert2::assert!(series.exemplars[0].timestamp == 10_500);
    assert2::assert!((series.exemplars[0].value - 7.0).abs() < f64::EPSILON);
    assert2::assert!(
        series.exemplars[0]
            .labels
            .iter()
            .map(|label| (label.name.as_str(), label.value.as_str()))
            .collect::<Vec<_>>()
            == vec![("span_id", "def"), ("trace_id", "abc")]
    );
}

#[tokio::test]
async fn remote_read_endpoint_returns_matching_native_histograms() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        float_histogram(HistogramTotals {
            count: 4.0,
            sum: 10.0,
        }),
    );
    let state = api_state(store);
    let app = prometheus_router(Arc::clone(&state));
    let request = pb::v1::ReadRequest {
        queries: vec![read_query(
            MillisRange {
                start_ms: 10_000,
                end_ms: 10_000,
            },
            vec![eq_matcher("__name__", "request_duration_seconds")],
        )],
        accepted_response_types: vec![
            pb::v1::ResponseType::StreamedXorChunks as i32,
            pb::v1::ResponseType::Samples as i32,
        ],
    };
    let response = remote_read(&app, &request).await;

    assert2::assert!(response.status() == StatusCode::OK);
    assert2::assert!(
        response.headers()["Content-Type"]
            == "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
    );
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let streamed = pb::v1::ChunkedReadResponse::decode(first_streamed_payload(&bytes))
        .expect("streamed remote read response");
    let chunk = &streamed.chunked_series[0].chunks[0];
    assert2::assert!(chunk.r#type == pb::v1::chunk::Encoding::FloatHistogram as i32);
    assert2::assert!(chunk.min_time_ms == 10_000);
    assert2::assert!(chunk.max_time_ms == 10_000);
    assert2::assert!(chunk.data.starts_with(&[0, 1, 0x40]));

    let request = pb::v1::ReadRequest {
        queries: vec![read_query(
            MillisRange {
                start_ms: 10_000,
                end_ms: 10_000,
            },
            vec![eq_matcher("__name__", "request_duration_seconds")],
        )],
        accepted_response_types: vec![pb::v1::ResponseType::StreamedXorChunks as i32],
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");
    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let streamed = pb::v1::ChunkedReadResponse::decode(first_streamed_payload(&bytes))
        .expect("streamed remote read response");
    assert2::assert!(
        streamed.chunked_series[0].chunks[0].r#type
            == pb::v1::chunk::Encoding::FloatHistogram as i32
    );
}

#[tokio::test]
async fn remote_read_endpoint_streams_prometheus_xor_frames() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api")]),
            7_200_000,
            12_000.0,
        )
        .app();
    let request = pb::v1::ReadRequest {
        queries: vec![read_query(
            MillisRange {
                start_ms: 7_200_000,
                end_ms: 7_200_000,
            },
            vec![eq_matcher("__name__", "up")],
        )],
        accepted_response_types: vec![
            pb::v1::ResponseType::StreamedXorChunks as i32,
            pb::v1::ResponseType::Samples as i32,
        ],
    };
    let response = remote_read(&app, &request).await;

    assert2::assert!(response.status() == StatusCode::OK);
    assert2::assert!(
        response.headers()["Content-Type"].to_str().unwrap()
            == "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
    );
    assert2::assert!(response.headers().get("Content-Encoding").is_none());
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("streamed response");
    let payload = first_streamed_payload(&body);
    let response = pb::v1::ChunkedReadResponse::decode(payload).expect("chunked read response");
    assert2::assert!(response.query_index == 0);
    assert2::assert!(response.chunked_series.len() == 1);
    let series = &response.chunked_series[0];
    assert2::assert!(series.chunks.len() == 1);
    assert2::assert!(series.chunks[0].r#type == pb::v1::chunk::Encoding::Xor as i32);
    assert2::assert!(series.chunks[0].min_time_ms == 7_200_000);
    assert2::assert!(series.chunks[0].max_time_ms == 7_200_000);
}

#[tokio::test]
async fn cardinality_label_names_endpoint_returns_label_name_counts() {
    let mut store = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 10_000, 1.0)
        .store();
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "up"), ("job", "other"), ("zone", "hidden")]),
        10_000,
        1.0,
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/cardinality/label_names").await;

    let body = json_with_status(response, StatusCode::OK).await;
    // Mimir returns the cardinality object directly, with no status envelope.
    assert2::assert!(body.get("status").is_none());
    assert2::assert!(body["label_names_count"].as_i64() == Some(3));
    assert2::assert!(body["label_values_count_total"].as_i64() == Some(4));
    assert2::assert!(
        body["cardinality"].clone()
            == serde_json::json!([
                {"label_name": "job", "label_values_count": 2},
                {"label_name": "__name__", "label_values_count": 1},
                {"label_name": "instance", "label_values_count": 1},
            ])
    );
}

#[tokio::test]
async fn cardinality_label_names_endpoint_honors_limit_parameter() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 10_000, 1.0)
        .app();

    let response = get(&app, "/api/v1/cardinality/label_names?limit=1").await;

    let body = json_with_status(response, StatusCode::OK).await;
    // job has two distinct values, so it sorts first under the limit.
    assert2::assert!(
        body["cardinality"]
            .as_array()
            .expect("cardinality array")
            .len()
            == 1
    );
    assert2::assert!(body["cardinality"][0]["label_name"].as_str() == Some("job"));
    assert2::assert!(body["cardinality"][0]["label_values_count"].as_i64() == Some(2));
}

#[tokio::test]
async fn cardinality_label_names_endpoint_filters_selector_parameter() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("zone", "us")]),
            10_000,
            1.0,
        )
        .app();

    let response = get(
        &app,
        "/api/v1/cardinality/label_names?selector=up%7Bjob%3D%22api%22%7D",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["label_names_count"].as_i64() == Some(3));
    assert2::assert!(body["label_values_count_total"].as_i64() == Some(3));
    assert2::assert!(
        body["cardinality"].clone()
            == serde_json::json!([
                {"label_name": "__name__", "label_values_count": 1},
                {"label_name": "instance", "label_values_count": 1},
                {"label_name": "job", "label_values_count": 1},
            ])
    );
}

#[tokio::test]
async fn cardinality_label_names_endpoint_accepts_post_form_body() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 10_000, 1.0)
        .app();

    let response = post_form(&app, "/api/v1/cardinality/label_names", "limit=1").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(
        body["cardinality"]
            .as_array()
            .expect("cardinality array")
            .len()
            == 1
    );
    assert2::assert!(body["cardinality"][0]["label_name"].as_str() == Some("job"));
}

#[tokio::test]
async fn cardinality_label_names_endpoint_rejects_invalid_limit_parameter() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/cardinality/label_names?limit=abc").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(body["error"].as_str() == Some("invalid limit parameter"));
}

#[tokio::test]
async fn cardinality_label_names_endpoint_accepts_documented_count_methods() {
    let app = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 10_000, 1.0)
        .app();

    for count_method in ["inmemory", "active"] {
        let response = get(
            &app,
            format!("/api/v1/cardinality/label_names?count_method={count_method}"),
        )
        .await;

        let body = json_with_status(response, StatusCode::OK).await;
        assert2::assert!(
            body["cardinality"]
                .as_array()
                .expect("cardinality array")
                .len()
                == 2
        );
    }
}

#[tokio::test]
async fn cardinality_label_names_endpoint_rejects_invalid_count_method_parameter() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/cardinality/label_names?count_method=blocks").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(body["error"].as_str() == Some("invalid count_method parameter"));
}

#[tokio::test]
async fn cardinality_active_series_endpoint_returns_series_labels_under_mimir_prefix() {
    let mut store = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            20_000,
            2.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
            10_000,
            1.0,
        )
        .store();
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "up"), ("job", "hidden"), ("instance", "z")]),
        10_000,
        1.0,
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/prometheus/api/v1/cardinality/active_series").await;

    let body = json_with_status(response, StatusCode::OK).await;
    // Mimir active_series returns a bare object whose `data` array holds flat
    // label maps -- no status envelope, no seriesLabels/metric wrapper.
    assert2::assert!(body.get("status").is_none());
    assert2::assert!(
        body["data"].clone()
            == serde_json::json!([
                {"__name__": "up", "instance": "a", "job": "api"},
                {"__name__": "up", "instance": "b", "job": "web"},
            ])
    );
}

#[tokio::test]
async fn cardinality_active_series_endpoint_filters_selector_parameter() {
    let app = up_api_a_and_web_b().app();

    let response = get(
        &app,
        "/prometheus/api/v1/cardinality/active_series?selector=up%7Bjob%3D%22api%22%7D",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body.get("status").is_none());
    assert2::assert!(
        body["data"].clone()
            == serde_json::json!([
                {"__name__": "up", "instance": "a", "job": "api"},
            ])
    );
}

#[tokio::test]
async fn cardinality_active_series_endpoint_honors_limit_parameter() {
    let app = up_api_a_and_web_b().app();

    let response = get(&app, "/prometheus/api/v1/cardinality/active_series?limit=1").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body.get("status").is_none());
    assert2::assert!(body["data"].as_array().expect("data array").len() == 1);
}

#[tokio::test]
async fn cardinality_active_series_endpoint_accepts_post_form_body() {
    let app = up_api_a_and_web_b().app();

    let response = post_form(
        &app,
        "/prometheus/api/v1/cardinality/active_series",
        "selector=up%7Bjob%3D%22api%22%7D",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body.get("status").is_none());
    assert2::assert!(
        body["data"].clone()
            == serde_json::json!([
                {"__name__": "up", "instance": "a", "job": "api"},
            ])
    );
}

#[tokio::test]
async fn cardinality_label_values_endpoint_returns_label_value_counts() {
    let mut store = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            20_000,
            2.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "b")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "c")]),
            10_000,
            1.0,
        )
        .store();
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "up"), ("job", "hidden"), ("instance", "z")]),
        10_000,
        1.0,
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/cardinality/label_values").await;

    let body = json_with_status(response, StatusCode::OK).await;
    // Mimir nests per-value cardinality under each label, with no envelope.
    assert2::assert!(body.get("status").is_none());
    assert2::assert!(body["series_count_total"].as_i64() == Some(3));
    assert2::assert!(
        body["labels"].clone()
            == serde_json::json!([
            {
                "label_name": "__name__",
                "label_values_count": 1,
                "series_count": 3,
                "cardinality": [{"label_value": "up", "series_count": 3}],
            },
            {
                "label_name": "instance",
                "label_values_count": 3,
                "series_count": 3,
                "cardinality": [
                    {"label_value": "a", "series_count": 1},
                    {"label_value": "b", "series_count": 1},
                    {"label_value": "c", "series_count": 1},
                ],
            },
            {
                "label_name": "job",
                "label_values_count": 2,
                "series_count": 3,
                "cardinality": [
                    {"label_value": "api", "series_count": 2},
                    {"label_value": "web", "series_count": 1},
                ],
            },
            ])
    );
}

#[tokio::test]
async fn cardinality_label_values_endpoint_filters_label_names_parameter() {
    let app = up_api_a_and_web_b().app();

    let response = get(
        &app,
        "/api/v1/cardinality/label_values?label_names%5B%5D=job",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["series_count_total"].as_i64() == Some(2));
    assert2::assert!(
        body["labels"].clone()
            == serde_json::json!([
                {
                    "label_name": "job",
                    "label_values_count": 2,
                    "series_count": 2,
                    "cardinality": [
                        {"label_value": "api", "series_count": 1},
                        {"label_value": "web", "series_count": 1},
                    ],
                },
            ])
    );
}

#[tokio::test]
async fn cardinality_label_values_endpoint_filters_selector_parameter() {
    let app = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("zone", "us")]),
            10_000,
            1.0,
        )
        .app();

    let response = get(
        &app,
        "/api/v1/cardinality/label_values?selector=up%7Bjob%3D%22api%22%7D&label_names%5B%5D=job",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["series_count_total"].as_i64() == Some(1));
    assert2::assert!(
        body["labels"].clone()
            == serde_json::json!([
                {
                    "label_name": "job",
                    "label_values_count": 1,
                    "series_count": 1,
                    "cardinality": [{"label_value": "api", "series_count": 1}],
                },
            ])
    );
}

#[tokio::test]
async fn cardinality_label_values_endpoint_honors_limit_parameter() {
    let app = up_api_a_and_web_b().app();

    let response = get(&app, "/api/v1/cardinality/label_values?limit=1").await;

    let body = json_with_status(response, StatusCode::OK).await;
    // limit caps each label's nested per-value cardinality array.
    let labels = body["labels"].as_array().expect("labels array");
    assert2::assert!(!labels.is_empty());
    for label in labels {
        assert2::assert!(
            label["cardinality"]
                .as_array()
                .expect("cardinality array")
                .len()
                == 1
        );
    }
}

#[tokio::test]
async fn cardinality_label_values_endpoint_accepts_post_form_body() {
    let app = up_api_a_and_web_b().app();

    let response = post_form(&app, "/api/v1/cardinality/label_values", "limit=2").await;

    let body = json_with_status(response, StatusCode::OK).await;
    let labels = body["labels"].as_array().expect("labels array");
    // __name__ has the highest series_count, so it sorts first.
    assert2::assert!(body.get("status").is_none());
    assert2::assert!(body["series_count_total"].as_i64() == Some(2));
    assert2::assert!(labels.len() == 3);
    assert2::assert!(labels[0]["label_name"].as_str() == Some("__name__"));
}

#[tokio::test]
async fn cardinality_label_values_endpoint_rejects_invalid_limit_parameter() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/cardinality/label_values?limit=abc").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(body["error"].as_str() == Some("invalid limit parameter"));
}

#[tokio::test]
async fn format_query_endpoint_returns_formatted_expression() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/format_query?query=foo%2Fbar").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"].as_str() == Some("foo / bar"));
}

#[tokio::test]
async fn histogram_trim_round_trips_through_get_and_post_ast_apis_and_evaluation() {
    let mut store = InMemoryMetricStore::new();
    // Schema 0: one observation in (0.5, 1], three in (1, 2].
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "h"), ("job", "api")]),
        10_000,
        {
            let mut histogram = float_histogram(HistogramTotals {
                count: 4.0,
                sum: 5.0,
            });
            histogram.positive_spans = vec![BucketSpan {
                offset: 0,
                length: 2,
            }];
            histogram.positive_counts = vec![1.0, 3.0];
            histogram
        },
    );
    let app = prometheus_router(api_state(store));
    let query = "sum(histogram_count((h </ 2) >/ 1)) + 1";
    let encoded = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("query", query)
        .finish();
    for method in ["GET", "POST"] {
        for endpoint in ["format_query", "parse_query"] {
            let uri = if method == "GET" {
                format!("/api/v1/{endpoint}?{encoded}")
            } else {
                format!("/prometheus/api/v1/{endpoint}")
            };
            let response = send(
                &app,
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("X-Scope-OrgID", "tenant-a")
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(if method == "POST" {
                        Body::from(encoded.clone())
                    } else {
                        Body::empty()
                    })
                    .unwrap(),
            )
            .await;
            let body = json_with_status(response, StatusCode::OK).await;
            if endpoint == "format_query" {
                assert2::assert!(body["data"] == query);
            } else {
                assert2::assert!(body["data"]["lhs"]["expr"]["args"][0]["op"] == ">/");
                assert2::assert!(
                    body["data"]["lhs"]["expr"]["args"][0]["lhs"]["expr"]["op"] == "</"
                );
                assert2::assert!(body["data"]["lhs"]["expr"]["func"]["name"] == "histogram_count");
            }
        }
    }
    let response = get(&app, format!("/api/v1/query?{encoded}&time=10")).await;
    assert2::assert!(response.status() == StatusCode::OK);
    assert2::assert!(
        response_json(response).await["data"]["result"]
            == serde_json::json!([{"metric":{},"value":[10,"4"]}])
    );
    // Trim is not a comparison: bool is illegal, scalar operands parse but fail evaluation.
    for (query, endpoint, status) in [
        ("h </ bool 2", "parse_query", StatusCode::BAD_REQUEST),
        ("1 </ 2", "parse_query", StatusCode::OK),
        ("1 </ 2", "query", StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        let encoded = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .finish();
        let response = get(&app, format!("/api/v1/{endpoint}?{encoded}&time=10")).await;
        assert2::assert!(response.status() == status, "{query}");
    }
}

#[tokio::test]
async fn parse_query_endpoint_is_available_under_mimir_prefix() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(
        &app,
        "/prometheus/api/v1/parse_query?query=up%7Bjob%3D%22api%22%7D",
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert_up_vector_selector(
        &body,
        &serde_json::json!([
            {
                "name": "job",
                "type": "=",
                "value": "api"
            }
        ]),
    );
}

#[tokio::test]
async fn parse_query_endpoint_returns_prometheus_error_for_missing_query() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/parse_query").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(body["error"].as_str() == Some("missing query parameter"));
}

#[tokio::test]
async fn status_buildinfo_endpoint_returns_prometheus_envelope() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/status/buildinfo").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["version"].as_str() == Some(env!("CARGO_PKG_VERSION")));
    assert2::assert!(
        body["data"]["revision"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert2::assert!(
        body["data"]["branch"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert2::assert!(
        body["data"]["buildUser"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert2::assert!(
        body["data"]["buildDate"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert2::assert!(body["data"]["goVersion"].as_str() == Some("not applicable (Rust)"));
}

#[tokio::test]
async fn status_flags_endpoint_returns_prometheus_flag_strings() {
    let opts = EngineOpts {
        lookback_delta: minutes(7),
        ..EngineOpts::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), opts)
            .with_max_concurrent_queries(11)
            .with_runtime_status("debug", Some(hours(2))),
    );
    let app = prometheus_router(state);

    let response = send(
        &app,
        Request::builder()
            .uri("/api/v1/status/flags")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["query.lookback-delta"].as_str() == Some("7m"));
    assert2::assert!(body["data"]["query.max-concurrency"].as_str() == Some("11"));
    assert2::assert!(body["data"]["log.level"].as_str() == Some("debug"));
    assert2::assert!(body["data"]["storage.tsdb.retention.time"].as_str() == Some("2h"));
}

#[tokio::test]
async fn status_config_endpoint_is_available_under_mimir_prefix() {
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_max_concurrent_queries(9)
            .with_runtime_status("info", Some(hours(3))),
    );
    let app = prometheus_router(state);

    let response = send(
        &app,
        Request::builder()
            .uri("/prometheus/api/v1/status/config")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["yaml"].as_str().is_some_and(|yaml| {
        yaml.contains("scrape_config: not applicable")
            && yaml.contains("query_max_concurrency: 9")
            && yaml.contains("storage_retention: 3h")
            && !yaml.contains("scrape_interval")
    }));
}

#[tokio::test]
async fn status_tsdb_endpoint_returns_tenant_cardinality_stats() {
    let mut store = TenantFloats::new()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
            20_000,
            2.0,
        )
        .sample(
            labels(&[("__name__", "errors_total"), ("job", "api")]),
            30_000,
            3.0,
        )
        .store();
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "hidden"), ("job", "ignored")]),
        10_000,
        1.0,
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/status/tsdb?limit=2").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["headStats"]["numSeries"].as_i64() == Some(3));
    assert2::assert!(body["data"]["headStats"]["minTime"].as_i64() == Some(10_000));
    assert2::assert!(body["data"]["headStats"]["maxTime"].as_i64() == Some(30_000));
    assert2::assert!(
        body["data"]["seriesCountByMetricName"].clone()
            == serde_json::json!([
                {"name": "up", "value": 2},
                {"name": "errors_total", "value": 1},
            ])
    );
    assert2::assert!(
        body["data"]["labelValueCountByLabelName"].clone()
            == serde_json::json!([
                {"name": "__name__", "value": 2},
                {"name": "instance", "value": 2},
            ])
    );
    assert2::assert!(
        body["data"]["seriesCountByLabelValuePair"].clone()
            == serde_json::json!([
                {"name": "__name__=up", "value": 2},
                {"name": "job=api", "value": 2},
            ])
    );
}

#[tokio::test]
async fn status_tsdb_endpoint_rejects_invalid_limit_parameter_with_prometheus_error() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/api/v1/status/tsdb?limit=abc").await;

    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"].as_str() == Some("error"));
    assert2::assert!(body["errorType"].as_str() == Some("bad_data"));
    assert2::assert!(body["error"].as_str() == Some("invalid limit parameter"));
}

#[tokio::test]
async fn status_tsdb_blocks_endpoint_returns_empty_block_list() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = get(&app, "/prometheus/api/v1/status/tsdb/blocks").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["blocks"].clone() == serde_json::json!([]));
}

#[tokio::test]
async fn status_tsdb_blocks_endpoint_returns_compacted_blocks() {
    let mut store = InMemoryMetricStore::new();
    store.push_tsdb_block(
        "tenant-a",
        "metrics/tenant-a/float/0001.parquet",
        10_000,
        70_000,
        42,
        3,
    );
    let app = prometheus_router(api_state(store));

    let response = get(&app, "/api/v1/status/tsdb/blocks").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(
        body["data"]["blocks"][0]["ulid"].as_str() == Some("metrics/tenant-a/float/0001.parquet")
    );
    assert2::assert!(body["data"]["blocks"][0]["minTime"].as_i64() == Some(10_000));
    assert2::assert!(body["data"]["blocks"][0]["maxTime"].as_i64() == Some(70_000));
    assert2::assert!(body["data"]["blocks"][0]["stats"]["numSamples"].as_i64() == Some(42));
    assert2::assert!(body["data"]["blocks"][0]["stats"]["numSeries"].as_i64() == Some(3));
}

#[tokio::test]
async fn status_walreplay_endpoint_reports_live_materialized_offsets_without_claiming_done() {
    let head = WalHead::with_retention(minutes(12));
    head.apply_wal_record_at(
        &WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![("__name__".to_string(), "up".into())],
            payload: SamplePayload::Float {
                timestamp_ms: 10_000,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        },
        PartitionIndex(2),
        Offset(41),
    );
    let wal_tail = RoleReadiness::new().gate("wal-head");
    wal_tail.mark_ready();
    let body = walreplay_status(head, wal_tail).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["min"].as_i64() == Some(41));
    assert2::assert!(body["data"]["max"].is_null());
    assert2::assert!(body["data"]["current"].as_i64() == Some(41));
    assert2::assert!(
        body["data"]["state"].as_str()
            == Some("unknown (WAL tail attached; broker high watermark unavailable)")
    );
}

#[tokio::test]
async fn status_walreplay_reports_a_configured_tail_that_has_not_attached() {
    let head = WalHead::with_retention(minutes(12));
    let wal_tail = RoleReadiness::new().gate("wal-head");
    let body = walreplay_status(head, wal_tail).await;
    assert2::assert!(body["data"]["current"].is_null());
    assert2::assert!(body["data"]["state"].as_str() == Some("waiting (WAL tail is not attached)"));
}

#[tokio::test]
async fn status_walreplay_without_a_tail_is_explicitly_not_applicable() {
    let app = prometheus_router(api_state(InMemoryMetricStore::new()));

    let response = send(
        &app,
        Request::builder()
            .uri("/api/v1/status/walreplay")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["data"]["min"].is_null());
    assert2::assert!(body["data"]["max"].is_null());
    assert2::assert!(body["data"]["current"].is_null());
    assert2::assert!(
        body["data"]["state"].as_str() == Some("not applicable (no WAL head configured)")
    );
}

#[tokio::test]
async fn status_runtimeinfo_endpoint_is_available_under_mimir_prefix() {
    let mut store = TenantFloats::new()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 10_000, 1.0)
        .sample(
            labels(&[("__name__", "errors_total"), ("job", "api")]),
            10_000,
            1.0,
        )
        .store();
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "hidden"), ("job", "ignored")]),
        10_000,
        1.0,
    );
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_runtime_status("warn", Some(minutes(12))),
    );
    let app = prometheus_router(state);

    let response = get(&app, "/prometheus/api/v1/status/runtimeinfo").await;

    let body = json_with_status(response, StatusCode::OK).await;
    assert2::assert!(body["status"].as_str() == Some("success"));
    assert2::assert!(body["data"]["startTime"].as_str().is_some());
    assert2::assert!(body["data"]["serverTime"].as_str().is_some());
    assert2::assert!(body["data"]["reloadConfigSuccess"].as_bool() == Some(true));
    assert2::assert!(body["data"]["timeSeriesCount"].as_i64() == Some(2));
    assert2::assert!(
        body["data"]["hostname"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert2::assert!(
        body["data"]["GOMAXPROCS"]
            .as_u64()
            .is_some_and(|value| value > 0)
    );
    assert2::assert!(body["data"]["storageRetention"].as_str() == Some("12m"));
    assert2::assert!(body["data"]["corruptionCount"].is_null());
    assert2::assert!(body["data"]["corruptionCountStatus"].as_str().is_some());
}

#[tokio::test]
async fn byte_label_identity_is_preserved_until_the_http_json_boundary() {
    let mut store = InMemoryMetricStore::new();
    for bytes in [vec![0xff], vec![0xfe], "�".as_bytes().to_vec()] {
        let mut labels = krabka_promql::PromqlLabels::from_pairs([("__name__", "byte_input")]);
        labels.insert("raw", krabka_promql::PromqlString::from(bytes));
        store.push_float("tenant-a", labels, 10_000, 2.0);
    }
    let app = prometheus_router(api_state(store));
    for (query, expected) in [
        (
            "count(byte_input)",
            serde_json::json!({"resultType":"vector","result":[{"metric":{},"value":[10,"3"]}]}),
        ),
        (
            "sum by(raw)(byte_input)",
            serde_json::json!({"resultType":"vector","result":[{"metric":{"raw":"�"},"value":[10,"2"]},{"metric":{"raw":"�"},"value":[10,"2"]},{"metric":{"raw":"�"},"value":[10,"2"]}]}),
        ),
        (
            r#"byte_input{raw="\xff"}"#,
            serde_json::json!({"resultType":"vector","result":[{"metric":{"__name__":"byte_input","raw":"�"},"value":[10,"2"]}]}),
        ),
        (
            r#"label_join(label_replace(byte_input{raw="\xff"},"captured","$1","raw","(.*)"),"joined","\xfe","captured","raw")"#,
            serde_json::json!({"resultType":"vector","result":[{"metric":{"__name__":"byte_input","raw":"�","captured":"�","joined":"���"},"value":[10,"2"]}]}),
        ),
    ] {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("time", "10")
            .finish();
        let response = send(
            &app,
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("x-scope-orgid", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
        assert2::assert!(response.status() == StatusCode::OK, "{query}");
        let body = response_json(response).await;
        assert2::assert!(body["data"] == expected, "{query}");
    }
    for (path, expected) in [
        (
            "/api/v1/label/raw/values",
            serde_json::json!(["�", "�", "�"]),
        ),
        (
            "/api/v1/series?match%5B%5D=byte_input%7Braw%3D%22%5Cxff%22%7D",
            serde_json::json!([{"__name__":"byte_input","raw":"�"}]),
        ),
    ] {
        let response = send(
            &app,
            Request::builder()
                .uri(path)
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert2::assert!(response.status() == StatusCode::OK);
        assert2::assert!(response_json(response).await["data"] == expected);
    }
    let body = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("query", r#"byte_input{raw=~"\xff"}"#)
        .append_pair("time", "10")
        .finish();
    let response = send(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header("x-scope-orgid", "tenant-a")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap(),
    )
    .await;
    let body = json_with_status(response, StatusCode::BAD_REQUEST).await;
    assert2::assert!(body["status"] == "error" && body["errorType"] == "bad_data");
}

#[tokio::test]
async fn remote_read_preserves_go_byte_labels_in_samples_and_chunked_frames() {
    use krabka_metrics::wire::remote_read_pb::v1 as raw;
    let mut store = InMemoryMetricStore::new();
    for (bytes, value) in [
        (vec![0xff], 2.0),
        (vec![0xfe], 3.0),
        ("�".as_bytes().to_vec(), 4.0),
    ] {
        let mut labels = krabka_promql::PromqlLabels::from_pairs([("__name__", "byte_input")]);
        labels.insert("raw", krabka_promql::PromqlString::from(bytes));
        store.push_float("tenant-a", labels, 10_000, value);
    }
    let app = prometheus_router(api_state(store));
    for (response_type, selected) in [
        (0, None),
        (0, Some(vec![0xff])),
        (1, None),
        (1, Some(vec![0xfe])),
    ] {
        let mut matchers = vec![raw::LabelMatcher {
            r#type: 0,
            name: "__name__".into(),
            value: b"byte_input".to_vec(),
        }];
        if let Some(bytes) = &selected {
            matchers.push(raw::LabelMatcher {
                r#type: 0,
                name: "raw".into(),
                value: bytes.clone(),
            });
        }
        let request = raw::ReadRequest {
            queries: vec![raw::Query {
                start_timestamp_ms: 10_000,
                end_timestamp_ms: 10_000,
                matchers,
                hints: None,
            }],
            accepted_response_types: vec![response_type],
        };
        let compressed = SnappyEncoder::new()
            .compress_vec(&request.encode_to_vec())
            .unwrap();
        let response = send(
            &app,
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await;
        assert2::assert!(response.status() == StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let mut actual = Vec::new();
        if response_type == 0 {
            let decoded = SnappyDecoder::new().decompress_vec(&body).unwrap();
            for series in raw::ReadResponse::decode(decoded.as_slice())
                .unwrap()
                .results[0]
                .timeseries
                .clone()
            {
                let bytes = series
                    .labels
                    .into_iter()
                    .find(|label| label.name == "raw")
                    .unwrap()
                    .value;
                assert2::assert!(
                    series.samples.len() == 1 && series.samples[0].timestamp == 10_000
                );
                let want: f64 = match bytes.as_slice() {
                    [0xff] => 2.0,
                    [0xfe] => 3.0,
                    _ => 4.0,
                };
                assert2::assert!(series.samples[0].value.to_bits() == want.to_bits());
                actual.push(bytes);
            }
        } else {
            let mut remaining = body.as_ref();
            while !remaining.is_empty() {
                let length =
                    usize::try_from(prost::encoding::decode_varint(&mut remaining).unwrap())
                        .unwrap();
                let checksum = u32::from_be_bytes(remaining[..4].try_into().unwrap());
                let payload = &remaining[4..4 + length];
                assert2::assert!(checksum == crc32c::crc32c(payload));
                let frame = raw::ChunkedReadResponse::decode(payload).unwrap();
                assert2::assert!(frame.query_index == 0 && frame.chunked_series.len() == 1);
                let series = &frame.chunked_series[0];
                assert2::assert!(
                    series.chunks.len() == 1
                        && series.chunks[0].min_time_ms == 10_000
                        && series.chunks[0].max_time_ms == 10_000
                );
                actual.push(
                    series
                        .labels
                        .iter()
                        .find(|label| label.name == "raw")
                        .unwrap()
                        .value
                        .clone(),
                );
                remaining = &remaining[4 + length..];
            }
        }
        actual.sort();
        let mut expected = selected.map_or_else(
            || vec![vec![0xff], vec![0xfe], "�".as_bytes().to_vec()],
            |bytes| vec![bytes],
        );
        expected.sort();
        assert2::assert!(actual == expected);
    }
}

#[tokio::test]
async fn http_alert_templates_reuse_byte_identity_and_replayed_start_times() {
    let values = [
        vec![0xff],
        vec![0xfe],
        "�".as_bytes().to_vec(),
        b"__krabka_bytes_ff".to_vec(),
    ];
    let mut store = InMemoryMetricStore::new();
    for bytes in &values {
        let mut labels = krabka_promql::PromqlLabels::from_pairs([("__name__", "byte_input")]);
        labels.insert("raw", krabka_promql::PromqlString::from(bytes.clone()));
        store.push_float("tenant-a", labels, 60_000, 2.0);
    }
    let state = api_state(store);
    state.set_ruler_evaluation_time_ms(60_000);
    for (index, bytes) in values.iter().enumerate() {
        let mut labels = krabka_promql::PromqlLabels::from_pairs([("alertname", "ByteAlert")]);
        labels.insert("raw", krabka_promql::PromqlString::from(bytes.clone()));
        labels.insert("copied", krabka_promql::PromqlString::from(bytes.clone()));
        labels.insert(
            "from_query",
            krabka_promql::PromqlString::from(bytes.clone()),
        );
        state.apply_ruler_alert_state(krabka_promql::RulerAlertStateRecord {
            tenant: "tenant-a".into(),
            rule_id: "ByteAlert\nbyte_input > 0".into(),
            labels,
            active_since_ms: Some(i64::try_from(index).unwrap() * 1_000),
            keep_firing_until_ms: None,
        });
    }
    let app = prometheus_router(state.clone());
    let rule = r#"
name: bytes
rules:
  - alert: ByteAlert
    expr: byte_input > 0
    for: 1m
    labels:
      raw: '{{ $labels.raw }}'
      copied: '{{ $labels.raw }}'
      from_query: '{{ query (printf "byte_input{raw=%q}" $labels.raw) | first | label "raw" }}'
    annotations:
      origin_name: '{{ $labels.__name__ }}'
      failed_query: '{{ query `absent(` }}'
      dependent: '{{ if eq (query (printf "byte_input{raw=%q}" $labels.raw) | first | value) 2.0 }}{{ query (printf "byte_input{raw=%q}" (query (printf "byte_input{raw=%q}" $labels.raw) | first | label "raw")) | first | value }}{{ else }}wrong{{ end }}'
      skipped: '{{ if false }}{{ query `absent(` }}{{ else }}skipped{{ end }}'
      clock: '{{ now }}'
      fresh_samples: '{{ eq (query (printf "byte_input{raw=%q}" $labels.raw) | first) (query (printf "byte_input{raw=%q}" $labels.raw) | first) }}'
      same_sample: '{{ $sample := query (printf "byte_input{raw=%q}" $labels.raw) | first }}{{ eq $sample $sample }}'
      scalar_query: '{{ query "7" | first | value }}'

"#;
    let configured = post_yaml(&app, "/prometheus/config/v1/rules/bytes", rule).await;
    assert2::assert!(configured.status() == StatusCode::ACCEPTED);
    for time in [60_000, 120_000] {
        state.set_ruler_evaluation_time_ms(time);
        let response = get(&app, "/api/v1/alerts").await;
        let body = json_with_status(response, StatusCode::OK).await;
        let alerts = body["data"]["alerts"].as_array().unwrap();
        assert2::assert!(alerts.len() == 4);
        let mut starts = alerts
            .iter()
            .map(|alert| alert["activeAt"].as_str().unwrap())
            .collect::<Vec<_>>();
        starts.sort_unstable();
        assert2::assert!(
            starts
                == [
                    "1970-01-01T00:00:00Z",
                    "1970-01-01T00:00:01Z",
                    "1970-01-01T00:00:02Z",
                    "1970-01-01T00:00:03Z"
                ]
        );
        for alert in alerts {
            assert2::assert!(
                alert["labels"]["raw"] == alert["labels"]["copied"]
                    && alert["labels"]["from_query"] == alert["labels"]["raw"]
                    && alert["labels"].get("__name__").is_none()
            );
            assert2::assert!(alert["annotations"]["origin_name"] == "byte_input");
            assert2::assert!(alert["annotations"]["dependent"] == "2");
            assert2::assert!(alert["annotations"]["skipped"] == "skipped");
            assert2::assert!(alert["annotations"]["fresh_samples"] == "false");
            assert2::assert!(alert["annotations"]["same_sample"] == "true");
            assert2::assert!(alert["annotations"]["scalar_query"] == "7");
            assert2::assert!(alert["annotations"]["clock"] == (time / 1000).to_string());
            assert2::assert!(
                alert["annotations"]["failed_query"]
                    .as_str()
                    .unwrap()
                    .starts_with("<error expanding template: ")
            );
            let wanted = if time == 120_000 || alert["activeAt"] == "1970-01-01T00:00:00Z" {
                "firing"
            } else {
                "pending"
            };
            assert2::assert!(alert["state"] == wanted);
        }
    }
}

#[tokio::test]
async fn http_alerts_keep_histogram_expressions_and_typed_query_template_values() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        krabka_promql::PromqlLabels::from_pairs([("__name__", "native_input"), ("job", "api")]),
        60_000,
        {
            let mut histogram = float_histogram(HistogramTotals {
                count: 5.0,
                sum: 9.0,
            });
            histogram.schema = -53;
            histogram.reset_hint = ResetHint::Gauge;
            histogram.positive_spans = vec![BucketSpan {
                offset: 0,
                length: 3,
            }];
            histogram.positive_counts = vec![2.0, 0.0, 3.0];
            histogram.custom_values = Some(vec![1.0, 2.0]);
            histogram
        },
    );
    let state = api_state(store);
    state.set_ruler_evaluation_time_ms(60_000);
    let app = prometheus_router(state);
    let rule = r#"
name: native
rules:
  - alert: NativeAlert
    expr: native_input
    labels:
      count: '{{ $value.Count }}'
    annotations:
      typed: '{{ $value.Count }}/{{ $value.Sum }}/{{ $value.Schema }}/{{ $value.UsesCustomBuckets }}'
      buckets: '{{ $value.String }}'
      queried: '{{ query "native_input" | first | value }}'
      invalid_numeric: '{{ $value | humanize }}'
"#;
    let configured = post_yaml(&app, "/prometheus/config/v1/rules/native", rule).await;
    assert2::assert!(configured.status() == StatusCode::ACCEPTED);
    let response = get(&app, "/api/v1/alerts").await;
    let body = json_with_status(response, StatusCode::OK).await;
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert2::assert!(alerts.len() == 1);
    let alert = &alerts[0];
    assert2::assert!(alert["labels"]["count"] == "5" && alert["labels"]["job"] == "api");
    assert2::assert!(alert["labels"].get("__name__").is_none());
    assert2::assert!(alert["annotations"]["typed"] == "5/9/-53/true");
    assert2::assert!(alert["annotations"]["buckets"] == "{count:5, sum:9, [-Inf,1]:2, (2,+Inf]:3}");
    assert2::assert!(alert["annotations"]["queried"] == alert["annotations"]["buckets"]);
    assert2::assert!(
        alert["annotations"]["invalid_numeric"]
            .as_str()
            .unwrap()
            .starts_with("<error expanding template: ")
    );
    assert2::assert!(alert["state"] == "firing" && alert["value"] == "0");
}
