//! Headline multi-tenant isolation across every Tempo HTTP read surface.
//!
//! This is an in-process test. It needs no Docker and is never `#[ignore]`. It
//! boots the real Slice 5 querier router over a real TCP socket and drives it
//! with `reqwest`, so every request flows through the genuine `X-Scope-OrgID`
//! tenant extractor.
//!
//! The sharpest probe here is a *colliding `trace_id`*. Both tenants ingest a
//! trace whose `trace_id` bytes are byte-for-byte identical. The assertions can
//! pass only if isolation happens at the tenant key, before the by-id and
//! row-group lookup, rather than after it. A leak would surface as cross-tenant
//! span bleed, even though neither tenant ever sent the other's spans.
//!
//! Ingest goes through the real distributor OTLP-protobuf door, so the tenant
//! resolves from `X-Scope-OrgID` exactly as in production. The test then loads
//! the captured `SpanRecord`s into the tenant-keyed `InMemorySpanStore` that
//! backs the querier. That matches how the real store namespaces data by
//! tenant.

use std::{
    collections::BTreeMap,
    future::IntoFuture as _,
    iter,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use assert2::check;
use axum::{
    body::Body,
    http::{HeaderValue, Request, StatusCode},
};
use clap::Parser;
use http_body_util::BodyExt as _;
use krabka_blockstore::{TENANT_HEADER, TenantId, TenantPolicy};
use krabka_observability::{
    CancellationToken, RoleReadiness,
    server_security::{
        AuthFailureReason, AuthMethod, SecurityEvents, ServerListener, ServerSecurity,
        ServerSecurityArgs, authenticate_requests, install_crypto_provider, serve_router,
    },
};
use krabka_traceql::{
    AttrValue as TraceqlAttrValue, EngineOpts, InMemorySpanStore, InputSpan, TraceqlEngine,
};
use krabka_traces::{
    AttrValue, Limits, Span, SpanRecord, TracesError,
    distributor::{self, DistributorState, JaegerGrpcService, OtlpGrpcService, WalSink},
    frontend::{
        FrontendConfig, HttpQuerier, MembershipView, MockCatalog, MockQuerier, QuerierScheme,
        QueryFrontend, router_with_backend,
    },
    limits::OverridesProvider,
    querier::http::HttpConfig,
    wire::{
        jaeger_grpc::api_v2::{
            Batch as JaegerBatch, PostSpansRequest, Process as JaegerProcess, Span as JaegerSpan,
            collector_service_client::CollectorServiceClient,
            collector_service_server::CollectorService as _,
        },
        otlp::decode_otlp,
    },
};
use krabka_units::{Time, convert::TimeExt as _};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{
        ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
        trace_service_server::TraceService as _,
    },
    common::v1::{AnyValue, InstrumentationScope, KeyValue as OtlpKeyValue, any_value::Value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan, TracesData},
};
use prost::Message as _;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use reqwest::StatusCode as ReqwestStatusCode;
use serde_json::Value as JsonValue;
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;
use tonic::{
    Code as GrpcCode, Request as GrpcRequest,
    metadata::{AsciiMetadataValue, MetadataMap},
};
use tower::ServiceExt as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The colliding trace identity both tenants ingest under the *same* bytes.
const COLLIDING_TRACE_ID: [u8; 16] = [0xAB; 16];
const COLLIDING_TRACE_ID_HEX: &str = "abababababababababababababababab";

/// Attribute key present only in tenant-a's spans.
const TENANT_A_ONLY_KEY: &str = "tenant_only";
const TENANT_A_ONLY_VALUE: &str = "A";

#[derive(Clone, Default)]
struct CapturingSink {
    records: Arc<Mutex<Vec<SpanRecord>>>,
}

impl CapturingSink {
    /// The tenant of every record appended so far, in order.
    fn tenants(&self) -> Vec<String> {
        self.records
            .lock()
            .expect("capturing sink lock poisoned")
            .iter()
            .map(|record| record.tenant.clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl WalSink for CapturingSink {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
        self.records
            .lock()
            .map_err(|_| TracesError::Wal("capturing sink lock poisoned".into()))?
            .push(rec);
        Ok(())
    }
}

/// A running querier bound to a real ephemeral socket.
struct TestServer {
    base_url: String,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

impl TestServer {
    fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

/// Push one tenant's OTLP payload through the real distributor door and return
/// the captured `SpanRecord`s for that tenant.
async fn ingest(tenant: &str, otlp_body: &[u8]) -> TestResult<Vec<SpanRecord>> {
    let sink = CapturingSink::default();
    let state = Arc::new(DistributorState::new(Arc::new(sink.clone())));
    let resp = authenticated(distributor::router(state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/x-protobuf")
                .header(TENANT_HEADER, tenant)
                .body(Body::from(otlp_body.to_vec()))?,
        )
        .await?;
    assert2::assert!(resp.status() == StatusCode::OK);
    let _ = resp.into_body().collect().await?;
    let records = sink
        .records
        .lock()
        .map_err(|_| "capturing sink lock poisoned")?
        .clone();
    Ok(records)
}

/// Boot the querier over a real socket, atop a tenant-keyed store seeded from
/// `records`. The overrides drive per-tenant limit enforcement.
async fn start_querier(
    records: Vec<SpanRecord>,
    overrides: OverridesProvider,
) -> TestResult<TestServer> {
    let store = Arc::new(TraceqlEngine::new(
        Arc::new(span_store_from_records(&records)),
        EngineOpts::default(),
    ));
    let cfg = HttpConfig {
        overrides,
        ..HttpConfig::default()
    };
    let app = authenticated(krabka_traces::querier::http::router_with_config(
        store,
        cfg,
        RoleReadiness::new(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    Ok(TestServer {
        base_url: format!("http://127.0.0.1:{port}"),
        shutdown: tx,
    })
}

fn span_store_from_records(records: &[SpanRecord]) -> InMemorySpanStore {
    let mut grouped: BTreeMap<(String, [u8; 16]), Vec<Span>> = BTreeMap::new();
    for record in records {
        grouped
            .entry((record.tenant.clone(), record.span.trace_id))
            .or_default()
            .push(record.span.clone());
    }

    let mut store = InMemorySpanStore::new();
    for ((tenant, _), spans) in grouped {
        let root = spans
            .iter()
            .find(|span| span.parent_span_id.is_none())
            .unwrap_or(&spans[0]);
        let root_service = resource_attr(root, "service.name")
            .unwrap_or("unknown")
            .to_string();
        let root_name = root.name.clone();
        store.push_trace(
            &tenant,
            &root_service,
            &root_name,
            spans.into_iter().map(input_span).collect(),
        );
    }
    store
}

fn input_span(span: Span) -> InputSpan {
    let mut attrs = span.resource_attrs;
    attrs.extend(span.span_attrs);
    InputSpan {
        trace_id: span.trace_id,
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name,
        kind: span.kind.as_i32(),
        start_unix_nano: span.start_ns,
        duration: Time::from_nanos(span.duration_ns),
        status_code: span.status.as_i32(),
        status_message: span.status_message,
        instrumentation_name: span.instrumentation_scope,
        instrumentation_version: span.instrumentation_version,
        attrs: attrs
            .into_iter()
            .filter_map(|attr| Some((attr.key, traceql_attr(attr.value)?)))
            .collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

fn traceql_attr(value: AttrValue) -> Option<TraceqlAttrValue> {
    match value {
        AttrValue::Str(value) => Some(TraceqlAttrValue::Str(value)),
        AttrValue::Int(value) => Some(TraceqlAttrValue::Int(value)),
        AttrValue::Double(value) => Some(TraceqlAttrValue::Float(value)),
        AttrValue::Bool(value) => Some(TraceqlAttrValue::Bool(value)),
        AttrValue::Bytes(_) => None,
    }
}

fn resource_attr<'a>(span: &'a Span, key: &str) -> Option<&'a str> {
    span.resource_attrs
        .iter()
        .find_map(|attr| match &attr.value {
            AttrValue::Str(value) if attr.key == key => Some(value.as_str()),
            _ => None,
        })
}

/// Build a one-span OTLP trace under the colliding `trace_id`.
///
/// `extra_attrs` lets the caller attach a tenant-unique span attribute so the
/// two tenants' traces are distinguishable in content while sharing identity.
fn colliding_trace(service: &str, root_name: &str, extra_attrs: &[(&str, &str)]) -> Vec<u8> {
    let mut span_attrs = vec![string_kv("http.method", "POST")];
    for (key, value) in extra_attrs {
        span_attrs.push(string_kv(key, value));
    }
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", service)],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "krabka-isolation".into(),
                    version: "1.0.0".into(),
                    ..InstrumentationScope::default()
                }),
                spans: vec![OtlpSpan {
                    trace_id: COLLIDING_TRACE_ID.to_vec(),
                    span_id: vec![2; 8],
                    name: root_name.into(),
                    start_time_unix_nano: 1_000,
                    end_time_unix_nano: 1_000 + 500_000_000,
                    attributes: span_attrs,
                    ..OtlpSpan::default()
                }],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}

/// An OTLP trace with N spans that share one fresh `trace_id`. It drives ingest
/// volume for the per-tenant quota probe.
fn trace_with_n_spans(trace_seed: u8, n: usize) -> Vec<u8> {
    let spans = (0..n)
        .map(|i| OtlpSpan {
            trace_id: [trace_seed; 16].to_vec(),
            span_id: [i.to_le_bytes()[0].wrapping_add(1); 8].to_vec(),
            name: format!("span-{i}"),
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 1_500,
            ..OtlpSpan::default()
        })
        .collect();
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "loadgen")],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}

fn string_kv(key: &str, value: &str) -> OtlpKeyValue {
    OtlpKeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..OtlpKeyValue::default()
    }
}

async fn get_json(
    client: &reqwest::Client,
    url: &str,
    tenant: &str,
) -> TestResult<(ReqwestStatusCode, JsonValue)> {
    let resp = client.get(url).header(TENANT_HEADER, tenant).send().await?;
    let status = resp.status();
    let body = resp.bytes().await?;
    let json = if body.is_empty() {
        JsonValue::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(JsonValue::Null)
    };
    Ok((status, json))
}

/// Collect the span names contained in a `/api/v2/traces/{id}` response.
fn trace_span_names(trace: &JsonValue) -> Vec<String> {
    trace["trace"]["resourceSpans"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|rs| rs["scopeSpans"].as_array().into_iter().flatten())
        .flat_map(|ss| ss["spans"].as_array().into_iter().flatten())
        .filter_map(|span| span["name"].as_str().map(str::to_string))
        .collect()
}

/// Collect the `rootTraceName`s from a `/api/search` response.
fn root_trace_names(search: &JsonValue) -> Vec<String> {
    search["traces"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|trace| trace["rootTraceName"].as_str().map(str::to_string))
        .collect()
}

/// Collect every tag name across all scopes in a `/api/v2/search/tags` response.
fn tag_names(tags: &JsonValue) -> Vec<String> {
    tags["scopes"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|scope| scope["tags"].as_array().into_iter().flatten())
        .filter_map(|tag| tag.as_str().map(str::to_string))
        .collect()
}

/// Collect the tag values from a v1 `/api/search/tag/{tag}/values` response.
fn tag_values(values: &JsonValue) -> Vec<String> {
    values["tagValues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str().map(str::to_string))
        .collect()
}

#[tokio::test]
async fn tenants_are_fully_isolated_across_all_read_surfaces() -> TestResult {
    // SAME trace_id bytes in both tenants, same service, different root name,
    // plus an attribute unique to tenant-a.
    let a_records = ingest(
        "tenant-a",
        &colliding_trace(
            "checkout",
            "POST /a",
            &[(TENANT_A_ONLY_KEY, TENANT_A_ONLY_VALUE)],
        ),
    )
    .await?;
    let b_records = ingest(
        "tenant-b",
        &colliding_trace("checkout", "POST /b", &[("plain", "x")]),
    )
    .await?;
    let mut records = a_records;
    records.extend(b_records);
    let server = start_querier(records, OverridesProvider::new(Limits::default())).await?;
    let client = reqwest::Client::new();

    // start/end are epoch seconds; the handler multiplies by 1e9, so keep `end`
    // small enough that `end * 1e9` stays within i64. The seeded spans sit at
    // ns=1_000 (≈ epoch 0), so a wide-but-finite window covers them.
    let full_range = "start=0&end=2000000000";

    // 1) trace_by_id: tenant-a sees "POST /a", tenant-b sees "POST /b" — neither
    //    sees the other's span, even though the trace_id bytes are identical.
    let (sa, ta) = get_json(
        &client,
        &format!(
            "{}/api/v2/traces/{COLLIDING_TRACE_ID_HEX}?{full_range}",
            server.base_url
        ),
        "tenant-a",
    )
    .await?;
    let (sb, tb) = get_json(
        &client,
        &format!(
            "{}/api/v2/traces/{COLLIDING_TRACE_ID_HEX}?{full_range}",
            server.base_url
        ),
        "tenant-b",
    )
    .await?;
    check!(sa == ReqwestStatusCode::OK);
    check!(sb == ReqwestStatusCode::OK);
    let a_names = trace_span_names(&ta);
    let b_names = trace_span_names(&tb);
    check!(a_names == vec!["POST /a".to_string()]);
    check!(b_names == vec!["POST /b".to_string()]);
    check!(!a_names.contains(&"POST /b".to_string()));
    check!(!b_names.contains(&"POST /a".to_string()));

    // 2) search: each tenant's result set contains only its own root name.
    let select_all = "%7B%20.http.method%20%3D%20%22POST%22%20%7D";
    let (_, search_a) = get_json(
        &client,
        &format!("{}/api/search?q={select_all}&{full_range}", server.base_url),
        "tenant-a",
    )
    .await?;
    let (_, search_b) = get_json(
        &client,
        &format!("{}/api/search?q={select_all}&{full_range}", server.base_url),
        "tenant-b",
    )
    .await?;
    check!(root_trace_names(&search_a) == vec!["POST /a".to_string()]);
    check!(root_trace_names(&search_b) == vec!["POST /b".to_string()]);

    // 3) /api/v2/search/tags: tenant-a has the `tenant_only` tag, tenant-b does not.
    let (_, tags_a) = get_json(
        &client,
        &format!(
            "{}/api/v2/search/tags?scope=span&{full_range}",
            server.base_url
        ),
        "tenant-a",
    )
    .await?;
    let (_, tags_b) = get_json(
        &client,
        &format!(
            "{}/api/v2/search/tags?scope=span&{full_range}",
            server.base_url
        ),
        "tenant-b",
    )
    .await?;
    check!(tag_names(&tags_a).contains(&TENANT_A_ONLY_KEY.to_string()));
    check!(!tag_names(&tags_b).contains(&TENANT_A_ONLY_KEY.to_string()));

    // 4) tag/{tag}/values: tenant-a sees `tenant_only=A`; tenant-b sees nothing.
    let (_, values_a) = get_json(
        &client,
        &format!(
            "{}/api/search/tag/{TENANT_A_ONLY_KEY}/values?{full_range}",
            server.base_url
        ),
        "tenant-a",
    )
    .await?;
    let (_, values_b) = get_json(
        &client,
        &format!(
            "{}/api/search/tag/{TENANT_A_ONLY_KEY}/values?{full_range}",
            server.base_url
        ),
        "tenant-b",
    )
    .await?;
    check!(tag_values(&values_a).contains(&TENANT_A_ONLY_VALUE.to_string()));
    check!(tag_values(&values_b).is_empty());

    // 5) TraceQL select on the tenant-a-only attribute returns nothing for tenant-b.
    let select_a_only = "%7B%20.tenant_only%20%3D%20%22A%22%20%7D";
    let (_, q_a) = get_json(
        &client,
        &format!(
            "{}/api/search?q={select_a_only}&{full_range}",
            server.base_url
        ),
        "tenant-a",
    )
    .await?;
    let (_, q_b) = get_json(
        &client,
        &format!(
            "{}/api/search?q={select_a_only}&{full_range}",
            server.base_url
        ),
        "tenant-b",
    )
    .await?;
    check!(root_trace_names(&q_a) == vec!["POST /a".to_string()]);
    assert2::assert!(root_trace_names(&q_b).is_empty());

    server.shutdown();
    Ok(())
}

/// One provider, both doors.
///
/// The traces service builds exactly one `OverridesProvider`, and the ingest
/// gate in the distributor and the read gate in the querier both resolve a
/// tenant through it. Two parallel limit types stood here before, and the
/// projection between them pinned `max_traces_per_search` to the compiled
/// default, so no file and no flag could move a read limit however clearly it
/// named one. This test fails if that projection comes back: the same document
/// has to change what both doors do.
#[tokio::test]
async fn one_overrides_provider_drives_the_ingest_and_the_read_gate() -> TestResult {
    let overrides = OverridesProvider::from_yaml(
        r"
overrides:
  tenant-tight:
    max_spans_per_request: 1
    max_traces_per_search: 1
",
    )?;

    // The ingest door: a two-span push is over tenant-tight's request cap and
    // within every other tenant's.
    let sink = CapturingSink::default();
    let mut state = DistributorState::new(Arc::new(sink.clone()));
    state.overrides = overrides.clone();
    let door = authenticated(distributor::router(Arc::new(state)));
    let push = |tenant: &'static str, seed: u8| {
        let door = door.clone();
        async move {
            door.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/x-protobuf")
                    .header(TENANT_HEADER, tenant)
                    .body(Body::from(trace_with_n_spans(seed, 2)))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
        }
    };

    check!(push("tenant-tight", 1).await == StatusCode::BAD_REQUEST);
    check!(push("tenant-loose", 2).await == StatusCode::OK);

    // The read door, from the same provider: a `limit=2` search is over
    // tenant-tight's search cap and within every other tenant's.
    let records = sink
        .records
        .lock()
        .map_err(|_| "capturing sink lock poisoned")?
        .clone();
    let server = start_querier(records, overrides).await?;
    let client = reqwest::Client::new();
    let search = |tenant: &'static str| {
        let url = format!(
            "{}/api/search?q=%7B%7D&start=0&end=1&limit=2",
            server.base_url
        );
        let client = client.clone();
        async move { get_json(&client, &url, tenant).await }
    };

    let (tight, body) = search("tenant-tight").await?;
    check!(tight == ReqwestStatusCode::BAD_REQUEST);
    check!(
        body["error"]
            .as_str()
            .is_some_and(|message| message.contains("max traces per search"))
    );
    let (loose, _) = search("tenant-loose").await?;
    check!(loose == ReqwestStatusCode::OK);

    server.shutdown();
    Ok(())
}

#[tokio::test]
async fn per_tenant_quota_throttles_only_the_capped_tenant() -> TestResult {
    // tenant-a has a tiny ingest-rate cap; tenant-b is unbounded. Pushing the
    // same burst to both proves quota buckets are keyed per tenant: the capped
    // tenant 429s while the other tenant stays 200 at the same instant.
    let overrides = OverridesProvider::from_yaml(
        r"
overrides:
  tenant-a:
    ingestion_rate_spans_per_sec: 1
    ingestion_burst_spans: 1
",
    )?;

    // Ingest enforcement lives in the distributor door. Build a distributor with
    // the same overrides and drive both tenants through it.
    let sink = CapturingSink::default();
    let mut state = DistributorState::new(Arc::new(sink.clone()));
    state.overrides = overrides;
    let router = authenticated(distributor::router(Arc::new(state)));
    let client = reqwest::Client::new();

    // Bind a real socket for the distributor so X-Scope-OrgID flows through HTTP.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    let base_url = format!("http://127.0.0.1:{port}");

    // tenant-a: first single-span push consumes the burst; second is over-rate.
    let a_first = client
        .post(format!("{base_url}/v1/traces"))
        .header("content-type", "application/x-protobuf")
        .header(TENANT_HEADER, "tenant-a")
        .body(trace_with_n_spans(1, 1))
        .send()
        .await?
        .status();
    let a_second = client
        .post(format!("{base_url}/v1/traces"))
        .header("content-type", "application/x-protobuf")
        .header(TENANT_HEADER, "tenant-a")
        .body(trace_with_n_spans(2, 1))
        .send()
        .await?
        .status();
    check!(a_first == ReqwestStatusCode::OK);
    check!(a_second == ReqwestStatusCode::TOO_MANY_REQUESTS);

    // tenant-b is unaffected at the same instant: a multi-span burst succeeds.
    let b_status = client
        .post(format!("{base_url}/v1/traces"))
        .header("content-type", "application/x-protobuf")
        .header(TENANT_HEADER, "tenant-b")
        .body(trace_with_n_spans(3, 50))
        .send()
        .await?
        .status();
    check!(b_status == ReqwestStatusCode::OK);

    let _ = tx.send(());
    Ok(())
}

/// How every boundary answers one tenant request shape.
#[derive(Clone, Copy, Debug)]
enum Answer {
    /// The request runs as this tenant.
    Served(&'static str),
    /// The request gets a 400 or `InvalidArgument`, and nothing is stored or
    /// queried under any tenant.
    Rejected,
}

/// Each `X-Scope-OrgID` shape a client can send, with the one answer that
/// every ingest and query boundary gives it.
///
/// The three malformed rows are the regression guard for a resolver that
/// reads a value it cannot use as absent: a non-UTF-8 value once became the
/// anonymous tenant, and `a/b` and `..` once passed through as tenants.
const TENANT_CASES: [(&str, Option<&[u8]>, Answer); 6] = [
    ("absent", None, Answer::Served("anonymous")),
    ("empty", Some(b""), Answer::Served("anonymous")),
    ("named", Some(b"tenant-a"), Answer::Served("tenant-a")),
    ("separator", Some(b"a/b"), Answer::Rejected),
    ("parent segment", Some(b".."), Answer::Rejected),
    ("not UTF-8", Some(b"\xff"), Answer::Rejected),
];

/// The body a boundary sends when it rejects `value`: the resolver's message,
/// which `krabka-blockstore` matches to the Mimir and Loki text.
fn rejection_message(value: Option<&[u8]>) -> String {
    TenantId::resolve(value, &TenantPolicy::anonymous())
        .expect_err("the case is malformed")
        .to_string()
}

/// A GET `uri` with the tenant header set to `value`, or with no header.
fn get_request(uri: &str, value: Option<&[u8]>) -> TestResult<Request<Body>> {
    let mut request = Request::builder().method("GET").uri(uri);
    if let Some(value) = value {
        request = request.header(TENANT_HEADER, HeaderValue::from_bytes(value)?);
    }
    Ok(request.body(Body::empty())?)
}

fn otlp_request(value: Option<&[u8]>) -> TestResult<GrpcRequest<ExportTraceServiceRequest>> {
    let mut request = GrpcRequest::new(ExportTraceServiceRequest {
        resource_spans: TracesData::decode(trace_with_n_spans(1, 1).as_slice())?.resource_spans,
    });
    *request.metadata_mut() = tenant_metadata(value)?;
    request
        .extensions_mut()
        .insert(krabka_observability::server_security::Principal::Unauthenticated);
    Ok(request)
}

fn jaeger_request(value: Option<&[u8]>) -> TestResult<GrpcRequest<PostSpansRequest>> {
    let mut request = GrpcRequest::new(PostSpansRequest {
        batch: Some(JaegerBatch {
            process: Some(JaegerProcess {
                service_name: "checkout".into(),
                tags: Vec::new(),
            }),
            spans: vec![JaegerSpan {
                trace_id: vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
                span_id: vec![0, 0, 0, 0, 0, 0, 0, 3],
                operation_name: "GET /grpc".into(),
                start_time: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                duration: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 25_000,
                }),
                ..JaegerSpan::default()
            }],
        }),
    });
    *request.metadata_mut() = tenant_metadata(value)?;
    request
        .extensions_mut()
        .insert(krabka_observability::server_security::Principal::Unauthenticated);
    Ok(request)
}

fn tenant_metadata(value: Option<&[u8]>) -> TestResult<MetadataMap> {
    let mut metadata = MetadataMap::new();
    if let Some(value) = value {
        metadata.insert(TENANT_HEADER, AsciiMetadataValue::try_from(value)?);
    }
    Ok(metadata)
}

/// Each HTTP ingest door resolves its tenant in its own handler, so each one
/// runs every row. A door that fell back to the anonymous tenant on a value it
/// could not read would store a span in the malformed rows; a door that used
/// the raw value would store it under `a/b`.
#[tokio::test]
async fn every_distributor_http_door_resolves_a_tenant_the_same_way() -> TestResult {
    let zipkin = br#"[{"traceId":"0000000000000001","id":"0000000000000002","name":"x","timestamp":1,"duration":1}]"#;
    let doors = [
        (
            "/v1/traces",
            "application/x-protobuf",
            trace_with_n_spans(1, 1),
            StatusCode::OK,
        ),
        (
            "/api/push",
            "application/x-protobuf",
            trace_with_n_spans(1, 1),
            StatusCode::OK,
        ),
        (
            "/api/v2/spans",
            "application/json",
            zipkin.to_vec(),
            StatusCode::ACCEPTED,
        ),
    ];

    for (uri, content_type, body, success) in doors {
        for (case, value, answer) in TENANT_CASES {
            let sink = CapturingSink::default();
            let door = authenticated(distributor::router(Arc::new(DistributorState::new(
                Arc::new(sink.clone()),
            ))));
            let mut request = Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", content_type);
            if let Some(value) = value {
                request = request.header(TENANT_HEADER, HeaderValue::from_bytes(value)?);
            }
            let resp = door
                .oneshot(request.body(Body::from(body.clone()))?)
                .await?;
            let status = resp.status();
            let text = resp.into_body().collect().await?.to_bytes();

            match answer {
                Answer::Served(tenant) => {
                    check!(status == success, "{uri} {case}");
                    check!(sink.tenants() == vec![tenant.to_string()], "{uri} {case}");
                }
                Answer::Rejected => {
                    check!(status == StatusCode::BAD_REQUEST, "{uri} {case}");
                    check!(
                        text.as_ref() == rejection_message(value).as_bytes(),
                        "{uri} {case}"
                    );
                    check!(sink.tenants().is_empty(), "{uri} {case}");
                }
            }
        }
    }
    Ok(())
}

/// The OTLP and Jaeger gRPC doors read the same key from gRPC metadata, and
/// answer every row as the HTTP doors do, with `InvalidArgument` in place of
/// a 400.
#[tokio::test]
async fn both_distributor_grpc_doors_resolve_a_tenant_the_same_way() -> TestResult {
    for (case, value, answer) in TENANT_CASES {
        let otlp_sink = CapturingSink::default();
        let otlp =
            OtlpGrpcService::new(Arc::new(DistributorState::new(Arc::new(otlp_sink.clone()))))
                .export(otlp_request(value)?)
                .await
                .map(|_| ());
        let jaeger_sink = CapturingSink::default();
        let jaeger = JaegerGrpcService::new(Arc::new(DistributorState::new(Arc::new(
            jaeger_sink.clone(),
        ))))
        .post_spans(jaeger_request(value)?)
        .await
        .map(|_| ());

        for (door, result, sink) in [("otlp", otlp, otlp_sink), ("jaeger", jaeger, jaeger_sink)] {
            match answer {
                Answer::Served(tenant) => {
                    check!(result.is_ok(), "{door} {case}");
                    check!(sink.tenants() == vec![tenant.to_string()], "{door} {case}");
                }
                Answer::Rejected => {
                    let status = result.expect_err("a malformed tenant is refused");
                    check!(status.code() == GrpcCode::InvalidArgument, "{door} {case}");
                    check!(
                        status.message() == rejection_message(value),
                        "{door} {case}"
                    );
                    check!(sink.tenants().is_empty(), "{door} {case}");
                }
            }
        }
    }
    Ok(())
}

/// Span records for the colliding trace under `tenant`, with a root span
/// named after the tenant, so a response shows whose data it holds.
fn tenant_named_records(tenant: &str) -> TestResult<Vec<SpanRecord>> {
    let data = TracesData::decode(
        colliding_trace("checkout", &format!("root of {tenant}"), &[]).as_slice(),
    )?;
    Ok(decode_otlp(&data)?
        .into_iter()
        .map(|span| SpanRecord {
            tenant: tenant.to_string(),
            span,
        })
        .collect())
}

/// Every querier route resolves the tenant in its own handler. Each route
/// runs every row: a served row reaches the store as the named tenant, and a
/// rejected row stops at a 400 before any read.
///
/// The anonymous and `tenant-a` data share one trace id and differ only in
/// the root span name, so the search and by-id bodies show which tenant a
/// request read.
#[tokio::test]
async fn every_querier_route_resolves_a_tenant_the_same_way() -> TestResult {
    let mut records = tenant_named_records("anonymous")?;
    records.extend(tenant_named_records("tenant-a")?);
    let store = Arc::new(TraceqlEngine::new(
        Arc::new(span_store_from_records(&records)),
        EngineOpts::default(),
    ));
    let querier = authenticated(krabka_traces::querier::http::router_with_config(
        store,
        HttpConfig::default(),
        RoleReadiness::new(),
    ));
    // One hour around the seeded spans. A wider window gives the metrics
    // routes one step per minute of it to evaluate.
    let window = "start=0&end=3600";
    let routes = [
        format!("/api/search?q=%7B%7D&{window}"),
        format!("/api/search/tags?{window}"),
        format!("/api/v2/search/tags?{window}"),
        format!("/api/search/tag/http.method/values?{window}"),
        format!("/api/v2/search/tag/span.http.method/values?{window}"),
        format!("/api/metrics/query_range?q=%7B%7D%20%7C%20rate()&{window}&step=60"),
        format!("/api/metrics/query?q=%7B%7D%20%7C%20rate()&{window}"),
        format!("/api/v2/traces/{COLLIDING_TRACE_ID_HEX}?{window}"),
        format!("/api/traces/{COLLIDING_TRACE_ID_HEX}?{window}"),
    ];

    for route in &routes {
        for (case, value, answer) in TENANT_CASES {
            let resp = querier.clone().oneshot(get_request(route, value)?).await?;
            let status = resp.status();
            let body = resp.into_body().collect().await?.to_bytes();
            match answer {
                Answer::Served(_) => {
                    check!(status == StatusCode::OK, "{route} {case}");
                }
                Answer::Rejected => {
                    check!(status == StatusCode::BAD_REQUEST, "{route} {case}");
                    check!(
                        body.as_ref() == rejection_message(value).as_bytes(),
                        "{route} {case}"
                    );
                }
            }
        }
    }

    for (case, value, answer) in TENANT_CASES {
        let Answer::Served(tenant) = answer else {
            continue;
        };
        let search = querier
            .clone()
            .oneshot(get_request(&routes[0], value)?)
            .await?;
        let search: JsonValue =
            serde_json::from_slice(&search.into_body().collect().await?.to_bytes())?;
        check!(
            root_trace_names(&search) == vec![format!("root of {tenant}")],
            "{case}"
        );
        let by_id = querier
            .clone()
            .oneshot(get_request(&routes[7], value)?)
            .await?;
        let by_id: JsonValue =
            serde_json::from_slice(&by_id.into_body().collect().await?.to_bytes())?;
        check!(
            trace_span_names(&by_id) == vec![format!("root of {tenant}")],
            "{case}"
        );
    }
    Ok(())
}

/// The tenant of every job the mock querier took, across every job kind.
fn recorded_tenants(backend: &MockQuerier) -> Vec<String> {
    let search = backend.search_calls().into_iter().map(|call| call.tenant);
    let by_id = backend.trace_calls().into_iter().map(|call| call.tenant);
    let tags = backend
        .tag_names_calls()
        .into_iter()
        .map(|call| call.tenant);
    let values = backend
        .tag_values_calls()
        .into_iter()
        .map(|call| call.tenant);
    let metrics = backend.metrics_calls().into_iter().map(|call| call.tenant);
    search
        .chain(by_id)
        .chain(tags)
        .chain(values)
        .chain(metrics)
        .map(TenantId::into_string)
        .collect()
}

/// Every frontend route resolves the tenant before it plans a job. A served
/// row fans out jobs that carry the resolved tenant, and a rejected row is a
/// 400 that fans out nothing.
#[tokio::test]
async fn every_frontend_route_resolves_a_tenant_the_same_way() -> TestResult {
    let routes = [
        "/api/search?q=%7B%7D&start=0&end=100",
        "/api/v2/traces/abababababababababababababababab?start=0&end=100",
        "/api/v2/search/tags?start=0&end=100",
        "/api/v2/search/tag/span.name/values?start=0&end=100",
        "/api/metrics/query_range?q=%7B%7D%20%7C%20rate()&start=0&end=100&step=10",
        "/api/metrics/query?q=%7B%7D%20%7C%20rate()&start=0&end=100",
    ];

    for route in routes {
        for (case, value, answer) in TENANT_CASES {
            let frontend = Arc::new(QueryFrontend::new(
                Arc::new(MockQuerier::new()),
                Arc::new(MockCatalog::new(Vec::new())),
                FrontendConfig::default(),
                MembershipView::fixed(["q1:3200"]),
            ));
            let resp = authenticated(router_with_backend(
                Arc::clone(&frontend),
                RoleReadiness::new(),
            ))
            .oneshot(get_request(route, value)?)
            .await?;
            let status = resp.status();
            let body = resp.into_body().collect().await?.to_bytes();
            let tenants = recorded_tenants(frontend.backend_ref());

            match answer {
                Answer::Served(tenant) => {
                    check!(status != StatusCode::BAD_REQUEST, "{route} {case}");
                    check!(!tenants.is_empty(), "{route} {case}: a job fanned out");
                    check!(tenants.iter().all(|seen| seen == tenant), "{route} {case}");
                }
                Answer::Rejected => {
                    check!(status == StatusCode::BAD_REQUEST, "{route} {case}");
                    check!(
                        body.as_ref() == rejection_message(value).as_bytes(),
                        "{route} {case}"
                    );
                    check!(tenants.is_empty(), "{route} {case}");
                }
            }
        }
    }
    Ok(())
}

/// End to end through the real transport: the frontend resolves the tenant,
/// and `HttpQuerier` writes it under `TENANT_HEADER` for a real querier to
/// resolve again. Only a forwarded header lets `tenant-a` read its own root;
/// an empty client header reaches the querier as the explicit anonymous
/// tenant.
#[tokio::test]
async fn the_frontend_forwards_the_resolved_tenant_to_the_querier() -> TestResult {
    let mut records = tenant_named_records("anonymous")?;
    records.extend(tenant_named_records("tenant-a")?);
    let querier = start_querier(records, OverridesProvider::new(Limits::default())).await?;
    let querier_addr = querier
        .base_url
        .strip_prefix("http://")
        .ok_or("the querier url has a scheme")?
        .to_string();
    let frontend = Arc::new(QueryFrontend::new(
        Arc::new(HttpQuerier::new(
            std::time::Duration::from_secs(5),
            krabka_traces::frontend::QuerierScheme::Http,
            &krabka_observability::server_security::InternalClient::default(),
        )?),
        Arc::new(MockCatalog::new(Vec::new())),
        FrontendConfig::default(),
        MembershipView::fixed([querier_addr]),
    ));
    let router = authenticated(router_with_backend(frontend, RoleReadiness::new()));

    for (case, value, answer) in TENANT_CASES {
        let Answer::Served(tenant) = answer else {
            continue;
        };
        let resp = router
            .clone()
            .oneshot(get_request(
                "/api/search?q=%7B%7D&start=0&end=2000000000",
                value,
            )?)
            .await?;
        check!(resp.status() == StatusCode::OK, "{case}");
        let body: JsonValue =
            serde_json::from_slice(&resp.into_body().collect().await?.to_bytes())?;
        check!(
            root_trace_names(&body) == vec![format!("root of {tenant}")],
            "{case}"
        );
    }

    querier.shutdown();
    Ok(())
}

// The routers read the principal from the request extensions, where the
// authentication layer puts it. This is that layer with no security flags,
// which serves every request as unauthenticated.
fn authenticated(router: axum::Router) -> axum::Router {
    authenticate_requests(router, &ServerSecurity::default())
}

// ---------------------------------------------------------------------------
// The listeners with TLS and a credentials file.
//
// Each test generates its certificates with `rcgen`, so no key material is in
// the repository. The credentials file has three principals: `grafana` holds
// tenant-a, `stranger` holds tenant-b, and `internal` holds every tenant.
// ---------------------------------------------------------------------------

const GRAFANA_TOKEN: &str = "grafana-6f1c2a9e4b7d0e3f5a8c1b4d7e0f3a6c";
const STRANGER_TOKEN: &str = "stranger-3a5c7e9b1d2f4a6c8e0b2d4f6a8c0e2b";
const INTERNAL_TOKEN: &str = "internal-9d8c7b6a5f4e3d2c1b0a9f8e7d6c5b4a";

#[derive(Parser)]
struct SecurityFlags {
    #[command(flatten)]
    security: ServerSecurityArgs,
}

fn load_security(flags: &[String]) -> TestResult<ServerSecurity> {
    install_crypto_provider();
    let argv = iter::once("krabka-traces".to_owned()).chain(flags.iter().cloned());
    Ok(SecurityFlags::try_parse_from(argv)?.security.load()?)
}

fn sha256_hex(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

// A CA, and the directory that holds the files the flags name.
struct Pki {
    dir: TempDir,
    authority: CertifiedIssuer<'static, KeyPair>,
}

impl Pki {
    fn new() -> TestResult<Self> {
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(DnType::CommonName, "krabka traces test ca");
        let authority = CertifiedIssuer::self_signed(params, KeyPair::generate()?)?;
        Ok(Self {
            dir: TempDir::new()?,
            authority,
        })
    }

    fn write(&self, name: &str, contents: impl AsRef<[u8]>) -> TestResult<String> {
        let path = self.dir.path().join(name);
        std::fs::write(&path, contents)?;
        Ok(path.display().to_string())
    }

    // TLS with a server certificate for `localhost` and `127.0.0.1`, and
    // authentication with the three test principals.
    fn listener_flags(&self) -> TestResult<Vec<String>> {
        let mut params =
            CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])?;
        params.distinguished_name.push(DnType::CommonName, "traces");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate()?;
        let certificate = params.signed_by(&key, &self.authority)?;
        let mut flags = vec![
            "--server-tls-cert-path".to_owned(),
            self.write("server.pem", certificate.pem())?,
            "--server-tls-key-path".to_owned(),
            self.write("server-key.pem", key.serialize_pem())?,
        ];
        flags.extend(self.credentials_flags()?);
        Ok(flags)
    }

    fn credentials_flags(&self) -> TestResult<Vec<String>> {
        let credentials = format!(
            "principals:\n\
             - name: grafana\n  token_sha256: [\"{}\"]\n  tenants: [\"tenant-a\"]\n\
             - name: stranger\n  token_sha256: [\"{}\"]\n  tenants: [\"tenant-b\"]\n\
             - name: internal\n  token_sha256: [\"{}\"]\n  tenants: [\"*\"]\n",
            sha256_hex(GRAFANA_TOKEN),
            sha256_hex(STRANGER_TOKEN),
            sha256_hex(INTERNAL_TOKEN),
        );
        Ok(vec![
            "--auth-credentials-config".to_owned(),
            self.write("credentials.yaml", credentials)?,
        ])
    }

    // The internal client of a caller that trusts this CA, and that presents
    // `token` when there is one.
    fn internal_client_flags(&self, token: Option<&str>) -> TestResult<Vec<String>> {
        let mut flags = vec![
            "--internal-client-tls-ca-path".to_owned(),
            self.write("internal-ca.pem", self.authority.pem())?,
        ];
        if let Some(token) = token {
            flags.push("--internal-client-token-path".to_owned());
            flags.push(self.write("internal-token", format!("{token}\n"))?);
        }
        Ok(flags)
    }

    fn https_client(&self) -> TestResult<reqwest::Client> {
        let authority = reqwest::Certificate::from_pem(self.authority.pem().as_bytes())?;
        Ok(reqwest::Client::builder()
            .tls_certs_only([authority])
            .build()?)
    }

    fn grpc_endpoint(&self, addr: SocketAddr) -> TestResult<tonic::transport::Endpoint> {
        let tls = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(
                self.authority.pem(),
            ))
            .domain_name("localhost");
        Ok(tonic::transport::Endpoint::from_shared(format!("https://{addr}"))?.tls_config(tls)?)
    }
}

// Every security decision a listener reports, as text.
#[derive(Default)]
struct RecordedEvents(Mutex<Vec<String>>);

impl RecordedEvents {
    fn take(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .expect("no test panics while holding the lock"),
        )
    }

    fn push(&self, event: String) {
        self.0
            .lock()
            .expect("no test panics while holding the lock")
            .push(event);
    }
}

impl SecurityEvents for RecordedEvents {
    fn authentication_failed(
        &self,
        _source: Option<SocketAddr>,
        _attempted: Option<AuthMethod>,
        reason: AuthFailureReason,
    ) {
        self.push(format!("authentication failed: {reason:?}"));
    }

    fn authentication_succeeded(
        &self,
        _source: Option<SocketAddr>,
        _principal: &str,
        _method: AuthMethod,
    ) {
    }

    fn tenant_denied(&self, principal: &str, _method: AuthMethod, tenant: &TenantId) {
        self.push(format!("tenant denied: {principal} {tenant}"));
    }

    fn admin_denied(&self, principal: &str, _method: AuthMethod) {
        self.push(format!("admin denied: {principal}"));
    }
}

async fn serve_secured(
    router: axum::Router,
    security: &ServerSecurity,
    stop: &CancellationToken,
) -> TestResult<SocketAddr> {
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let listener = ServerListener::bind(tcp, security)?;
    let addr = listener.local_addr();
    tokio::spawn(
        serve_router(listener, router, security)
            .with_graceful_shutdown(stop.clone().cancelled_owned())
            .into_future(),
    );
    Ok(addr)
}

fn secured_querier(tenant: &str) -> TestResult<axum::Router> {
    let engine = TraceqlEngine::new(
        Arc::new(span_store_from_records(&tenant_named_records(tenant)?)),
        EngineOpts::default(),
    );
    Ok(krabka_traces::querier::http::router_with_config(
        Arc::new(engine),
        HttpConfig::default(),
        RoleReadiness::new(),
    ))
}

fn with_bearer<T>(mut request: GrpcRequest<T>, token: Option<&str>) -> TestResult<GrpcRequest<T>> {
    if let Some(token) = token {
        request.metadata_mut().insert(
            "authorization",
            AsciiMetadataValue::try_from(format!("Bearer {token}"))?,
        );
    }
    Ok(request)
}

// An OTLP/HTTP push to a TLS listener with a credentials file. A missing or
// unknown credential is a 401 and a principal without the tenant is a 403,
// and neither reaches the WAL. The security events, which feed the audit
// trail, name both failures and the denial.
#[tokio::test]
async fn a_secured_otlp_http_push_reaches_the_wal_only_for_a_granted_principal() -> TestResult {
    let pki = Pki::new()?;
    let events = Arc::new(RecordedEvents::default());
    let security = load_security(&pki.listener_flags()?)?.with_security_events(events.clone());
    let sink = CapturingSink::default();
    let stop = CancellationToken::new();
    let (addr, _server) = distributor::serve(
        "127.0.0.1:0".parse()?,
        Arc::new(DistributorState::new(Arc::new(sink.clone()))),
        &security,
        stop.clone(),
    )
    .await?;
    let client = pki.https_client()?;
    let url = format!("https://{addr}/v1/traces");

    for (case, token, status, stored) in [
        ("no credential", None, ReqwestStatusCode::UNAUTHORIZED, 0),
        (
            "an unknown token",
            Some("not-a-configured-token"),
            ReqwestStatusCode::UNAUTHORIZED,
            0,
        ),
        (
            "a principal without tenant-a",
            Some(STRANGER_TOKEN),
            ReqwestStatusCode::FORBIDDEN,
            0,
        ),
        (
            "a principal granted tenant-a",
            Some(GRAFANA_TOKEN),
            ReqwestStatusCode::OK,
            1,
        ),
    ] {
        let mut request = client
            .post(&url)
            .header("content-type", "application/x-protobuf")
            .header(TENANT_HEADER, "tenant-a")
            .body(trace_with_n_spans(7, 1));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        check!(request.send().await?.status() == status, "{case}");
        check!(
            sink.tenants() == vec!["tenant-a".to_string(); stored],
            "{case}"
        );
    }
    check!(
        events.take()
            == vec![
                "authentication failed: MissingCredential".to_string(),
                "authentication failed: UnknownCredential".to_string(),
                "tenant denied: stranger tenant-a".to_string(),
            ]
    );
    stop.cancel();
    Ok(())
}

// A querier on a TLS listener with a credentials file answers `/api/search`
// only for a principal that holds the tenant, and answers `/ready` with no
// credential, as a probe sends it.
#[tokio::test]
async fn a_secured_querier_answers_search_only_for_a_granted_principal() -> TestResult {
    let pki = Pki::new()?;
    let security = load_security(&pki.listener_flags()?)?;
    let stop = CancellationToken::new();
    let addr = serve_secured(secured_querier("tenant-a")?, &security, &stop).await?;
    let client = pki.https_client()?;
    let search = format!("https://{addr}/api/search?q=%7B%7D&start=0&end=2000000000");

    for (case, token, status, body) in [
        (
            "no credential",
            None,
            ReqwestStatusCode::UNAUTHORIZED,
            "unauthorized\n",
        ),
        (
            "a principal without tenant-a",
            Some(STRANGER_TOKEN),
            ReqwestStatusCode::FORBIDDEN,
            "principal \"stranger\" is not allowed to access tenant \"tenant-a\"\n",
        ),
    ] {
        let mut request = client.get(&search).header(TENANT_HEADER, "tenant-a");
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await?;
        check!(response.status() == status, "{case}");
        check!(response.text().await? == body, "{case}");
    }

    let granted = client
        .get(&search)
        .header(TENANT_HEADER, "tenant-a")
        .bearer_auth(GRAFANA_TOKEN)
        .send()
        .await?;
    check!(granted.status() == ReqwestStatusCode::OK);
    let granted: JsonValue = granted.json().await?;
    check!(root_trace_names(&granted) == vec!["root of tenant-a".to_string()]);

    let ready = client.get(format!("https://{addr}/ready")).send().await?;
    check!(ready.status() == ReqwestStatusCode::OK);
    stop.cancel();
    Ok(())
}

// Both gRPC doors serve TLS through the shared listener, and the
// authentication layer runs before the service. OTLP answers every case;
// Jaeger answers the missing credential and the granted principal.
#[tokio::test]
async fn secured_grpc_doors_authenticate_and_authorize_before_the_wal() -> TestResult {
    let pki = Pki::new()?;
    let security = load_security(&pki.listener_flags()?)?;
    let sink = CapturingSink::default();
    let state = Arc::new(DistributorState::new(Arc::new(sink.clone())));
    let stop = CancellationToken::new();
    let (otlp_addr, _otlp) = distributor::serve_otlp_grpc(
        "127.0.0.1:0".parse()?,
        Arc::clone(&state),
        &security,
        stop.clone(),
    )
    .await?;
    let (jaeger_addr, _jaeger) =
        distributor::serve_jaeger_grpc("127.0.0.1:0".parse()?, state, &security, stop.clone())
            .await?;
    let mut otlp = TraceServiceClient::new(pki.grpc_endpoint(otlp_addr)?.connect().await?);
    let mut jaeger = CollectorServiceClient::new(pki.grpc_endpoint(jaeger_addr)?.connect().await?);

    for (case, token, code, stored) in [
        ("no credential", None, Some(GrpcCode::Unauthenticated), 0),
        (
            "a principal without tenant-a",
            Some(STRANGER_TOKEN),
            Some(GrpcCode::PermissionDenied),
            0,
        ),
        ("a principal granted tenant-a", Some(GRAFANA_TOKEN), None, 1),
    ] {
        let request = with_bearer(otlp_request(Some(b"tenant-a"))?, token)?;
        let outcome = otlp.export(request).await.map_err(|status| status.code());
        check!(outcome.err() == code, "otlp {case}");
        check!(
            sink.tenants() == vec!["tenant-a".to_string(); stored],
            "otlp {case}"
        );
    }

    for (case, token, code, stored) in [
        ("no credential", None, Some(GrpcCode::Unauthenticated), 1),
        ("a principal granted tenant-a", Some(GRAFANA_TOKEN), None, 2),
    ] {
        let request = with_bearer(jaeger_request(Some(b"tenant-a"))?, token)?;
        let outcome = jaeger
            .post_spans(request)
            .await
            .map_err(|status| status.code());
        check!(outcome.err() == code, "jaeger {case}");
        check!(
            sink.tenants() == vec!["tenant-a".to_string(); stored],
            "jaeger {case}"
        );
    }
    stop.cancel();
    Ok(())
}

// The frontend authenticates the end user, and calls a querier on a TLS
// listener with authentication on as the internal principal. With the
// internal credential the search returns the tenant's trace. With the
// credential removed the querier refuses the fan-out, and the frontend passes
// the 401 on. A user without the tenant stops at the frontend with a 403.
#[tokio::test]
async fn the_frontend_reaches_a_secured_querier_with_the_internal_credential() -> TestResult {
    let pki = Pki::new()?;
    let security = load_security(&pki.listener_flags()?)?;
    let stop = CancellationToken::new();
    let querier_addr = serve_secured(secured_querier("tenant-a")?, &security, &stop).await?;
    let frontend = |caller: &ServerSecurity| -> TestResult<axum::Router> {
        let frontend = Arc::new(QueryFrontend::new(
            Arc::new(HttpQuerier::new(
                Duration::from_secs(5),
                QuerierScheme::Https,
                caller.internal_client(),
            )?),
            Arc::new(MockCatalog::new(Vec::new())),
            FrontendConfig::default(),
            MembershipView::fixed([querier_addr.to_string()]),
        ));
        Ok(authenticate_requests(
            router_with_backend(frontend, RoleReadiness::new()),
            &security,
        ))
    };
    let with_credential = frontend(&load_security(
        &pki.internal_client_flags(Some(INTERNAL_TOKEN))?,
    )?)?;
    let without_credential = frontend(&load_security(&pki.internal_client_flags(None)?)?)?;
    let search = |token: &str| -> TestResult<Request<Body>> {
        Ok(Request::builder()
            .method("GET")
            .uri("/api/search?q=%7B%7D&start=0&end=2000000000")
            .header(TENANT_HEADER, "tenant-a")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())?)
    };

    let served = with_credential
        .clone()
        .oneshot(search(GRAFANA_TOKEN)?)
        .await?;
    check!(served.status() == StatusCode::OK);
    let body: JsonValue = serde_json::from_slice(&served.into_body().collect().await?.to_bytes())?;
    check!(root_trace_names(&body) == vec!["root of tenant-a".to_string()]);

    let refused = without_credential.oneshot(search(GRAFANA_TOKEN)?).await?;
    check!(refused.status() == StatusCode::UNAUTHORIZED);

    let denied = with_credential.oneshot(search(STRANGER_TOKEN)?).await?;
    check!(denied.status() == StatusCode::FORBIDDEN);
    stop.cancel();
    Ok(())
}

// A datagram cannot carry a credential, so with authentication on the Jaeger
// compact receiver does not bind its port. The held socket proves it: a
// receiver that tried to bind the same address would fail with `AddrInUse`.
// With no credentials file the receiver starts, as upstream's does.
#[tokio::test]
async fn the_jaeger_compact_receiver_starts_only_without_authentication() -> TestResult {
    let pki = Pki::new()?;
    let held = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    let state = Arc::new(DistributorState::new(Arc::new(CapturingSink::default())));
    let stop = CancellationToken::new();

    let with_authentication = distributor::serve_jaeger_compact_udp(
        held.local_addr()?,
        Arc::clone(&state),
        &load_security(&pki.credentials_flags()?)?,
        stop.clone(),
    )
    .await?;
    let without_authentication = distributor::serve_jaeger_compact_udp(
        "127.0.0.1:0".parse()?,
        state,
        &ServerSecurity::default(),
        stop.clone(),
    )
    .await?;

    check!(with_authentication.is_none());
    check!(without_authentication.is_some());
    stop.cancel();
    Ok(())
}
