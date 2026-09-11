//! What a client sees when a querier is not there.
//!
//! The frontend fans a search over its queriers. Two things about that pool
//! are not the same problem, and the tests here pin both.
//!
//! A **cold block** is in object storage, and every querier can read it. Losing
//! a querier costs nothing: the block is re-assigned and the answer is whole.
//!
//! The **live tier** is not. A querier run with `--querier-live-store` consumes
//! the traces WAL in the group `krabka-traces-querier-live-store`, so N querier
//! replicas hold N disjoint slices of the recent spans and no other querier can
//! supply the slice of one that is gone. An answer that quietly leaves that
//! slice out is a fraction of the data returned with a 200, which is the
//! failure this milestone is named after.
//!
//! Every assertion here is on the JSON body the HTTP client receives, through
//! the real `HttpQuerier` against real loopback queriers, with membership built
//! by the same `refresh_membership` the role binary runs.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use assert2::check;
use axum::{
    Router,
    extract::State,
    http::{StatusCode, Uri},
    response::IntoResponse as _,
    routing::get,
};
use krabka_traces::frontend::{
    HttpQuerier, HttpReadinessProbe, MembershipView, QueryFrontend,
    config::FrontendConfig,
    job::{BlockMetaInfo, RowGroupInfo, TraceIndexCatalog},
    refresh_membership,
    server::router_with_backend,
};
use krabka_units::{
    ByteSize,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use serde_json::{Value, json};

/// Every query string one stub querier was asked, in arrival order.
type Log = Arc<Mutex<Vec<String>>>;

/// The state one stub querier answers from.
#[derive(Clone)]
struct StubState {
    log: Log,
    /// The trace id this querier's own live-store holds, and nobody else's.
    live_trace: String,
    /// Whether a by-id lookup on this querier finds the trace.
    holds_trace: bool,
}

/// A running stub querier: where to reach it, and what it was asked.
struct Stub {
    addr: String,
    log: Log,
    live_trace: String,
}

/// A stub querier that answers `/ready` with `readiness` and serves searches.
///
/// A search carrying `block=` is a cold job, and every stub answers those the
/// same way, because a block is readable from any of them. A search with no
/// scan params is the live shard, and each stub answers with its own trace id,
/// so a querier left out of the fan-out shows up as a trace that is not in the
/// response.
async fn spawn_querier(readiness: StatusCode, ready_body: &'static str, live_trace: &str) -> Stub {
    spawn_querier_holding(readiness, ready_body, live_trace, false).await
}

/// As [`spawn_querier`], and this one answers a by-id lookup with a trace.
async fn spawn_querier_holding(
    readiness: StatusCode,
    ready_body: &'static str,
    live_trace: &str,
    holds_trace: bool,
) -> Stub {
    let state = StubState {
        log: Arc::new(Mutex::new(Vec::new())),
        live_trace: live_trace.to_string(),
        holds_trace,
    };
    let app = Router::new()
        .route(
            "/ready",
            get(move || async move { (readiness, ready_body) }),
        )
        .route(
            "/api/search",
            get(|State(state): State<StubState>, uri: Uri| async move {
                let query = uri.query().unwrap_or_default().to_string();
                let cold = query.contains("block=");
                state.log.lock().unwrap().push(query);
                let trace_id = if cold {
                    "cold".to_string()
                } else {
                    state.live_trace.clone()
                };
                axum::Json(json!({
                    "traces": [trace_json(&trace_id)],
                    "metrics": { "inspectedBytes": "1" }
                }))
            }),
        )
        .route(
            "/api/v2/traces/{trace_id}",
            get(|State(state): State<StubState>| async move {
                state.log.lock().unwrap().push("by-id".to_string());
                if !state.holds_trace {
                    return (StatusCode::NOT_FOUND, "trace not found").into_response();
                }
                axum::Json(json!({
                    "trace": {
                        "resourceSpans": [{
                            "resource": { "attributes": [] },
                            "scopeSpans": [{
                                "scope": {},
                                "spans": [{ "spanId": "BgYGBgYGBgY=", "name": "op" }]
                            }]
                        }]
                    },
                    "status": "COMPLETE",
                    "message": ""
                }))
                .into_response()
            }),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Stub {
        addr: addr.to_string(),
        log: state.log,
        live_trace: state.live_trace,
    }
}

fn trace_json(trace_id: &str) -> Value {
    json!({
        "traceID": trace_id,
        "rootServiceName": "svc",
        "rootTraceName": "GET /",
        "startTimeUnixNano": "1",
        "durationMs": 1,
        "spanSets": [{ "spans": [], "matched": 0 }]
    })
}

/// Two cold blocks for `t1`, each a whole-block job.
fn two_block_catalog() -> TraceIndexCatalog {
    let block = |id: &str, start: i64, end: i64| BlockMetaInfo {
        block_id: id.to_string(),
        start_ns: start,
        end_ns: end,
        size: ByteSize::from_bytes(100),
        row_groups: vec![RowGroupInfo {
            index: 0,
            compressed: ByteSize::from_bytes(100),
        }],
    };
    TraceIndexCatalog::new(BTreeMap::from([(
        "t1".to_string(),
        vec![
            block("blocks/a.parquet", 0, 1_000_000_000),
            block("blocks/b.parquet", 1_000_000_000, 2_000_000_000),
        ],
    )]))
}

fn cfg() -> FrontendConfig {
    FrontendConfig {
        // Every window reaches the hot tier, so every plan carries a live shard.
        hot_frontier_ns: 0,
        ..FrontendConfig::default()
    }
}

/// Build the frontend the way the role binary does: probe the endpoints, then
/// serve with whatever the probe found.
async fn serve(endpoints: &[String], cfg: FrontendConfig) -> std::net::SocketAddr {
    let probe = HttpReadinessProbe::new(Duration::from_secs(5)).unwrap();
    let membership = MembershipView::empty();
    membership.publish(refresh_membership(endpoints, &probe).await);
    serve_with(membership, cfg).await
}

async fn serve_with(membership: MembershipView, cfg: FrontendConfig) -> std::net::SocketAddr {
    let backend = HttpQuerier::new(cfg.request_timeout.to_std()).unwrap();
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(two_block_catalog()),
        cfg,
        membership,
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router_with_backend(qf))
            .await
            .unwrap();
    });
    addr
}

async fn search(frontend: std::net::SocketAddr) -> (StatusCode, Value) {
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{frontend}/api/search?q=%7B%7D&start=0&end=2"
        ))
        .header("X-Scope-OrgID", "t1")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.json::<Value>().await.unwrap_or(Value::Null);
    (status, body)
}

fn trace_ids(body: &Value) -> BTreeSet<String> {
    body["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["traceID"].as_str().unwrap().to_string())
        .collect()
}

fn cold_job_count(stub: &Stub) -> usize {
    stub.log
        .lock()
        .unwrap()
        .iter()
        .filter(|q| q.contains("block="))
        .count()
}

fn live_job_count(stub: &Stub) -> usize {
    stub.log
        .lock()
        .unwrap()
        .iter()
        .filter(|q| !q.contains("block="))
        .count()
}

/// The whole pool is ready: nothing is lost, so the body is exactly the shape
/// Tempo returns. The `warnings` key must be **absent**, not empty -- a client
/// that sees the key at all is entitled to read it as a caveat.
#[tokio::test]
async fn a_whole_pool_answers_with_no_warnings_and_the_plain_tempo_body() {
    let a = spawn_querier(StatusCode::OK, "ready\n", "hot-a").await;
    let b = spawn_querier(StatusCode::OK, "ready\n", "hot-b").await;
    let endpoints = vec![a.addr.clone(), b.addr.clone()];
    let frontend = serve(&endpoints, cfg()).await;

    let (status, body) = search(frontend).await;
    check!(status == StatusCode::OK);
    check!(
        body.get("warnings").is_none(),
        "a complete answer carries no warnings key: {body}"
    );
    // The live tier is sharded, so the live shard reaches both queriers; the
    // two cold blocks are readable from either, so they are scanned once each.
    check!(live_job_count(&a) == 1 && live_job_count(&b) == 1);
    check!(cold_job_count(&a) + cold_job_count(&b) == 2);
    // Both hot slices reached the client, together with the cold one.
    check!(
        trace_ids(&body)
            == BTreeSet::from([
                a.live_trace.clone(),
                b.live_trace.clone(),
                "cold".to_string()
            ])
    );
    check!(body["metrics"]["totalJobs"] == json!(4));
    check!(body["metrics"]["totalBlocks"] == json!(2));
}

/// A querier that fails its readiness probe is out of the pool before the
/// query is planned. Its blocks move to the querier that is up, so the cold
/// half of the answer is whole; the hot slice only it held cannot move, so the
/// client is told which querier is missing and why.
#[tokio::test]
async fn a_querier_that_is_not_ready_takes_no_jobs_and_is_named_in_the_response() {
    let up = spawn_querier(StatusCode::OK, "ready\n", "hot-up").await;
    let down = spawn_querier(
        StatusCode::SERVICE_UNAVAILABLE,
        "not ready: live-store\n",
        "hot-down",
    )
    .await;
    let endpoints = vec![up.addr.clone(), down.addr.clone()];
    let frontend = serve(&endpoints, cfg()).await;

    let (status, body) = search(frontend).await;
    check!(status == StatusCode::OK);

    // Not one job -- cold or live -- went to the querier that failed its probe.
    check!(
        down.log.lock().unwrap().is_empty(),
        "an unready querier still received {} jobs",
        down.log.lock().unwrap().len()
    );
    // The cold half is complete regardless: both blocks were scanned.
    check!(cold_job_count(&up) == 2);
    check!(body["metrics"]["totalBlocks"] == json!(2));

    // The hot slice the ejected querier held is genuinely absent from the
    // body -- this is the loss, made visible.
    check!(
        trace_ids(&body) == BTreeSet::from([up.live_trace.clone(), "cold".to_string()]),
        "the ejected querier's hot slice is missing: {body}"
    );

    // And the answer says so, naming the querier and the gate its own
    // `/ready` reported, rather than returning the fraction as a whole.
    let warnings = body["warnings"].as_array().expect("warnings present");
    check!(warnings.len() == 1, "{warnings:?}");
    let warning = warnings[0].as_str().unwrap();
    check!(warning.contains(&down.addr), "{warning}");
    check!(warning.contains("live-store"), "{warning}");
}

/// A querier that answers `/ready` and then dies is the case membership cannot
/// catch. The job fails, and a search shard is a partition of the data, so the
/// query fails rather than returning the surviving shards as a whole answer.
#[tokio::test]
async fn a_querier_that_dies_after_the_probe_fails_the_query_rather_than_shrinking_it() {
    let up = spawn_querier(StatusCode::OK, "ready\n", "hot-up").await;
    // A socket that was bound and released: it passes nothing, and the probe
    // is not consulted because the membership is handed in already ready.
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap().to_string();
    drop(dead);

    let membership = MembershipView::fixed([up.addr.clone(), dead_addr]);
    let frontend = serve_with(membership, cfg()).await;

    let (status, _body) = search(frontend).await;
    check!(
        status == StatusCode::BAD_GATEWAY,
        "a lost shard is an error, not a smaller 200"
    );
}

/// With nobody ready there is no answer to give. Returning `200 []` would be
/// the completest form of the loss this path exists to prevent.
#[tokio::test]
async fn an_empty_pool_fails_rather_than_returning_an_empty_result() {
    let down = spawn_querier(
        StatusCode::SERVICE_UNAVAILABLE,
        "not ready: trace-index\n",
        "hot-down",
    )
    .await;
    let frontend = serve(std::slice::from_ref(&down.addr), cfg()).await;

    let (status, _body) = search(frontend).await;
    check!(status == StatusCode::BAD_GATEWAY);
    check!(down.log.lock().unwrap().is_empty());
}

/// The live tier is what a lost querier actually costs. A query window that
/// never reaches the hot frontier plans no live shard, so an excluded querier
/// costs nothing at all and the answer must not claim otherwise.
#[tokio::test]
async fn a_cold_only_window_loses_nothing_to_an_excluded_querier_and_says_nothing() {
    let up = spawn_querier(StatusCode::OK, "ready\n", "hot-up").await;
    let down = spawn_querier(
        StatusCode::SERVICE_UNAVAILABLE,
        "not ready: live-store\n",
        "hot-down",
    )
    .await;
    let endpoints = vec![up.addr.clone(), down.addr.clone()];
    let frontend = serve(
        &endpoints,
        FrontendConfig {
            // The hot tier starts far past the query window, so no live shard
            // is planned.
            hot_frontier_ns: i64::MAX,
            ..FrontendConfig::default()
        },
    )
    .await;

    let (status, body) = search(frontend).await;
    check!(status == StatusCode::OK);
    check!(live_job_count(&up) == 0, "no live shard was planned");
    check!(cold_job_count(&up) == 2, "both blocks still scanned");
    check!(
        body.get("warnings").is_none(),
        "a cold-only answer loses nothing to an ejected querier: {body}"
    );
    check!(trace_ids(&body) == BTreeSet::from(["cold".to_string()]));
}

async fn by_id(frontend: std::net::SocketAddr) -> (StatusCode, String) {
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{frontend}/api/v2/traces/{}",
            "0a".repeat(16)
        ))
        .header("X-Scope-OrgID", "t1")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    (status, resp.text().await.unwrap_or_default())
}

/// A by-id 404 asserts the trace does not exist anywhere. With a querier out
/// of the fan-out, nobody asked the one that may have held it, so the body
/// says what was never looked at instead of claiming absence.
#[tokio::test]
async fn a_by_id_miss_admits_which_querier_was_never_asked() {
    let up = spawn_querier(StatusCode::OK, "ready\n", "hot-up").await;
    let down = spawn_querier(
        StatusCode::SERVICE_UNAVAILABLE,
        "not ready: live-store\n",
        "hot-down",
    )
    .await;
    let frontend = serve(&[up.addr.clone(), down.addr.clone()], cfg()).await;

    let (status, body) = by_id(frontend).await;
    check!(status == StatusCode::NOT_FOUND);
    check!(body.contains(&down.addr), "{body}");
    check!(body.contains("live-store"), "{body}");
    check!(down.log.lock().unwrap().is_empty());
}

/// A whole pool that finds nothing really did look everywhere, so the 404 is
/// the plain Tempo one.
#[tokio::test]
async fn a_by_id_miss_on_a_whole_pool_is_the_plain_tempo_404() {
    let a = spawn_querier(StatusCode::OK, "ready\n", "hot-a").await;
    let b = spawn_querier(StatusCode::OK, "ready\n", "hot-b").await;
    let frontend = serve(&[a.addr.clone(), b.addr.clone()], cfg()).await;

    let (status, body) = by_id(frontend).await;
    check!(status == StatusCode::NOT_FOUND);
    check!(body == "trace not found", "{body}");
}

/// A trace assembled while a querier was out of the fan-out is missing
/// whatever recent spans that querier held. Tempo's v2 envelope already has
/// the field for that: `status: PARTIAL` with the reason in `message`.
#[tokio::test]
async fn an_assembled_trace_is_partial_when_a_querier_was_left_out() {
    let up = spawn_querier_holding(StatusCode::OK, "ready\n", "hot-up", true).await;
    let down = spawn_querier(
        StatusCode::SERVICE_UNAVAILABLE,
        "not ready: live-store\n",
        "hot-down",
    )
    .await;
    let frontend = serve(&[up.addr.clone(), down.addr.clone()], cfg()).await;

    let resp = reqwest::Client::new()
        .get(format!(
            "http://{frontend}/api/v2/traces/{}",
            "0a".repeat(16)
        ))
        .header("X-Scope-OrgID", "t1")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .unwrap();
    check!(resp.status() == StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    check!(body["status"] == json!("PARTIAL"), "{body}");
    let message = body["message"].as_str().unwrap();
    check!(message.contains(&down.addr), "{message}");
    check!(message.contains("live-store"), "{message}");
}
