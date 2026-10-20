//! `HttpQuerier` request shape and Tempo-JSON parse, pinned by a loopback axum
//! stub.
//!
//! The tests verify the querier's real scan-job contract, which is `block`,
//! `rowGroupStart` and `rowGroupEnd`. They verify `X-Scope-OrgID`. They also
//! verify that a 404 by-id response degrades to an empty partial.
//!
//! Every request names the querier it is for. The transport dials that
//! address and picks nothing of its own.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use assert2::check;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, Uri},
    routing::get,
};
use krabka_blockstore::{TENANT_HEADER, TenantId};
use krabka_traces::frontend::{
    backend::{QuerierBackend, SearchJobRequest, TagValuesJobRequest, TraceByIdJobRequest},
    http_backend::HttpQuerier,
    job::JobShard,
};

#[path = "support/mod.rs"]
mod support;

type Log = Arc<Mutex<Vec<String>>>;

fn search_stub(seen: Log, include_tenant: bool, body: serde_json::Value) -> Router {
    Router::new()
        .route(
            "/api/search",
            get(
                move |State(log): State<Log>, headers: HeaderMap, uri: Uri| {
                    let body = body.clone();
                    async move {
                        let params = uri.query().unwrap_or_default().to_string();
                        let entry = if include_tenant {
                            let tenant = headers
                                .get(TENANT_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default();
                            format!("{tenant}|{params}")
                        } else {
                            params
                        };
                        log.lock().unwrap().push(entry);
                        Json(body)
                    }
                },
            ),
        )
        .with_state(seen)
}

#[tokio::test]
async fn http_querier_search_job_sends_scan_params_and_parses() {
    let seen: Log = Arc::new(Mutex::new(Vec::new()));

    let app = search_stub(
        seen.clone(),
        true,
        serde_json::json!({
            "traces": [{
                "traceID": "0a".repeat(16),
                "rootServiceName": "svc",
                "rootTraceName": "GET /",
                "startTimeUnixNano": "1",
                "durationMs": 1,
                "spanSets": [{ "spans": [], "matched": 0 }]
            }],
            "metrics": { "totalBlocks": "1", "inspectedTraces": "1", "inspectedBytes": "64" }
        }),
    );

    let addr = spawn(app).await;
    let backend = plain_http_querier();

    let out = backend
        .search_job(&SearchJobRequest {
            tenant: TenantId::new("tenant-x").unwrap(),
            query: "{ }".to_string(),
            start_ns: 0,
            end_ns: 100_000_000_000,
            limit: 20,
            spss: 3,
            shard: JobShard::Block {
                block_id: "blk-1".to_string(),
                row_group_start: 2,
                row_group_end: 5,
            },
            querier: addr.to_string(),
        })
        .await
        .unwrap();

    assert2::assert!(out.traces.len() == 1);
    assert2::assert!(out.metrics.inspected_bytes == 64);
    assert2::assert!(out.metrics.total_blocks == 1);

    let log = seen.lock().unwrap();
    assert2::assert!(log.len() == 1);
    let entry = &log[0];
    check!(entry.starts_with("tenant-x|"));
    // The real querier scan-job params.
    check!(entry.contains("block=blk-1"));
    check!(entry.contains("rowGroupStart=2"));
    check!(entry.contains("rowGroupEnd=5"));
    // start/end are epoch seconds.
    check!(entry.contains("start=0"));
    check!(entry.contains("end=100"));
}

#[tokio::test]
async fn http_querier_live_shard_sends_no_scan_params() {
    let seen: Log = Arc::new(Mutex::new(Vec::new()));
    let app = search_stub(
        seen.clone(),
        false,
        serde_json::json!({ "traces": [], "metrics": {} }),
    );
    let addr = spawn(app).await;
    let backend = plain_http_querier();

    backend
        .search_job(&SearchJobRequest {
            tenant: TenantId::new("t1").unwrap(),
            query: "{ }".to_string(),
            start_ns: 0,
            end_ns: 1,
            limit: 20,
            spss: 3,
            shard: JobShard::Live,
            querier: addr.to_string(),
        })
        .await
        .unwrap();

    let log = seen.lock().unwrap();
    assert2::assert!(!log[0].contains("block="));
    assert2::assert!(!log[0].contains("rowGroup"));
}

#[tokio::test]
async fn http_querier_by_id_404_is_empty_partial() {
    let app = Router::new().route(
        "/api/v2/traces/{trace_id}",
        get(|| async {
            axum::http::Response::builder()
                .status(axum::http::StatusCode::NOT_FOUND)
                .body(axum::body::Body::from("trace not found"))
                .unwrap()
        }),
    );
    let addr = spawn(app).await;
    let backend = plain_http_querier();

    let out = backend
        .trace_by_id_job(&trace_by_id_request(addr, [9; 16]))
        .await
        .unwrap();

    assert2::assert!(out.trace.is_empty());
}

#[tokio::test]
async fn http_querier_by_id_parses_v2_envelope() {
    let app = Router::new().route(
        "/api/v2/traces/{trace_id}",
        get(|| async { support::complete_trace_response() }),
    );
    let addr = spawn(app).await;
    let backend = plain_http_querier();

    let out = backend
        .trace_by_id_job(&trace_by_id_request(addr, [10; 16]))
        .await
        .unwrap();

    assert2::assert!((out.trace.span_count(), out.trace.status.as_str()) == (1, "COMPLETE"));
}

#[tokio::test]
async fn http_querier_tag_values_encodes_tag_path_segment() {
    let seen: Log = Arc::new(Mutex::new(Vec::new()));
    let app =
        Router::new()
            .route(
                "/api/v2/search/tag/{tag}/values",
                get(
                    |State(s): State<Log>,
                     axum::extract::Path(tag): axum::extract::Path<String>| async move {
                        s.lock().unwrap().push(tag);
                        axum::Json(serde_json::json!({ "tagValues": [], "metrics": {} }))
                    },
                ),
            )
            .with_state(seen.clone());
    let addr = spawn(app).await;
    let backend = plain_http_querier();

    // `#` interpolated raw would start a URL fragment and truncate the path
    // (the stub's `{tag}/values` route would never match). Path-segment
    // encoding round-trips it intact.
    backend
        .tag_values_job(&TagValuesJobRequest {
            tenant: TenantId::new("t1").unwrap(),
            tag: "a#b".to_string(),
            start_ns: 0,
            end_ns: 1,
            shard: JobShard::Live,
            querier: addr.to_string(),
        })
        .await
        .unwrap();

    let log = seen.lock().unwrap();
    assert2::assert!(log.as_slice() == ["a#b"]);
}

/// A `t1` trace-by-id job for `trace_id` over `[0, 1)` ns, sent to the
/// querier at `addr`.
fn trace_by_id_request(addr: SocketAddr, trace_id: [u8; 16]) -> TraceByIdJobRequest {
    TraceByIdJobRequest {
        tenant: TenantId::new("t1").unwrap(),
        trace_id,
        start_ns: 0,
        end_ns: 1,
        querier: addr.to_string(),
    }
}

/// An HTTP querier backend with a five-second timeout and the default
/// internal client.
fn plain_http_querier() -> HttpQuerier {
    HttpQuerier::new(
        Duration::from_secs(5),
        krabka_traces::frontend::QuerierScheme::Http,
        &krabka_observability::server_security::InternalClient::default(),
    )
    .unwrap()
}

async fn spawn(app: Router) -> SocketAddr {
    support::spawn_axum(app).await
}
