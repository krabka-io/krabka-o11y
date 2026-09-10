//! logs -> traces: a trace id attached to a log line comes back out of the logs
//! path in a form the traces path accepts.
//!
//! Correlating a log with its trace is the reason structured metadata carries a
//! `trace_id` at all: a reader finds the log line, reads the id off it, and asks
//! the traces store for the trace. Both halves exist — `krabka-observability`
//! ingests and stores structured metadata, `krabka-traces` looks a trace up by
//! id — and nothing joined them, so nothing established that the id one side
//! writes is the id the other side can be asked for.
//!
//! ```text
//! POST /loki/api/v1/push  (trace_id in structured metadata)
//!   -> WalLogRecord::structured_metadata      (krabka-observability)
//!   -> GET /loki/api/v1/query_range           the line is queryable
//!   -> hex -> [u8; 16]                        [the join]
//!   -> LiveStore::trace_by_id                 (krabka-traces)
//! ```
//!
//! # What this deliberately does not assert
//!
//! The JSON encoding of structured metadata in a Loki query response is being
//! corrected against a real Loki container as this is written, and there is no
//! stable shape to pin. So the id is read back off the **typed** `WalLogRecord`
//! that ingest produced, not out of the query response's JSON, and the query
//! leg asserts only that the line is queryable and comes back. Pinning the wire
//! shape belongs in `krabka-observability`'s own differential suite, which is
//! where it is being fixed; the link this crate exists to prove is that the id
//! survives the logs path intact and is usable against traces, and that is what
//! is asserted.

use std::time::{SystemTime, UNIX_EPOCH};

use assert2::{assert, check};
use axum::http::{Request, StatusCode};
use krabka_blockstore::{LabelIndex, LogBlockIndex};
use krabka_observability::{InMemoryWalSink, QuerierState, distributor_router, loki_router};
use serde_json::{Value, json};
use tower::ServiceExt as _;

const TENANT: &str = "tenant-a";
const LINE: &str = "order 4711 failed: upstream timeout";

/// The trace this log line belongs to. It is written into the push body as hex,
/// the way an OpenTelemetry-instrumented emitter writes it, and it has to
/// survive back out as the same 16 bytes `krabka-traces` keys a trace on.
const TRACE_ID: [u8; 16] = [
    0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36,
];
const SPAN_ID: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];

/// Now, in nanoseconds. The distributor rejects samples older than its
/// `reject_old_samples_max_age`, so the line has to be recent rather than at a
/// fixed epoch.
fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_nanos(),
    )
    .expect("the current time fits in an i64 of nanoseconds")
}

/// The trace the log line points at, held in the traces path's in-memory store.
fn live_store() -> krabka_traces::LiveStore {
    let mut store = krabka_traces::LiveStore::new(i64::MAX);
    store.ingest(krabka_traces::SpanRecord {
        tenant: TENANT.to_string(),
        span: krabka_traces::Span {
            trace_id: TRACE_ID,
            span_id: SPAN_ID,
            parent_span_id: None,
            name: "POST /orders".to_string(),
            kind: krabka_traces::SpanKind::Server,
            start_ns: now_ns(),
            duration_ns: 5_000_000,
            status: krabka_traces::StatusCode::Error,
            status_message: "upstream timeout".to_string(),
            resource_attrs: Vec::new(),
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        },
    });
    store
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trace_id_on_a_log_line_fetches_the_trace() {
    let timestamp_ns = now_ns();
    let sink = InMemoryWalSink::default();

    // --- logs: push one line carrying the trace id as structured metadata --
    let push = distributor_router(sink.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("Content-Type", "application/json")
                .header("X-Scope-OrgID", TENANT)
                .body(axum::body::Body::from(
                    json!({
                        "streams": [{
                            "stream": { "app": "orders", "level": "error" },
                            "values": [[
                                timestamp_ns.to_string(),
                                LINE,
                                {
                                    "trace_id": hex::encode(TRACE_ID),
                                    "span_id": hex::encode(SPAN_ID),
                                },
                            ]],
                        }],
                    })
                    .to_string(),
                ))
                .expect("loki push request"),
        )
        .await
        .expect("loki push response");

    assert!(push.status() == StatusCode::NO_CONTENT);

    // --- logs: the id survived ingest as typed structured metadata ---------
    let records = sink.records();
    assert!(records.len() == 1);
    let record = &records[0];

    check!(record.tenant == TENANT);
    check!(record.line == LINE);
    check!(record.timestamp_ns == timestamp_ns);
    check!(
        record
            .structured_metadata
            .get("trace_id")
            .map(String::as_str)
            == Some(hex::encode(TRACE_ID).as_str())
    );
    check!(
        record
            .structured_metadata
            .get("span_id")
            .map(String::as_str)
            == Some(hex::encode(SPAN_ID).as_str())
    );

    // --- logs: and the line is queryable back out of the same store --------
    let data_root = tempfile::tempdir().expect("querier data root");
    // Empty cold indexes and a `0` compaction frontier: the compactor has not
    // run, so every pushed record is still in the hot tail and the query is
    // answered from there. `build_service_router` is the deployed entry point,
    // but it insists on a manifest the compactor writes; `QuerierState` is the
    // same querier assembled directly, which is what an uncompacted store is.
    let querier = loki_router(
        QuerierState::new(
            data_root.path().to_path_buf(),
            LabelIndex::default(),
            LogBlockIndex::default(),
        )
        .with_hot_tail(sink.clone(), 0),
    );

    let response = querier
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/loki/api/v1/query_range?query={}&start={}&end={}&direction=forward",
                    urlencode("{app=\"orders\"} |= \"upstream timeout\""),
                    timestamp_ns - 1_000_000_000,
                    timestamp_ns + 1_000_000_000,
                ))
                .header("X-Scope-OrgID", TENANT)
                .body(axum::body::Body::empty())
                .expect("query_range request"),
        )
        .await
        .expect("query_range response");

    assert!(response.status() == StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("query_range body");
    let json: Value = serde_json::from_slice(&body).expect("query_range json");

    check!(json["status"] == "success");
    // The line came back. How the response encodes the metadata beside it is
    // `krabka-observability`'s business, and is asserted there.
    let lines = collect_lines(&json);
    check!(lines == vec![LINE]);

    // --- traces: the id off the log line fetches the trace -----------------
    let hex_id = record
        .structured_metadata
        .get("trace_id")
        .expect("the log line carries a trace id");
    let raw = hex::decode(hex_id).expect("the trace id is hex");
    let trace_id: [u8; 16] = raw
        .try_into()
        .expect("a trace id is sixteen bytes, which is what traces keys on");

    let spans = live_store().trace_by_id(TENANT, &trace_id);
    assert!(spans.len() == 1);
    check!(spans[0].span_id == SPAN_ID);
    check!(spans[0].name == "POST /orders");
    check!(spans[0].status == krabka_traces::StatusCode::Error);

    // A trace id that no log line carried must not resolve, or the lookup above
    // would prove nothing about which id was used.
    check!(live_store().trace_by_id(TENANT, &[0xff; 16]).is_empty());
}

/// Every `values` entry across every returned stream, in order.
///
/// The entry is `[timestamp, line]` with an optional trailing metadata element,
/// so the line is the second element whatever the encoding does after it.
fn collect_lines(json: &Value) -> Vec<&str> {
    json["data"]["result"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .flat_map(|stream| {
            stream["values"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default()
                .iter()
        })
        .filter_map(|entry| entry.get(1).and_then(Value::as_str))
        .collect()
}

/// Percent-encodes a `LogQL` selector for a query string.
///
/// The selector is full of `{`, `"` and spaces, none of which survive a raw
/// URI. `reqwest` would do this, but the query goes through the router directly
/// rather than over a socket.
fn urlencode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}
