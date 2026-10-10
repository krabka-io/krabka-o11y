//! The tail endpoint, and the hot WAL tail it streams over a websocket.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use axum::{Router, http::StatusCode};
use futures_util::{SinkExt as _, StreamExt as _};
use krabka_blockstore::labels;
use krabka_observability::{InMemoryWalSink, LogWalSink, WalLogRecord, loki_router};
use serde_json::{Value, json};
use support::{
    LogEntry, fixture, log_entry, next_frame, next_frame_within_two_seconds, open_tail,
    serve_for_websocket,
};
use tokio::time::{Duration, timeout};
use tokio_tungstenite::connect_async;

const ERROR_TAIL: &str =
    "/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000";

fn record(timestamp_ns: i64, line: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}

/// The tail frame that carries only `entry` of the `api` prod stream.
fn frame_of(entry: LogEntry<'_>) -> Value {
    json!({
        "streams": [{
            "stream": {"app": "api", "env": "prod"},
            "values": [[entry.timestamp_ns.to_string(), entry.line]]
        }],
    })
}

/// A querier over the shared fixture whose hot tail holds `records`, with
/// everything through 19 ns already compacted.
async fn hot_tail_app(
    records: impl IntoIterator<Item = WalLogRecord>,
) -> (InMemoryWalSink, Router) {
    let hot_tail = InMemoryWalSink::default();
    for record in records {
        hot_tail.append(record).await.unwrap();
    }
    let app = loki_router(fixture().with_hot_tail(hot_tail.clone(), 19));
    (hot_tail, app)
}

#[tokio::test]
async fn tail_endpoint_does_not_resend_records_after_an_idle_poll() {
    let (hot_tail, app) = hot_tail_app([record(20, "api first error")]).await;
    let (mut socket, server) = open_tail(app, &format!("{ERROR_TAIL}&end=0.000000030")).await;

    let frame = next_frame_within_two_seconds(&mut socket).await;
    assert!(frame == frame_of(log_entry(20, "api first error")));

    // Let the stream poll an unchanged buffer several times over. Only a
    // stream that leaves its cursor alone across those idle polls stays quiet;
    // one that rewinds resends what it already sent, and that resent frame --
    // not the new record -- is what arrives next.
    tokio::time::sleep(Duration::from_millis(400)).await;

    hot_tail
        .append(record(21, "api later error"))
        .await
        .unwrap();
    let frame = next_frame_within_two_seconds(&mut socket).await;
    server.abort();
    assert!(frame == frame_of(log_entry(21, "api later error")));
}

#[tokio::test]
async fn tail_endpoint_streams_hot_wal_tail_over_websocket() {
    let (hot_tail, app) = hot_tail_app([record(20, "api hot error")]).await;
    let (mut socket, server) = open_tail(app, &format!("{ERROR_TAIL}&end=0.000000030")).await;

    let frame = next_frame(&mut socket).await;

    assert!(frame == frame_of(log_entry(20, "api hot error")));

    socket
        .send(tokio_tungstenite::tungstenite::Message::Ping(
            b"alive".to_vec().into(),
        ))
        .await
        .unwrap();
    let pong = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(pong, tokio_tungstenite::tungstenite::Message::Pong(payload) if payload == b"alive"[..])
    );

    hot_tail
        .append(record(21, "api later error"))
        .await
        .unwrap();
    let frame = next_frame_within_two_seconds(&mut socket).await;
    server.abort();

    assert!(frame == frame_of(log_entry(21, "api later error")));
}

#[tokio::test]
async fn tail_endpoint_applies_limit_to_hot_wal_tail_frame() {
    let (_, app) = hot_tail_app([
        record(20, "api first error"),
        record(21, "api second error"),
    ])
    .await;
    let (mut socket, server) =
        open_tail(app, &format!("{ERROR_TAIL}&end=0.000000030&limit=1")).await;

    let frame = next_frame(&mut socket).await;
    server.abort();

    assert!(frame == frame_of(log_entry(20, "api first error")));
}

#[tokio::test]
async fn tail_endpoint_defaults_limit_to_one_hundred_entries() {
    let (_, app) =
        hot_tail_app((0..101).map(|index| record(20 + index, &format!("api error {index}")))).await;
    let (mut socket, server) = open_tail(app, &format!("{ERROR_TAIL}&end=0.000000200")).await;

    let frame = next_frame(&mut socket).await;
    server.abort();
    let values = frame
        .pointer("/streams/0/values")
        .and_then(Value::as_array)
        .unwrap();

    check!(values.len() == 100);
    check!(values.first() == Some(&json!(["20", "api error 0"])));
    check!(values.last() == Some(&json!(["119", "api error 99"])));
}

#[tokio::test]
async fn tail_endpoint_rejects_delay_for_over_five_seconds() {
    let (request, server) = serve_for_websocket(
        loki_router(fixture()),
        "/loki/api/v1/tail?delay_for=6&query=%7Bapp%3D%22api%22%7D",
    )
    .await;

    let error = connect_async(request).await.unwrap_err();
    server.abort();

    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("expected HTTP websocket error");
    };
    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/plain"))
    );
    assert!(response.body().as_deref() == Some("delay_for can't be greater than 5".as_bytes()));
}

#[tokio::test]
async fn tail_endpoint_accepts_delay_for_at_five_seconds() {
    let (mut socket, server) = open_tail(
        loki_router(fixture()),
        "/loki/api/v1/tail?delay_for=5&query=%7Bapp%3D%22api%22%7D",
    )
    .await;

    let _ = socket.close(None).await;
    server.abort();
}

#[tokio::test]
async fn tail_endpoint_delays_fresh_records_when_delay_for_is_set() {
    let hot_tail = InMemoryWalSink::default();
    // Future-dated records stay inside the delay window regardless of how
    // long fixture and socket setup take on a loaded runner.
    let timestamp_ns = i64::MAX / 2;
    hot_tail
        .append(record(timestamp_ns, "api fresh error"))
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, timestamp_ns.saturating_sub(1));
    let (mut socket, server) = open_tail(
        loki_router(state),
        &format!(
            "/loki/api/v1/tail?delay_for=1&query=%7Bapp%3D%22api%22%7D&start={}&end={}",
            timestamp_ns.saturating_sub(1),
            timestamp_ns.saturating_add(1),
        ),
    )
    .await;

    assert!(
        timeout(Duration::from_millis(150), socket.next())
            .await
            .is_err()
    );
    let _ = socket.close(None).await;
    server.abort();
}
