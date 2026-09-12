//! The tail endpoint, and the hot WAL tail it streams over a websocket.

mod support;

use std::collections::BTreeMap;

use assert2::{assert, check};
use axum::http::StatusCode;
use futures_util::{SinkExt as _, StreamExt as _};
use krabka_blockstore::labels;
use krabka_observability::{InMemoryWalSink, LogWalSink, WalLogRecord, loki_router};
use serde_json::{Value, json};
use support::{current_unix_epoch_nanos, fixture};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest as _};

#[tokio::test]
async fn tail_endpoint_does_not_resend_records_after_an_idle_poll() {
    let record = |timestamp_ns, line: &str| WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    };
    let frame_of = |timestamp: &str, line: &str| {
        json!({
            "streams": [{
                "stream": {"app": "api", "detected_level": "unknown", "env": "prod"},
                "values": [[timestamp, line]]
            }],
            "dropped_entries": []
        })
    };

    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(record(20, "api first error"))
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail.clone(), 19);
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30"
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let message = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert!(frame == frame_of("20", "api first error"));

    // Let the stream poll an unchanged buffer several times over. Only a
    // stream that leaves its cursor alone across those idle polls stays quiet;
    // one that rewinds resends what it already sent, and that resent frame --
    // not the new record -- is what arrives next.
    tokio::time::sleep(Duration::from_millis(400)).await;

    hot_tail
        .append(record(21, "api later error"))
        .await
        .unwrap();
    let message = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    server.abort();
    assert!(frame == frame_of("21", "api later error"));
}

#[tokio::test]
async fn tail_endpoint_streams_hot_wal_tail_over_websocket() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail.clone(), 19);
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30"
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let message = socket.next().await.unwrap().unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();

    assert!(
        frame
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "env": "prod"
                        },
                        "values": [
                            ["20", "api hot error"]
                        ]
                    }
                ],
                "dropped_entries": []
            })
    );

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
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 21,
            line: "api later error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let message = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    server.abort();

    assert!(
        frame
            == json!({
                    "streams": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["21", "api later error"]
                            ]
                        }
                    ],
                    "dropped_entries": []
            })
    );
}

#[tokio::test]
async fn tail_endpoint_applies_limit_to_hot_wal_tail_frame() {
    let hot_tail = InMemoryWalSink::default();
    for (timestamp_ns, line) in [(20, "api first error"), (21, "api second error")] {
        hot_tail
            .append(WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([("app", "api"), ("env", "prod")]),
                timestamp_ns,
                line: line.to_string(),
                structured_metadata: BTreeMap::new(),
                position: None,
            })
            .await
            .unwrap();
    }
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&limit=1"
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let message = socket.next().await.unwrap().unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    server.abort();

    assert!(
        frame
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "env": "prod"
                        },
                        "values": [
                            ["20", "api first error"]
                        ]
                    }
                ],
                "dropped_entries": []
            })
    );
}

#[tokio::test]
async fn tail_endpoint_defaults_limit_to_one_hundred_entries() {
    let hot_tail = InMemoryWalSink::default();
    for index in 0..101 {
        hot_tail
            .append(WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([("app", "api"), ("env", "prod")]),
                timestamp_ns: 20 + index,
                line: format!("api error {index}"),
                structured_metadata: BTreeMap::new(),
                position: None,
            })
            .await
            .unwrap();
    }
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=200"
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let message = socket.next().await.unwrap().unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
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
    let state = fixture();
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request =
        format!("ws://{addr}/loki/api/v1/tail?delay_for=6&query=%7Bapp%3D%22api%22%7D")
            .into_client_request()
            .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

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
    assert_eq!(
        response.body().as_deref(),
        Some("delay_for can't be greater than 5".as_bytes())
    );
}

#[tokio::test]
async fn tail_endpoint_accepts_delay_for_at_five_seconds() {
    let state = fixture();
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request =
        format!("ws://{addr}/loki/api/v1/tail?delay_for=5&query=%7Bapp%3D%22api%22%7D")
            .into_client_request()
            .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let _ = socket.close(None).await;
    server.abort();
}

#[tokio::test]
async fn tail_endpoint_delays_fresh_records_when_delay_for_is_set() {
    let hot_tail = InMemoryWalSink::default();
    let timestamp_ns = i64::try_from(current_unix_epoch_nanos()).unwrap();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns,
            line: "api fresh error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, timestamp_ns.saturating_sub(1));
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?delay_for=1&query=%7Bapp%3D%22api%22%7D&start={}&end={}",
        timestamp_ns.saturating_sub(1),
        timestamp_ns.saturating_add(1),
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    assert!(
        timeout(Duration::from_millis(150), socket.next())
            .await
            .is_err()
    );
    let _ = socket.close(None).await;
    server.abort();
}
