//! What a panic does to a running role, against the real serving path.
//!
//! A panic inside a request handler must cost that request and nothing else:
//! a 500, on a connection the client can go on using. Uncontained, it unwinds
//! through the per-connection task and the socket closes with no status, which
//! a client reports as a transport error and a dashboard reports as nothing.
//! And it must not cost the *next* request either, which is the harder half:
//! a panic taken while shared state is held poisons a plain `std::sync` lock
//! for the life of the process, so one bad record breaks every later request
//! that touches the same state.
//!
//! A panic inside a background loop is the opposite case, and it must cost the
//! role: there is no request to answer, and a process that survives it keeps
//! its listener, keeps passing a liveness probe, and answers from a tier that
//! stopped moving. `compactor.rs` pins that half for the compactor and querier
//! roles; the control for it is here.

mod support;

use std::sync::Arc;

use assert2::assert;
use async_trait::async_trait;
use krabka_observability::{
    KafkaWalRecord, LogWalConsumer, LogWalSink, PanicSafeShared, QuerierIndexSource, Role,
    ServiceConfig, ServiceDependencies, WalConsumerError, WalLogRecord, WalPosition, WalSinkError,
    serve_service_listener,
};
use krabka_units::Time;
use support::tenant_object_store_shard_catalog_service_fixture;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    time::{Duration, timeout},
};

/// A WAL consumer that polls forever and finds nothing, which is what an idle
/// hot tail looks like.
struct IdleWalConsumer;

#[async_trait]
impl LogWalConsumer for IdleWalConsumer {
    async fn poll(&mut self, _timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        Ok(Vec::new())
    }

    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        Ok(())
    }
}

/// A WAL sink that records what it accepted and panics on one line, the way a
/// handler with a bug on one code path would -- and it panics while holding
/// the shared state, which is the half that a plain `Mutex` would poison for
/// good.
struct SinkThatPanicsOnOneLine {
    accepted: Arc<PanicSafeShared<Vec<String>>>,
}

#[async_trait]
impl LogWalSink for SinkThatPanicsOnOneLine {
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError> {
        self.accepted.update(|accepted| {
            accepted.push(record.line.clone());
            assert!(record.line != "poison", "one bad record");
        });
        Ok(())
    }
}

fn distributor_config(data_root: &std::path::Path) -> ServiceConfig {
    ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().expect("a loopback address"),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-block-builder".to_string(),
        data_root: data_root.into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        ..ServiceConfig::default()
    }
}

/// A hot tail that is merely idle is not a fault, and the role keeps serving.
///
/// This is the control for
/// `querier_service_stops_serving_when_its_spawned_wal_consumer_loop_panics`
/// in the compactor suite, which pins the other half: the same querier stops
/// when the same loop panics. Without a control, that test would also pass
/// for a supervisor that ended the role whenever any task was polled.
#[tokio::test]
async fn an_idle_wal_consumer_leaves_the_querier_serving() {
    let (config, store, _dir) = tenant_object_store_shard_catalog_service_fixture().await;
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("a free port");
    let addr = listener.local_addr().expect("the port is bound");
    let role = tokio::spawn(async move {
        serve_service_listener(
            listener,
            config,
            ServiceDependencies::default().with_wal_consumer(IdleWalConsumer),
            Some(&store),
        )
        .await
    });

    let stopped = timeout(Duration::from_millis(500), role).await;

    assert!(stopped.is_err());
    assert!(TcpStream::connect(addr).await.is_ok());
}

/// One request's panic must not become every later request's panic.
///
/// This is the poisoned-lock failure in full: the sink panics while holding
/// the state that every push writes to. Uncontained, the first push drops the
/// connection; with the lock poisoned and recovered blindly, the second push
/// sees a half-written line. Both requests go over one socket, so a dropped
/// connection fails the second read rather than quietly passing.
#[tokio::test]
async fn a_handler_panic_answers_500_and_the_next_request_still_works() {
    let dir = tempfile::tempdir().expect("a temporary data root");
    let accepted = Arc::new(PanicSafeShared::<Vec<String>>::default());
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("a free port");
    let addr = listener.local_addr().expect("the port is bound");
    let role = tokio::spawn(serve_service_listener(
        listener,
        distributor_config(dir.path()),
        ServiceDependencies::default().with_wal_sink(SinkThatPanicsOnOneLine {
            accepted: Arc::clone(&accepted),
        }),
        None,
    ));

    let mut stream = TcpStream::connect(addr).await.expect("the port accepts");
    let panicking = push_one_line(&mut stream, addr, "poison").await;
    let following = push_one_line(&mut stream, addr, "good").await;
    role.abort();

    assert!(panicking.starts_with("HTTP/1.1 500 Internal Server Error"));
    assert!(following.starts_with("HTTP/1.1 204 No Content"));
    // The line that panicked never reached the shared state, and the line
    // after it did. A blind recovery from the poison would show both.
    assert!(accepted.snapshot() == vec!["good".to_string()]);
}

/// Pushes one log line over an already-open connection and returns the raw
/// response, leaving the connection open for the next one.
async fn push_one_line(stream: &mut TcpStream, addr: std::net::SocketAddr, line: &str) -> String {
    let timestamp = current_unix_second_ns().to_string();
    let payload = serde_json::json!({
        "streams": [{
            "stream": { "app": "api" },
            "values": [[timestamp, line]],
        }]
    })
    .to_string();
    stream
        .write_all(
            format!(
                "POST /loki/api/v1/push HTTP/1.1\r\nHost: {addr}\r\nX-Scope-OrgID: tenant-a\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
                payload.len()
            )
            .as_bytes(),
        )
        .await
        .expect("the request is written");
    read_one_response(stream).await
}

/// Reads exactly one HTTP/1.1 response, so the socket is left positioned at
/// the start of the next one rather than drained to EOF.
async fn read_one_response(stream: &mut TcpStream) -> String {
    let mut raw = Vec::new();
    let mut byte = [0_u8; 1];
    // Headers first: they end at the blank line, and only they can say how
    // long the body is.
    while !raw.ends_with(b"\r\n\r\n") {
        let read = stream.read(&mut byte).await.expect("the response arrives");
        assert!(read == 1, "the connection closed mid-response");
        raw.push(byte[0]);
    }
    let headers = String::from_utf8(raw.clone()).expect("the headers are text");
    let content_length = headers
        .lines()
        .find_map(|header| {
            header
                .strip_prefix("content-length: ")
                .or_else(|| header.strip_prefix("Content-Length: "))
        })
        .map_or(0, |value| {
            value.trim().parse::<usize>().expect("a numeric length")
        });
    let mut body = vec![0_u8; content_length];
    stream
        .read_exact(&mut body)
        .await
        .expect("the body arrives in full");
    raw.extend_from_slice(&body);
    String::from_utf8(raw).expect("the response is text")
}

fn current_unix_second_ns() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();
    i64::try_from(now).expect("unix seconds fit in i64") * 1_000_000_000
}
