//! The service router and its listener: the routes each role serves, and the limits it applies.

mod support;

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use assert2::{assert, check};
use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogLabels, LogRow, TimeRange, labels,
    write_log_block, write_log_index_manifest,
};
use krabka_observability::{
    CompactionFrontier, InMemoryWalSink, KafkaWalRecord, LogWalConsumer, LogWalSink, Offset,
    PartitionIndex, QuerierState, Role, ServiceConfig, ServiceDependencies, WalConsumerError,
    WalLogRecord, WalPosition, WalSinkError, build_service_router, loki_router,
    serve_service_listener, write_compaction_frontier_to_object_store,
};
use krabka_units::{Time, bytes, millis, nanos};
use object_store::path::Path as ObjectPath;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use prost::Message as _;
use serde_json::{Value, json};
use snap::raw::Encoder as SnappyEncoder;
use support::{
    BodyHeaders, DenyingQueryAuthorizer, ExpectedLokiError, JsonStream, LokiProtoEntry,
    LokiProtoPushRequest, LokiProtoStream, LokiStatsCounts, PushOutcome, RejectingIngestLimiter,
    Tenant, assert_loki_error, current_unix_epoch_nanos, expected_api_error,
    expected_loki_forwarded_api_error, expected_loki_forwarded_api_error_with_stats, json_body,
    kafka_wal_record, loki_forwarded_tenant_object_store_shard_catalog_service_fixture,
    minimal_service_config, proto_logs_request_at_ns, push_request, send, tenant_a_post,
    tenant_object_store_shard_catalog_service_fixture, text_body,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
    time::{Duration, timeout},
};
use tower::ServiceExt as _;

const API_PROD: [(&str, &str); 2] = [("app", "api"), ("env", "prod")];

#[derive(Clone)]
struct PendingWalSink;

#[async_trait]
impl LogWalSink for PendingWalSink {
    async fn append(&self, _record: WalLogRecord) -> Result<(), WalSinkError> {
        std::future::pending().await
    }
}

async fn get_response(app: axum::Router, uri: &str) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn post_form_response(
    app: axum::Router,
    uri: &str,
    body: &'static str,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
}

fn assert_content_type(response: &axum::response::Response, expected: &str, context: &str) {
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some(expected),
        "{context} content-type"
    );
}

async fn router(config: &ServiceConfig, dependencies: ServiceDependencies) -> Router {
    build_service_router(config, dependencies, None)
        .await
        .unwrap()
}

/// A distributor, a querier and a block builder, each with no state.
async fn role_routers() -> [(&'static str, Router); 3] {
    let distributor = router(
        &minimal_service_config(Role::Distributor),
        ServiceDependencies::default().with_wal_sink(InMemoryWalSink::default()),
    )
    .await;
    let querier = loki_router(QuerierState::new(
        ".",
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let compactor = router(
        &minimal_service_config(Role::BlockBuilder),
        ServiceDependencies::default(),
    )
    .await;
    [
        ("distributor", distributor),
        ("querier", querier),
        ("block-builder", compactor),
    ]
}

#[tokio::test]
async fn role_operations_routes_match_existing_behavior() {
    for (name, app) in role_routers().await {
        let response = get_response(app.clone(), "/ready").await;
        assert!(response.status() == StatusCode::OK, "{name} /ready status");
        assert_content_type(
            &response,
            "text/plain; charset=utf-8",
            &format!("{name} /ready"),
        );
        assert!(text_body(response).await == "ready\n", "{name} /ready body");

        let response = get_response(app.clone(), "/config").await;
        assert!(response.status() == StatusCode::OK, "{name} /config status");
        assert_content_type(
            &response,
            "application/yaml; charset=utf-8",
            &format!("{name} /config"),
        );
        assert!(
            text_body(response).await == "target: all\nauth_enabled: true\n",
            "{name} /config body"
        );

        let response = get_response(app.clone(), "/config?mode=defaults").await;
        assert!(
            response.status() == StatusCode::OK,
            "{name} /config?mode=defaults status"
        );
        assert_content_type(
            &response,
            "application/yaml; charset=utf-8",
            &format!("{name} /config?mode=defaults"),
        );
        assert!(
            text_body(response).await == "target: all\nauth_enabled: true\n",
            "{name} /config?mode=defaults body"
        );

        let response = get_response(app.clone(), "/config?mode=diff").await;
        assert!(
            response.status() == StatusCode::INTERNAL_SERVER_ERROR,
            "{name} /config?mode=diff status"
        );
        assert_content_type(
            &response,
            "text/plain; charset=utf-8",
            &format!("{name} /config?mode=diff"),
        );
        assert!(
            text_body(response).await == "unsupported type <nil>\n",
            "{name} /config?mode=diff body"
        );

        let response = get_response(app.clone(), "/services").await;
        assert!(
            response.status() == StatusCode::OK,
            "{name} /services status"
        );
        assert_content_type(
            &response,
            "text/plain; charset=utf-8",
            &format!("{name} /services"),
        );
        assert!(
            text_body(response).await
                == "query-scheduler => Running\n\
                    ingester-querier => Running\n\
                    query-frontend => Running\n\
                    server => Running\n\
                    querier => Running\n\
                    rule-evaluator => Running\n\
                    memberlist-kv => Running\n\
                    query-frontend-tripperware => Running\n\
                    pattern-ingester => Running\n\
                    pattern-ingester-tee => Running\n\
                    pattern-ring-client => Running\n\
                    ruler => Running\n\
                    cache-generation-loader => Running\n\
                    store => Running\n\
                    ring => Running\n\
                    ingester => Running\n\
                    compactor => Running\n\
                    distributor => Running\n\
                    query-scheduler-ring => Running\n",
            "{name} /services body"
        );

        let response = get_response(app.clone(), "/memberlist").await;
        assert!(
            response.status() == StatusCode::OK,
            "{name} /memberlist status"
        );
        assert_content_type(&response, "text/plain", &format!("{name} /memberlist"));
        assert!(
            text_body(response).await == "This instance doesn't use memberlist.",
            "{name} /memberlist body"
        );

        let response = get_response(app.clone(), "/metrics").await;
        assert!(
            response.status() == StatusCode::OK,
            "{name} /metrics status"
        );
        assert_content_type(
            &response,
            "text/plain; version=0.0.4; charset=utf-8",
            &format!("{name} /metrics"),
        );
        assert!(
            text_body(response).await.contains("# HELP"),
            "{name} /metrics body"
        );

        let response = get_response(app.clone(), "/loki/api/v1/status/buildinfo").await;
        assert!(
            response.status() == StatusCode::OK,
            "{name} buildinfo status"
        );
        assert_content_type(&response, "application/json", &format!("{name} buildinfo"));
        assert!(
            text_body(response).await
                == format!(
                    "{{\"version\":\"{}\",\"revision\":\"unknown\",\"branch\":\"unknown\",\"buildDate\":\"\",\"buildUser\":\"krabka\",\"goVersion\":\"not-go\"}}",
                    env!("CARGO_PKG_VERSION")
                ),
            "{name} buildinfo body"
        );

        let response = post_form_response(app.clone(), "/log_level", "log_level=verbose").await;
        assert!(
            response.status() == StatusCode::BAD_REQUEST,
            "{name} invalid log_level status"
        );
        assert_content_type(
            &response,
            "application/json",
            &format!("{name} invalid log_level"),
        );
        assert!(
            text_body(response).await
                == "{\"status\":\"failed\",\"message\":\"unrecognized log level \\\"verbose\\\"\"}",
            "{name} invalid log_level body"
        );
    }
}

#[tokio::test]
async fn role_ring_alias_routes_remain_available() {
    let [(_, distributor), (_, querier), (_, compactor)] = role_routers().await;

    for (app, uri, expected) in [
        (distributor.clone(), "/ring", "krabka-distributor"),
        (distributor, "/distributor/ring", "krabka-distributor"),
        (querier.clone(), "/ring", "krabka-querier"),
        (querier.clone(), "/scheduler/ring", "krabka-scheduler"),
        (querier, "/ruler/ring", "Cortex Ruler Status"),
        (compactor.clone(), "/ring", "krabka-compactor"),
        (compactor, "/compactor/ring", "krabka-compactor"),
    ] {
        let response = get_response(app, uri).await;
        assert!(response.status() == StatusCode::OK, "{uri} status");
        assert_content_type(&response, "text/html; charset=utf-8", uri);
        assert!(text_body(response).await.contains(expected), "{uri} body");
    }
}

/// What one push through a configured distributor did: the response status
/// and body, and the records its WAL sink holds afterwards.
async fn push_through(
    config: &ServiceConfig,
    dependencies: impl FnOnce(ServiceDependencies) -> ServiceDependencies,
    request: Request<Body>,
) -> PushOutcome {
    let sink = InMemoryWalSink::default();
    let app = router(
        config,
        dependencies(ServiceDependencies::default().with_wal_sink(sink.clone())),
    )
    .await;
    let response = send(&app, request).await;
    PushOutcome {
        status: response.status(),
        body: text_body(response).await,
        records: sink.records(),
    }
}

async fn push_json_to_distributor(
    config: &ServiceConfig,
    dependencies: impl FnOnce(ServiceDependencies) -> ServiceDependencies,
    payload: &Value,
) -> PushOutcome {
    push_through(
        config,
        dependencies,
        push_request(
            "/loki/api/v1/push",
            BodyHeaders {
                content_type: "application/json",
                content_encoding: None,
            },
            payload.to_string(),
        ),
    )
    .await
}

fn check_one_tenant_a_api_error(records: &[WalLogRecord]) {
    check!(records.len() == 1);
    check!(records[0].tenant == "tenant-a");
    check!(records[0].line == "api error");
}

fn now_second() -> String {
    current_unix_second_ns().to_string()
}

fn fifteen_minutes_ahead() -> String {
    (current_unix_second_ns() + 15 * 60 * 1_000_000_000).to_string()
}

#[tokio::test]
async fn service_router_builds_distributor_role() {
    let PushOutcome {
        status, records, ..
    } = push_json_to_distributor(
        &minimal_service_config(Role::Distributor),
        |dependencies| dependencies,
        &JsonStream {
            stream: json!(labels(API_PROD)),
            values: json!([[&now_second(), "api error"]]),
        }
        .payload(),
    )
    .await;

    assert!(status == StatusCode::NO_CONTENT);
    check_one_tenant_a_api_error(&records);
}

#[tokio::test]
async fn service_router_rejects_stale_loki_push_timestamp_without_wal_append() {
    let outcome = push_json_to_distributor(
        &minimal_service_config(Role::Distributor),
        |dependencies| dependencies,
        &JsonStream {
            stream: json!(labels([("app", "api")])),
            values: json!([["1000000000", "stale api error"]]),
        }
        .payload(),
    )
    .await;

    outcome.rejected_containing(&["timestamp too old", r#"{app="api", service_name="api"}"#]);
}

#[tokio::test]
async fn service_router_rejects_missing_protobuf_timestamp_like_loki_without_wal_append() {
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: None,
                line: "missing protobuf timestamp".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let outcome = push_through(
        &minimal_service_config(Role::Distributor),
        |dependencies| dependencies,
        push_request(
            "/loki/api/v1/push",
            BodyHeaders {
                content_type: "application/x-protobuf",
                content_encoding: None,
            },
            payload,
        ),
    )
    .await;

    outcome.rejected_containing(&[
        "timestamp too old",
        "0001-01-01T00:00:00Z",
        r#"{app="api", service_name="api"}"#,
    ]);
}

#[tokio::test]
async fn service_router_rejects_future_loki_push_timestamp_without_wal_append() {
    let outcome = push_json_to_distributor(
        &minimal_service_config(Role::Distributor),
        |dependencies| dependencies,
        &JsonStream {
            stream: json!(labels([("app", "api")])),
            values: json!([[&fifteen_minutes_ahead(), "future api error"]]),
        }
        .payload(),
    )
    .await;

    outcome.rejected_containing(&["timestamp too new", r#"{app="api", service_name="api"}"#]);
}

#[tokio::test]
async fn service_router_rejects_future_otlp_timestamp_without_wal_append() {
    let outcome = push_through(
        &minimal_service_config(Role::Distributor),
        |dependencies| dependencies,
        push_request(
            "/v1/logs",
            BodyHeaders {
                content_type: "application/json",
                content_encoding: None,
            },
            json!({
                "resourceLogs": [
                    {
                        "resource": {
                            "attributes": [
                                {"key": "service.name", "value": {"stringValue": "checkout"}}
                            ]
                        },
                        "scopeLogs": [
                            {
                                "logRecords": [
                                    {
                                        "timeUnixNano": fifteen_minutes_ahead(),
                                        "body": {"stringValue": "future otlp error"}
                                    }
                                ]
                            }
                        ]
                    }
                ]
            })
            .to_string(),
        ),
    )
    .await;

    outcome.rejected_containing(&["timestamp too new", r#"{service_name="checkout"}"#]);
}

#[tokio::test]
async fn service_router_rejects_loki_push_over_configured_ingest_body_limit_without_wal_append() {
    let config = ServiceConfig {
        max_ingest_body: Some(bytes(1)),
        ..minimal_service_config(Role::Distributor)
    };

    let outcome = push_json_to_distributor(
        &config,
        |dependencies| dependencies,
        &JsonStream {
            stream: json!(labels(API_PROD)),
            values: json!([["19", "api error"]]),
        }
        .payload(),
    )
    .await;

    outcome.rejected_as_loki_error(&ExpectedLokiError {
        status: StatusCode::TOO_MANY_REQUESTS,
        error_type: "rate_limited",
        contains: "ingest body",
    });
}

#[tokio::test]
async fn service_router_rejects_loki_push_over_ingest_quota_without_wal_append() {
    let outcome = push_json_to_distributor(
        &minimal_service_config(Role::Distributor),
        |dependencies| dependencies.with_ingest_limiter(RejectingIngestLimiter),
        &JsonStream {
            stream: json!(labels(API_PROD)),
            values: json!([[&now_second(), "api error"]]),
        }
        .payload(),
    )
    .await;

    outcome.rejected_as_loki_error(&ExpectedLokiError {
        status: StatusCode::TOO_MANY_REQUESTS,
        error_type: "rate_limited",
        contains: "tenant write quota exceeded",
    });
}

#[tokio::test]
async fn service_router_times_out_loki_push_when_wal_append_stalls() {
    let config = ServiceConfig {
        wal_append_timeout: Some(millis(1)),
        ..minimal_service_config(Role::Distributor)
    };
    let app = router(
        &config,
        ServiceDependencies::default().with_wal_sink(PendingWalSink),
    )
    .await;

    let response = send(
        &app,
        tenant_a_post("/loki/api/v1/push")
            .header("content-type", "application/json")
            .body(Body::from(
                JsonStream {
                    stream: json!(labels(API_PROD)),
                    values: json!([[&now_second(), "api timeout error"]]),
                }
                .payload()
                .to_string(),
            ))
            .unwrap(),
    )
    .await;

    assert!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    assert_loki_error(
        &json_body(response).await,
        "server_error",
        "wal append timed out",
    );
}

/// Serves a distributor that appends to `sink` on a bound loopback listener.
async fn spawn_distributor_listener(
    sink: InMemoryWalSink,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        serve_service_listener(
            listener,
            minimal_service_config(Role::Distributor),
            ServiceDependencies::default().with_wal_sink(sink),
            None,
        )
        .await
        .unwrap();
    });
    (addr, server)
}

#[tokio::test]
async fn service_listener_serves_distributor_role_on_bound_tcp_listener() {
    let sink = InMemoryWalSink::default();
    let (addr, server) = spawn_distributor_listener(sink.clone()).await;

    let payload = JsonStream {
        stream: json!(labels(API_PROD)),
        values: json!([[&now_second(), "api error"]]),
    }
    .payload()
    .to_string();
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /loki/api/v1/push HTTP/1.1\r\nHost: {addr}\r\nX-Scope-OrgID: tenant-a\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    server.abort();

    check!(response.starts_with("HTTP/1.1 204 No Content"));
    check_one_tenant_a_api_error(&sink.records());
}

#[tokio::test]
async fn service_listener_serves_otlp_grpc_logs_for_distributor_role() {
    let sink = InMemoryWalSink::default();
    let (addr, server) = spawn_distributor_listener(sink.clone()).await;

    let mut client = LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    // Dated to now: the gRPC export path carries the service's limits, so an
    // entry at the epoch is outside `reject_old_samples_max_age` and refused.
    let now_ns = u64::try_from(current_unix_epoch_nanos()).expect("a timestamp that fits");
    let mut request = tonic::Request::new(proto_logs_request_at_ns(now_ns));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());

    let response = client.export(request).await.unwrap();
    server.abort();

    check!(response.get_ref().partial_success.is_none());
    check_one_tenant_a_api_error(&sink.records());
}

const ERROR_QUERY_RANGE: &str = "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=30.000000000";

#[tokio::test]
async fn service_router_builds_querier_role_from_object_store_shard_catalog_config() {
    let (config, store, _dir) =
        loki_forwarded_tenant_object_store_shard_catalog_service_fixture().await;
    let app = build_service_router(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap();

    let (status, body) = Tenant("tenant-a").get_json(&app, ERROR_QUERY_RANGE).await;

    assert!(status == StatusCode::OK);
    assert!(body == expected_loki_forwarded_api_error());
}

/// A kept data root whose local manifest names `series` for tenant-a and no
/// blocks.
fn local_manifest_root(series: Vec<LogLabels>) -> std::path::PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    for series_labels in series {
        label_index.insert_series("tenant-a", series_labels);
    }
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();
    dir
}

fn local_querier_config(data_root: std::path::PathBuf) -> ServiceConfig {
    ServiceConfig {
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root,
        ..minimal_service_config(Role::Querier)
    }
}

async fn assert_query_refused(app: &axum::Router, uri: &str, expected: &ExpectedLokiError<'_>) {
    let (status, body) = Tenant("tenant-a").get_json(app, uri).await;

    assert!(status == expected.status);
    assert_loki_error(&body, expected.error_type, expected.contains);
}

#[tokio::test]
async fn service_router_applies_query_authorizer_dependency_to_querier_role() {
    let config = ServiceConfig {
        wal_group_id: "krabka-observability-querier".to_string(),
        ..local_querier_config(local_manifest_root(vec![labels(API_PROD)]))
    };

    assert_query_refused(
        &router(
            &config,
            ServiceDependencies::default().with_query_authorizer(DenyingQueryAuthorizer),
        )
        .await,
        "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D",
        &ExpectedLokiError {
            status: StatusCode::FORBIDDEN,
            error_type: "forbidden",
            contains: "tenant read ACL denied",
        },
    )
    .await;
}

#[tokio::test]
async fn service_router_builds_querier_role_with_hot_tail_dependency() {
    let data_root = local_manifest_root(vec![labels(API_PROD)]);

    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            position: Some(WalPosition {
                partition: PartitionIndex(0),
                offset: Offset(42),
            }),
            ..api_error_at(19_000_000_000)
        })
        .await
        .unwrap();

    let app = router(
        &local_querier_config(data_root),
        ServiceDependencies::default().with_hot_tail_frontier(hot_tail, CompactionFrontier::new(0)),
    )
    .await;

    let (status, body) = Tenant("tenant-a").get_json(&app, ERROR_QUERY_RANGE).await;

    assert!(status == StatusCode::OK);
    assert!(
        body == expected_loki_forwarded_api_error_with_stats(
            &LokiStatsCounts {
                ingester_lines: 1,
                ..LokiStatsCounts::default()
            }
            .expected_stats()
        )
    );
}

#[tokio::test]
async fn service_router_applies_configured_query_range_limit() {
    let config = ServiceConfig {
        max_query_range: Some(nanos(20)),
        ..local_querier_config(local_manifest_root(vec![labels(API_PROD)]))
    };

    assert_query_refused(
        &router(&config, ServiceDependencies::default()).await,
        "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=0.000000030",
        &ExpectedLokiError {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            contains: "query range",
        },
    )
    .await;
}

#[tokio::test]
async fn service_router_applies_configured_query_length_limit() {
    let config = ServiceConfig {
        max_query_string_bytes: Some(bytes(10)),
        ..local_querier_config(local_manifest_root(vec![labels(API_PROD)]))
    };

    assert_query_refused(
        &router(&config, ServiceDependencies::default()).await,
        "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D",
        &ExpectedLokiError {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            contains: "query length",
        },
    )
    .await;
}

#[tokio::test]
async fn service_router_applies_configured_query_series_limit() {
    let config = ServiceConfig {
        max_query_series: Some(1),
        ..local_querier_config(local_manifest_root(vec![
            labels(API_PROD),
            labels([("app", "worker"), ("env", "prod")]),
        ]))
    };

    assert_query_refused(
        &router(&config, ServiceDependencies::default()).await,
        "/loki/api/v1/query_range?query=%7Benv%3D%22prod%22%7D&start=0.000000000&end=0.000000030",
        &ExpectedLokiError {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            contains: "series",
        },
    )
    .await;
}

#[tokio::test]
async fn service_router_applies_configured_query_bytes_limit() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels(API_PROD));
    let mut block_index = BlockIndex::default();
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 10, "api ok", BTreeMap::new())],
    )
    .unwrap();
    block_index.insert(api_block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let config = ServiceConfig {
        max_query_read: Some(bytes(1)),
        ..local_querier_config(dir)
    };

    assert_query_refused(
        &router(&config, ServiceDependencies::default()).await,
        "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=0.000000030",
        &ExpectedLokiError {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            contains: "bytes",
        },
    )
    .await;
}

fn api_error_at(timestamp_ns: i64) -> WalLogRecord {
    WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels(API_PROD),
        timestamp_ns,
        line: "api error".to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}

#[tokio::test]
async fn service_router_builds_querier_role_with_wal_consumer_hot_tail_poller() {
    let data_root = local_manifest_root(vec![labels(API_PROD)]);
    let consumer = RecordingWalConsumer::new(vec![vec![kafka_wal_record(
        &api_error_at(19_000_000_000),
        PartitionIndex(0),
        Offset(42),
    )]]);
    let app = router(
        &local_querier_config(data_root),
        ServiceDependencies::default().with_wal_consumer(consumer),
    )
    .await;

    let body = timeout(Duration::from_millis(500), async {
        loop {
            let (status, body) = Tenant("tenant-a").get_json(&app, ERROR_QUERY_RANGE).await;

            assert!(status == StatusCode::OK);
            if body
                == expected_loki_forwarded_api_error_with_stats(
                    &LokiStatsCounts {
                        ingester_lines: 1,
                        ..LokiStatsCounts::default()
                    }
                    .expected_stats(),
                )
            {
                break body;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(
        body == expected_loki_forwarded_api_error_with_stats(
            &LokiStatsCounts {
                ingester_lines: 1,
                ..LokiStatsCounts::default()
            }
            .expected_stats()
        )
    );
}

#[tokio::test]
async fn service_router_loads_persisted_frontier_for_configured_querier_hot_tail() {
    let (config, store, _dir) = tenant_object_store_shard_catalog_service_fixture().await;
    write_compaction_frontier_to_object_store(
        &store,
        &ObjectPath::from("indexes"),
        &CompactionFrontier::new(i64::MIN).with_partition_offset(PartitionIndex(0), Offset(43)),
    )
    .await
    .unwrap();
    let poll_count = Arc::new(AtomicUsize::new(0));
    let consumer = RecordingWalConsumer::new(vec![vec![kafka_wal_record(
        &api_error_at(19),
        PartitionIndex(0),
        Offset(43),
    )]])
    .with_poll_count(poll_count.clone());
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_consumer(consumer),
        Some(&store),
    )
    .await
    .unwrap();

    timeout(Duration::from_millis(500), async {
        while poll_count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // real-time wait (not a progress poll): bare settle after the WAL consumer's first
    // poll, before issuing the query; no cheap synchronous observable to poll on here.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let (status, body) = Tenant("tenant-a").get_json(&app, "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0.000000000&end=0.000000030")
    .await;

    assert!(status == StatusCode::OK);
    assert!(body == expected_api_error());
}

#[tokio::test]
async fn service_router_builds_configured_local_object_store_for_querier_role() {
    let (mut config, _store, dir) =
        loki_forwarded_tenant_object_store_shard_catalog_service_fixture().await;
    config.object_store_url = Some(format!("file://{}", dir.display()));
    let app = router(&config, ServiceDependencies::default()).await;

    let (status, body) = Tenant("tenant-a").get_json(&app, ERROR_QUERY_RANGE).await;

    assert!(status == StatusCode::OK);
    assert!(body == expected_loki_forwarded_api_error());
}

struct RecordingWalConsumer {
    batches: Vec<Vec<KafkaWalRecord>>,
    poll_count: Option<Arc<AtomicUsize>>,
}

impl RecordingWalConsumer {
    fn new(batches: Vec<Vec<KafkaWalRecord>>) -> Self {
        Self {
            batches,
            poll_count: None,
        }
    }

    fn with_poll_count(mut self, poll_count: Arc<AtomicUsize>) -> Self {
        self.poll_count = Some(poll_count);
        self
    }
}

#[async_trait]
impl LogWalConsumer for RecordingWalConsumer {
    async fn poll(&mut self, _timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        if let Some(poll_count) = &self.poll_count {
            poll_count.fetch_add(1, Ordering::SeqCst);
        }
        if self.batches.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(self.batches.remove(0))
        }
    }

    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        Ok(())
    }
}

fn current_unix_second_ns() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();
    i64::try_from(now).expect("unix seconds fit in i64") * 1_000_000_000
}

/// One router, both halves of the surface.
///
/// `--target all` merges the distributor's write routes and the querier's read
/// routes onto one listener, and each role also contributes an ops surface --
/// `/ready`, `/config`, `/services` and the ring pages. Merged without care
/// those ops routes collide and `axum` panics at construction, so a router
/// that builds at all is already saying something. What it has to say as well
/// is that neither half was lost: a `--target all` serving only the query
/// routes would take no push, and one serving only the push routes would
/// answer no query, and both would pass a probe.
#[tokio::test]
async fn the_all_in_one_router_serves_the_write_and_read_surfaces_together() {
    // A real directory with a manifest in it: the querier half reads its local
    // index from `--data-root` as it constructs its routes, and keeps its
    // delete-request store and its rules there too.
    let data_root = tempfile::tempdir().expect("data root");
    write_log_index_manifest(
        data_root.path(),
        &LabelIndex::default(),
        &BlockIndex::default(),
    )
    .expect("an empty local manifest");
    let mut config = minimal_service_config(Role::All);
    config.data_root = data_root.path().to_path_buf();
    config.index_prefix = Some("logs".to_string());
    let app = router(
        &config,
        ServiceDependencies::default().with_wal_sink(InMemoryWalSink::default()),
    )
    .await;

    let response = get_response(app.clone(), "/ready").await;
    assert!(response.status() == StatusCode::OK);
    assert!(text_body(response).await == "ready\n");

    // `Loki` reports `all` from a process that runs every module, and a
    // runbook pointed at this one should read the same.
    let response = get_response(app.clone(), "/config").await;
    assert!(text_body(response).await.contains("target: all"));

    let pushed = send(
        &app,
        tenant_a_post("/loki/api/v1/push")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"streams":[{"stream":{"app":"api"},"values":[["1","hello"]]}]}"#,
            ))
            .unwrap(),
    )
    .await;
    assert!(pushed.status() != StatusCode::NOT_FOUND);

    let queried = get_response(
        app.clone(),
        "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=0.000000010",
    )
    .await;
    assert!(queried.status() != StatusCode::NOT_FOUND);

    // The block builder's half of the surface. `Loki` serves delete requests
    // from the role that writes durable storage, and so does this.
    let deletes = get_response(app, "/loki/api/v1/delete").await;
    assert!(deletes.status() != StatusCode::NOT_FOUND);
}
