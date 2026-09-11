//! Per-tenant limits, end to end: one provider, two tenants, two answers.

mod support;

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{
    LabelIndex, LogBlockIndex as BlockIndex, TenantId, labels, write_log_index_manifest,
};
use krabka_observability::{
    InMemoryWalSink, Limits, OverridesProvider, QuerierIndexSource, Role, ServiceConfig,
    ServiceDependencies, build_service_router, distributor_router_with_overrides, loki_router,
    serve_service_listener,
};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use serde_json::json;
use support::{
    assert_loki_error, current_unix_epoch_nanos, json_body, multi_tenant_fixture,
    proto_logs_request_at_ns, text_body,
};
use tokio::net::TcpListener;
use tower::ServiceExt as _;

/// The overrides file a tenant is capped by. `tenant-a` may read almost
/// nothing; `tenant-b` names no limit of its own and keeps the defaults.
const OVERRIDES: &str = r#"
overrides:
  tenant-a:
    max_query_read: "1B"
    max_line_size: "8B"
"#;

fn tenant_for_test(name: &str) -> TenantId {
    TenantId::new(name).expect("a valid tenant id")
}

fn provider() -> OverridesProvider {
    OverridesProvider::from_yaml(OVERRIDES).expect("the overrides file parses")
}

fn push_body(timestamp_ns: &str, line: &str) -> Body {
    Body::from(
        json!({
            "streams": [{
                "stream": {"app": "api"},
                "values": [[timestamp_ns, line]]
            }]
        })
        .to_string(),
    )
}

/// The milestone's headline: one router, one provider, two tenants, and the
/// capped tenant is refused the query the uncapped tenant is served. Nothing
/// about the request differs except the `X-Scope-OrgID` header.
#[tokio::test]
async fn a_per_tenant_override_changes_what_the_querier_serves() {
    let (state, _prod_bytes, _stage_bytes) = multi_tenant_fixture();
    let app = loki_router(state.with_limits_overrides(provider()));
    let uri = "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&start=0&end=30";

    let refused = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(refused.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(refused).await, "bad_data", "bytes");

    let served = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(served.status() == StatusCode::OK);
    let body = json_body(served).await;
    check!(body["status"] == "success");
    check!(
        body["data"]["result"][0]["values"][0][1] == "tenant-b api error",
        "the uncapped tenant still gets its line: {body}"
    );
}

/// The same provider on the write path. The capped tenant's line is refused
/// with `Loki`'s own message and never reaches the WAL; the uncapped
/// tenant's identical line is accepted.
#[tokio::test]
async fn a_per_tenant_override_changes_what_the_distributor_accepts() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router_with_overrides(sink.clone(), provider());
    let timestamp = current_unix_epoch_nanos().to_string();
    let line = "a line well over eight bytes";

    let refused = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(push_body(&timestamp, line))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(refused.status() == StatusCode::BAD_REQUEST);
    let message = text_body(refused).await;
    check!(
        message.contains("Max entry size '8' bytes exceeded"),
        "{message}"
    );

    let accepted = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-b")
                .header("content-type", "application/json")
                .body(push_body(&timestamp, line))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(accepted.status() == StatusCode::NO_CONTENT);

    let records = sink.records();
    check!(records.len() == 1, "only the uncapped tenant's line landed");
    check!(records[0].tenant == "tenant-b");
    check!(records[0].line == line);
}

/// A `defaults` block caps a tenant that has no entry of its own, and a
/// tenant's own entry still wins over it.
#[tokio::test]
async fn the_defaults_block_caps_a_tenant_with_no_entry_of_its_own() {
    let overrides = OverridesProvider::from_yaml(
        "defaults:\n  max_query_read: \"1B\"\noverrides:\n  tenant-b:\n    max_query_read: \"1GiB\"\n",
    )
    .expect("the overrides file parses");
    let (state, _prod_bytes, _stage_bytes) = multi_tenant_fixture();
    let app = loki_router(state.with_limits_overrides(overrides));
    let uri = "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&start=0&end=30";

    // `tenant-a` has no entry, so the defaults block applies to it.
    let refused = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(refused.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(refused).await, "bad_data", "bytes");

    // `tenant-b` names a limit of its own, which wins over the block.
    let served = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(served.status() == StatusCode::OK);
}

/// `--logs-limits-overrides-config` is what an operator actually sets, so
/// the file has to reach the router the service builds and not only the
/// provider a test constructs by hand.
#[tokio::test]
async fn the_overrides_config_flag_reaches_the_service_router() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api")]));
    label_index.insert_series("tenant-b", labels([("app", "api")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();

    let overrides_path = dir.join("logs-limits.yaml");
    std::fs::write(
        &overrides_path,
        "overrides:\n  tenant-a:\n    max_query_string_bytes: \"1B\"\n",
    )
    .unwrap();

    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        logs_limits_overrides_config: Some(overrides_path),
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();
    let uri = "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D";

    let refused = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(refused.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(refused).await, "bad_data", "query length");

    let served = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(served.status() == StatusCode::OK);
}

/// A file that does not parse stops the service rather than starting one
/// whose limits are not the ones the operator wrote.
#[tokio::test]
async fn an_unreadable_overrides_config_stops_the_service() {
    let dir = tempfile::tempdir().unwrap().keep();
    write_log_index_manifest(&dir, &LabelIndex::default(), &BlockIndex::default()).unwrap();
    let overrides_path = dir.join("logs-limits.yaml");
    std::fs::write(
        &overrides_path,
        "overrides:\n  tenant-a:\n    max_serie: 3\n",
    )
    .unwrap();

    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        logs_limits_overrides_config: Some(overrides_path),
        ..ServiceConfig::default()
    };
    let error = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .expect_err("a misspelled key stops the start");
    check!(
        error.to_string().contains("runtime overrides"),
        "the error names what it could not load: {error}"
    );
}

/// With no overrides file, the scalar flags are still the limits every
/// tenant gets.
#[tokio::test]
async fn the_scalar_limit_flags_cap_every_tenant_when_no_file_is_set() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api")]));
    label_index.insert_series("tenant-b", labels([("app", "api")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();

    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        max_query_string_bytes: Some(krabka_units::bytes(1)),
        ..ServiceConfig::default()
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    for tenant in ["tenant-a", "tenant-b"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D")
                    .header("X-Scope-OrgID", tenant)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status() == StatusCode::BAD_REQUEST, "{tenant}");
        assert_loki_error(&json_body(response).await, "bad_data", "query length");
    }
}

/// The gRPC OTLP export path used to build a state with every limit unset,
/// so it was the one ingest door with no body cap and no timestamp window.
/// It carries the service's provider now, and this holds it there.
#[tokio::test]
async fn the_grpc_otlp_export_is_body_limited_and_timestamp_windowed() {
    let sink = InMemoryWalSink::default();
    let dir = tempfile::tempdir().unwrap().keep();
    let overrides_path = dir.join("logs-limits.yaml");
    std::fs::write(
        &overrides_path,
        "overrides:\n  tenant-capped:\n    max_ingest_body: \"1B\"\n",
    )
    .unwrap();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        logs_limits_overrides_config: Some(overrides_path),
        ..ServiceConfig::default()
    };

    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_sink = sink.clone();
    let server = tokio::spawn(async move {
        serve_service_listener(
            listener,
            config,
            ServiceDependencies::default().with_wal_sink(server_sink),
            None,
        )
        .await
        .unwrap();
    });

    let mut client = LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let now_ns = u64::try_from(current_unix_epoch_nanos()).expect("a timestamp that fits");

    // The body cap: the capped tenant's export is over one byte.
    let mut request = tonic::Request::new(proto_logs_request_at_ns(now_ns));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-capped".parse().unwrap());
    let status = client
        .export(request)
        .await
        .expect_err("the export is over the body cap");
    check!(
        status.code() == tonic::Code::ResourceExhausted,
        "{status:?}"
    );

    // The timestamp window: an epoch-dated entry is older than the default
    // `reject_old_samples_max_age`, and this door applies it now.
    let mut request = tonic::Request::new(proto_logs_request_at_ns(19));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-open".parse().unwrap());
    let status = client
        .export(request)
        .await
        .expect_err("an entry at the epoch is outside the window");
    check!(status.code() == tonic::Code::InvalidArgument, "{status:?}");
    check!(
        status.message().contains("timestamp too old"),
        "{}",
        status.message()
    );

    // A recent entry from an uncapped tenant still goes through.
    let mut request = tonic::Request::new(proto_logs_request_at_ns(now_ns));
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-open".parse().unwrap());
    client.export(request).await.expect("a valid export");
    server.abort();

    let records = sink.records();
    check!(records.len() == 1, "only the valid export landed");
    check!(records[0].tenant == "tenant-open");
}

/// A limit the operator sets on the write path and a limit they set on the
/// read path come out of the same provider, so a process cannot cap a
/// tenant's ingest while leaving its reads on someone else's numbers.
#[tokio::test]
async fn one_provider_answers_both_the_write_and_the_read_gate() {
    let overrides = provider();
    check!(
        overrides
            .for_tenant(&tenant_for_test("tenant-a"))
            .max_line_size
            == krabka_units::bytes(8)
    );
    check!(
        overrides
            .for_tenant(&tenant_for_test("tenant-a"))
            .max_query_read
            == krabka_units::bytes(1)
    );
    check!(
        *overrides.for_tenant(&tenant_for_test("tenant-b")) == *overrides.defaults(),
        "a tenant with no entry gets the defaults on both paths"
    );
    check!(overrides.defaults().max_line_size == Limits::default().max_line_size);
}

/// `max_query_lookback` does not refuse a query, it moves its start
/// forward. A tenant capped to the last hour sees nothing from a block that
/// sits at the epoch, and an uncapped tenant reading the same query still
/// sees its line.
#[tokio::test]
async fn a_lookback_cap_moves_the_query_start_rather_than_refusing_the_query() {
    let overrides =
        OverridesProvider::from_yaml("overrides:\n  tenant-a:\n    max_query_lookback: \"1h\"\n")
            .expect("the overrides file parses");
    let (state, _prod_bytes, _stage_bytes) = multi_tenant_fixture();
    let app = loki_router(state.with_limits_overrides(overrides));
    let uri = "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&start=0&end=30";

    let clamped = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(clamped.status() == StatusCode::OK, "the query is answered");
    let body = json_body(clamped).await;
    check!(
        body["data"]["result"].as_array().is_some_and(Vec::is_empty),
        "the epoch-dated block is outside the lookback: {body}"
    );

    let served = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(served.status() == StatusCode::OK);
    let body = json_body(served).await;
    check!(
        body["data"]["result"][0]["values"][0][1] == "tenant-b api error",
        "the uncapped tenant reads the same block: {body}"
    );
}

/// `max_entries_limit_per_query` caps the `limit` a client asks for, and it
/// does so per tenant.
#[tokio::test]
async fn an_entries_limit_caps_the_limit_parameter_per_tenant() {
    let overrides = OverridesProvider::from_yaml(
        "overrides:\n  tenant-a:\n    max_entries_limit_per_query: 5\n",
    )
    .expect("the overrides file parses");
    let (state, _prod_bytes, _stage_bytes) = multi_tenant_fixture();
    let app = loki_router(state.with_limits_overrides(overrides));
    let uri = "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&start=0&end=30&limit=6";

    let refused = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(refused.status() == StatusCode::BAD_REQUEST);
    let message = text_body(refused).await;
    check!(
        message.contains("max entries limit per query exceeded"),
        "{message}"
    );

    let served = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(served.status() == StatusCode::OK);
}
