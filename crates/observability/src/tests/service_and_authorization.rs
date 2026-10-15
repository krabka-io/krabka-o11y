use axum::http::Extensions;
use clap::Parser as _;
use prost::Message as _;
use tower::ServiceExt as _;

use crate::{
    LogQueryAuthorizer as _,
    security_context::{MissingPrincipal, RequestSecurity, ServiceAudit},
    tests::prelude::{
        Arc, BTreeMap, BlockIndex, BrokerAccessPolicy, CONTENT_ENCODING, CONTENT_TYPE,
        CancellationToken, ClientResourcePolicy, DeferredQueryAuthorizerConnect, Duration,
        HeaderMap, LabelIndex, Limits, Mutex, ObjectStore, Principal, ProtoAnyValue,
        ProtoExportLogsServiceRequest, ProtoKeyValue, ProtoLogRecord, QueryAuthorizationError,
        Role, RoleReadiness, ServiceConfig, ServiceDependencies, StatusCode, TenantId,
        UnavailableQueryAuthorizer, Url, Value, WalLogRecord,
        build_compactor_configured_object_store, build_service_router_with_shutdown, check, json,
        normalize_otlp_http_logs, proto_any_value, sleep, write_log_index_manifest,
    },
};

mod recording_object_store;
pub(crate) use recording_object_store::RecordingObjectStore;

/// A request that came through no listener carries neither a principal nor a
/// peer address. While the service reads a credentials file, it must be
/// refused: serving it as unauthenticated would let a router that the service
/// serves without `serve_router` skip the credentials check. With no
/// credentials file, or no service audit context at all, as in an in-process
/// test router, it stays unauthenticated, which is the upstream posture. A
/// principal the authentication layer attached is used as it is.
#[test]
pub(crate) fn a_request_through_no_listener_fails_closed_when_authentication_is_required() {
    let with_audit = |authentication_required| {
        let mut extensions = Extensions::new();
        extensions.insert(ServiceAudit {
            authentication_required,
            ..ServiceAudit::disabled()
        });
        extensions
    };
    let principal_of = |extensions: &Extensions| {
        RequestSecurity::from_extensions(extensions).map(|security| security.principal)
    };

    check!(principal_of(&with_audit(true)) == Err(MissingPrincipal));
    check!(principal_of(&with_audit(false)) == Ok(Principal::Unauthenticated));
    check!(principal_of(&Extensions::new()) == Ok(Principal::Unauthenticated));

    let mut attached = with_audit(true);
    attached.insert(Principal::Unauthenticated);
    check!(principal_of(&attached) == Ok(Principal::Unauthenticated));
}

#[test]
pub(crate) fn service_readiness_requires_wal_and_authorization() {
    // A role that registers no precondition is ready: nothing about its
    // startup happens after its listener binds.
    assert2::assert!(RoleReadiness::new().is_ready());

    let readiness = RoleReadiness::new();
    let wal_tail = readiness.gate("wal-tail");
    let authorization = readiness.gate("query-authorization");
    assert2::assert!(!readiness.is_ready());
    assert2::assert!(readiness.pending() == ["wal-tail", "query-authorization"]);

    wal_tail.mark_ready();
    assert2::assert!(!readiness.is_ready());
    assert2::assert!(readiness.pending() == ["query-authorization"]);

    authorization.mark_ready();
    assert2::assert!(readiness.is_ready());

    // A background task that loses what it had takes the role out of
    // rotation again, without asking for a restart.
    wal_tail.mark_unready();
    assert2::assert!(!readiness.is_ready());
    assert2::assert!(readiness.pending() == ["wal-tail"]);
}

/// Sorting a Loki vector result orders it by sample value, and touches
/// nothing else: a matrix carries the same shape but must come back in the
/// order it arrived. Nothing had called this at all, so returning without
/// doing anything -- or sorting exactly the results it should not -- both
/// passed.
#[test]
pub(crate) fn sorting_a_loki_vector_result_orders_only_a_vector() {
    let sample = |value: &str| serde_json::json!({"metric": {"n": value}, "value": [0, value]});
    let order = |value: &serde_json::Value| {
        value
            .pointer("/data/result")
            .and_then(serde_json::Value::as_array)
            .expect("a result array")
            .iter()
            .map(|entry| {
                entry
                    .pointer("/metric/n")
                    .and_then(serde_json::Value::as_str)
                    .expect("a name")
                    .to_string()
            })
            .collect::<Vec<_>>()
    };

    let mut vector = serde_json::json!({
        "data": { "resultType": "vector", "result": [sample("3"), sample("1"), sample("2")] }
    });
    crate::tests::prelude::sort_loki_vector_result(
        &mut vector,
        crate::tests::prelude::SampleOrder::Ascending,
    );
    check!(order(&vector) == vec!["1", "2", "3"], "ascending");

    crate::tests::prelude::sort_loki_vector_result(
        &mut vector,
        crate::tests::prelude::SampleOrder::Descending,
    );
    check!(
        order(&vector) == vec!["3", "2", "1"],
        "descending reverses it"
    );

    // Same shape, different result type: left exactly as it came.
    let mut matrix = serde_json::json!({
        "data": { "resultType": "matrix", "result": [sample("3"), sample("1")] }
    });
    crate::tests::prelude::sort_loki_vector_result(
        &mut matrix,
        crate::tests::prelude::SampleOrder::Ascending,
    );
    check!(
        order(&matrix) == vec!["3", "1"],
        "a matrix is not reordered"
    );
}

struct RoleRequestCase {
    target: Role,
    method: &'static str,
    uri: &'static str,
}

/// The production path installs a broker-backed authorizer that fails closed
/// until it connects. The ruler routes and the delete-request routes must get
/// that authorizer too, and not the allow-all one an embedded router gets. A
/// broker that never answers therefore refuses a ruler call on the querier and
/// a delete call on the block builder, and `/ready` names the gate.
#[tokio::test]
pub(crate) async fn a_role_with_a_broker_fails_closed_on_rules_and_deletes_until_its_authorizer_connects()
 {
    let connect = DeferredQueryAuthorizerConnect {
        // Nothing listens on port 1, so the connect task never succeeds.
        bootstrap: "127.0.0.1:1".to_string(),
        topic: "__krabka_observability_logs_wal".to_string(),
        client_resource_policy: ClientResourcePolicy::default(),
        security: None,
        access_policy: BrokerAccessPolicy::for_config(&ServiceConfig::default())
            .expect("the default policy is valid"),
    };
    let unavailable = "query authorization check unavailable for tenant `tenant-a`: broker-backed query authorization is not connected";

    for case in [
        RoleRequestCase {
            target: Role::Querier,
            method: "GET",
            uri: "/loki/api/v1/rules",
        },
        RoleRequestCase {
            target: Role::Querier,
            method: "POST",
            uri: "/loki/api/v1/rules/default",
        },
        RoleRequestCase {
            target: Role::BlockBuilder,
            method: "POST",
            uri: "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D&start=1",
        },
        RoleRequestCase {
            target: Role::BlockBuilder,
            method: "GET",
            uri: "/loki/api/v1/delete",
        },
    ] {
        let data_root = tempfile::tempdir().expect("a temp dir");
        write_log_index_manifest(
            data_root.path(),
            &LabelIndex::default(),
            &BlockIndex::default(),
        )
        .expect("an empty manifest");
        let config = ServiceConfig {
            target: case.target,
            data_root: data_root.path().to_path_buf(),
            index_prefix: Some("observability/logs".to_string()),
            ..ServiceConfig::default()
        };
        let token = CancellationToken::new();
        let dependencies =
            ServiceDependencies::default().with_deferred_query_authorizer_connect(connect.clone());
        let service_router =
            build_service_router_with_shutdown(&config, dependencies, None, token.clone())
                .await
                .expect("the role builds");
        let router = service_router.router;
        let tasks = service_router.background_tasks;

        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(case.method)
                    .uri(case.uri)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(axum::body::Body::from("name: g\nrules: []\n"))
                    .expect("a request"),
            )
            .await
            .expect("a response");
        check!(
            response.status() == StatusCode::INTERNAL_SERVER_ERROR,
            "{} {}",
            case.method,
            case.uri
        );
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("a body");
        check!(
            serde_json::from_slice::<Value>(&body).ok()
                == Some(json!({
                    "status": "error",
                    "errorType": "server_error",
                    "error": unavailable,
                    "data": null,
                })),
            "{} {}",
            case.method,
            case.uri
        );

        let ready = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/ready")
                    .body(axum::body::Body::empty())
                    .expect("a request"),
            )
            .await
            .expect("a response");
        check!(
            ready.status() == StatusCode::SERVICE_UNAVAILABLE,
            "{:?}",
            case.target
        );
        check!(
            tasks.iter().any(|task| task.name == "query authorization"),
            "{:?}",
            case.target
        );

        token.cancel();
        for task in tasks {
            check!(task.handle.await.is_ok());
        }
    }
}

#[tokio::test]
pub(crate) async fn compactor_configured_object_store_builds_when_not_injected() {
    let object_store_dir = tempfile::tempdir().unwrap();
    let object_store_url = Url::from_directory_path(object_store_dir.path())
        .expect("temporary directory should be representable as a file URL")
        .to_string();
    let config = ServiceConfig::parse_from([
        "krabka-observability",
        "--target",
        "block-builder",
        "--object-store-url",
        &object_store_url,
    ]);

    let configured_store = build_compactor_configured_object_store(
        &config,
        None,
        krabka_blockstore::ObjectStoreMetrics::unregistered(),
    )
    .await
    .expect("valid object-store URL should configure a compactor store");

    assert!(
        configured_store.is_some(),
        "compactor should build the configured object store when no store is injected"
    );
}

/// The `OTLP`/HTTP logs handler must decompress `Content-Encoding: gzip`
/// before it protobuf-decodes. The OpenTelemetry SDK's `otlphttp` exporter,
/// which the demo's Alloy uses, gzips by default, so a regression here
/// means every emitted log line silently fails to decode, and no logs are
/// ingested.
#[test]
pub(crate) fn normalize_otlp_http_logs_decodes_gzip_identically_to_identity() {
    use std::io::Write as _;

    use opentelemetry_proto::tonic::{
        logs::v1::{ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };

    let request = ProtoExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![ProtoKeyValue {
                    key: "service.name".to_string(),
                    value: Some(ProtoAnyValue {
                        value: Some(proto_any_value::Value::StringValue("checkout".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![ProtoLogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(ProtoAnyValue {
                        value: Some(proto_any_value::Value::StringValue(
                            "hello world".to_string(),
                        )),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let raw = request.encode_to_vec();

    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

    // Identity (no Content-Encoding) decodes to a single record.
    let limits = Limits::unenforced();
    let tenant = TenantId::new("demo").expect("a valid tenant id");
    let identity = normalize_otlp_http_logs(&tenant, &headers, &raw, &limits)
        .expect("uncompressed OTLP proto logs should decode");
    check!(identity.len() == 1);
    check!(identity[0].line == "hello world");

    // The gzip-compressed body must decode to exactly the same records.
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&raw).unwrap();
    let gzipped = encoder.finish().unwrap();

    let mut gz_headers = headers.clone();
    gz_headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
    let from_gzip = normalize_otlp_http_logs(&tenant, &gz_headers, &gzipped, &limits)
        .expect("gzip-compressed OTLP proto logs should decode");
    check!(from_gzip == identity);
}

#[tokio::test]
pub(crate) async fn unavailable_query_authorizer_fails_closed() {
    let result = UnavailableQueryAuthorizer
        .check(
            &Principal::Unauthenticated,
            &TenantId::new("tenant-a").expect("a valid tenant id"),
        )
        .await;

    assert2::assert!(matches!(
        result,
        Err(QueryAuthorizationError::Unavailable { tenant, .. }) if tenant == "tenant-a"
    ));
}
