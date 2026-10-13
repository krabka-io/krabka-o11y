use super::*;

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
