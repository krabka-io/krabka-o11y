use std::net::SocketAddr;

use assert2::check;
use axum::extract::ConnectInfo;
use krabka_audit::{AuditLog, AuditStats};
use krabka_observability::{
    audit::{
        AuditClocks, AuditEvent, AuditOutcome, EpochMs, MECHANISM_BEARER,
        OPERATION_RULE_GROUP_DELETE, OPERATION_RULE_GROUP_SET, OPERATION_RULE_NAMESPACE_DELETE,
        OPERATION_TENANT_ACCESS, RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT,
        admin_operation, principal, resource, source_endpoint, unknown_source_endpoint,
    },
    server_security::{ClientAuth, PeerAddr, ServerSecurityArgs},
};

use super::*;

const OPS_TOKEN: &str = "ops-token-5f1c9d2e7a4b8c3d6e0f1a2b3c4d5e6f";
const OPS_TOKEN_SHA256: &str = "fd66d679ee99eef3efa2ef4127680aea063ca35c6b394cacad365fdeec1203be";
const GROUP_YAML: &str =
    "name: latency\nrules:\n  - record: job:up:sum\n    expr: sum by (job) (up)\n";

fn client() -> SocketAddr {
    "10.0.0.7:51000".parse().expect("a valid socket address")
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    tenant: &str,
    body: &'static str,
) -> StatusCode {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {OPS_TOKEN}"))
        .header("X-Scope-OrgID", tenant)
        .header("Content-Type", "application/yaml")
        .body(Body::from(body))
        .expect("a valid request");
    request.extensions_mut().insert(ConnectInfo(PeerAddr {
        socket: client(),
        client_identity: None,
    }));
    app.clone()
        .oneshot(request)
        .await
        .expect("the router answers")
        .status()
}

/// The event with its time set to zero, so a test can compare whole events.
fn at_time_zero(event: AuditEvent) -> AuditEvent {
    match event {
        AuditEvent::AdminOperation {
            outcome,
            principal,
            source,
            operation,
            resources,
            ..
        } => AuditEvent::AdminOperation {
            outcome,
            principal,
            source,
            operation,
            resources,
            time_ms: 0,
        },
        AuditEvent::AuthorizationDenied {
            principal,
            source,
            resource_type,
            resource_name,
            operation,
            ..
        } => AuditEvent::AuthorizationDenied {
            principal,
            source,
            resource_type,
            resource_name,
            operation,
            time_ms: 0,
        },
        other => other,
    }
}

/// Each ruler config mutation records one admin operation with its principal,
/// its client address, the tenant, namespace and group it changed, and whether
/// it succeeded. A read records nothing. A principal refused the tenant is
/// recorded once, by the server's security events, and not again as an
/// operation.
#[tokio::test]
pub(crate) async fn ruler_config_mutations_are_audited() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let credentials = directory.path().join("credentials.yaml");
    std::fs::write(
        &credentials,
        format!(
            "principals:\n  - name: ops\n    token_sha256: [\"{OPS_TOKEN_SHA256}\"]\n    tenants: [\"tenant-a\"]\n"
        ),
    )
    .expect("the credentials file is writable");
    let (log, mut events) = AuditLog::new(64);
    let audit = AuditHandle::new(
        log,
        Arc::new(AuditStats::new()),
        AuditClocks::system().clock,
    );
    let security = ServerSecurityArgs {
        server_tls_cert_path: None,
        server_tls_key_path: None,
        server_tls_client_ca_path: None,
        server_tls_client_auth: ClientAuth::NoClientCert,
        server_tls_handshake_timeout: secs(10),
        auth_credentials_config: Some(credentials),
        internal_client_token_path: None,
        internal_client_tls_cert_path: None,
        internal_client_tls_key_path: None,
        internal_client_tls_ca_path: None,
    }
    .load()
    .expect("the credentials load")
    .with_security_events(Arc::new(audit.clone()));
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_audit(audit),
    );
    let app = authenticate_requests(super::super::prometheus_router(state), &security);

    let statuses = [
        send(
            &app,
            "POST",
            "/prometheus/config/v1/rules/team",
            "tenant-a",
            GROUP_YAML,
        )
        .await,
        send(
            &app,
            "POST",
            "/prometheus/config/v1/rules/team",
            "tenant-a",
            "name: [",
        )
        .await,
        send(
            &app,
            "GET",
            "/prometheus/config/v1/rules/team",
            "tenant-a",
            "",
        )
        .await,
        send(
            &app,
            "DELETE",
            "/prometheus/config/v1/rules/team/latency",
            "tenant-a",
            "",
        )
        .await,
        send(
            &app,
            "DELETE",
            "/prometheus/config/v1/rules/team",
            "tenant-a",
            "",
        )
        .await,
        send(
            &app,
            "DELETE",
            "/prometheus/config/v1/rules/team",
            "tenant-b",
            "",
        )
        .await,
    ];
    let mut recorded = Vec::new();
    while let Ok(event) = events.try_recv() {
        recorded.push(at_time_zero(event));
    }

    check!(
        statuses
            == [
                StatusCode::ACCEPTED,
                StatusCode::BAD_REQUEST,
                StatusCode::OK,
                StatusCode::ACCEPTED,
                StatusCode::ACCEPTED,
                StatusCode::FORBIDDEN,
            ]
    );
    let ops = principal("ops", MECHANISM_BEARER);
    let tenant_a = resource(RESOURCE_TENANT, "tenant-a");
    let namespace = resource(RESOURCE_RULE_NAMESPACE, "team");
    let group = resource(RESOURCE_RULE_GROUP, "team/latency");
    check!(
        recorded
            == vec![
                admin_operation(
                    ops.clone(),
                    source_endpoint(client()),
                    OPERATION_RULE_GROUP_SET,
                    vec![tenant_a.clone(), namespace.clone(), group.clone()],
                    AuditOutcome::Success,
                    EpochMs(0),
                ),
                admin_operation(
                    ops.clone(),
                    source_endpoint(client()),
                    OPERATION_RULE_GROUP_SET,
                    vec![tenant_a.clone(), namespace.clone()],
                    AuditOutcome::Failure,
                    EpochMs(0),
                ),
                admin_operation(
                    ops.clone(),
                    source_endpoint(client()),
                    OPERATION_RULE_GROUP_DELETE,
                    vec![tenant_a.clone(), namespace.clone(), group],
                    AuditOutcome::Success,
                    EpochMs(0),
                ),
                admin_operation(
                    ops.clone(),
                    source_endpoint(client()),
                    OPERATION_RULE_NAMESPACE_DELETE,
                    vec![tenant_a, namespace],
                    AuditOutcome::Success,
                    EpochMs(0),
                ),
                AuditEvent::AuthorizationDenied {
                    principal: ops,
                    source: unknown_source_endpoint(),
                    resource_type: RESOURCE_TENANT.to_owned(),
                    resource_name: "tenant-b".to_owned(),
                    operation: OPERATION_TENANT_ACCESS.to_owned(),
                    time_ms: 0,
                },
            ]
    );
}
