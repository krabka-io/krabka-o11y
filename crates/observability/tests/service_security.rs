//! The security of the logs service over its real listener: TLS, credentials,
//! tenant grants, admin routes, and the audit trail.
//!
//! Every test binds `127.0.0.1:0` and talks to the listener with a real
//! client. The certificates come from `rcgen`, and the credentials file is
//! written to a temporary directory, so no key material is in the repository.
//! The audit tests read the records from a `krabka_audit::MemorySink` behind a
//! mock clock, and compare them with the records of the expected events.

#[path = "support/secure_router.rs"]
mod secure_router;
#[path = "support/server_security_pki.rs"]
mod server_security_pki;
mod support;
#[path = "support/token_sha256.rs"]
mod token_sha256;

use std::{fmt::Write as _, net::SocketAddr, path::Path, sync::Arc};

use assert2::check;
use async_trait::async_trait;
use axum::Router;
use clap::Parser as _;
use krabka_audit::{AuditRecord, ChainState, MemorySink};
use krabka_blockstore::{LabelIndex, LogBlockIndex, TenantId, write_log_index_manifest};
use krabka_observability::{
    CancellationToken, InMemoryWalSink, IngestLimitError, LogIngestLimiter, Role, ServiceConfig,
    ServiceDependencies, WalLogRecord,
    audit::{
        AuditArgs, AuditClocks, AuditEvent, AuditHandle, AuditOutcome, AuditService, EpochMs,
        MECHANISM_BEARER, MECHANISM_NONE, OPERATION_DELETE_REQUEST_CANCEL,
        OPERATION_DELETE_REQUEST_CREATE, OPERATION_INGESTER_FLUSH,
        OPERATION_INGESTER_PREPARE_SHUTDOWN_SET, OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET,
        OPERATION_INGESTER_SHUTDOWN, OPERATION_LOG_LEVEL_SET, OPERATION_RULE_GROUP_DELETE,
        OPERATION_RULE_GROUP_SET, OPERATION_RULE_NAMESPACE_DELETE, OPERATION_TENANT_ACCESS,
        OPERATION_TENANT_READ, OPERATION_TENANT_WRITE, ProductInfo, RESOURCE_DELETE_REQUEST,
        RESOURCE_INGESTER, RESOURCE_LOG_LEVEL, RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE,
        RESOURCE_TENANT, RESOURCE_WAL_TOPIC, admin_operation, authentication, authorization_denied,
        krabka_product, principal, resource, source_endpoint, unauthenticated_principal,
        unknown_source_endpoint,
    },
    build_service_router, serve_service_listener,
    server_security::{Principal, ServerSecurity, install_crypto_provider},
};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use qubit_clock::{ManualMonotonicClock, ManualWallClock, MonotonicClock as _};
use rcgen::{CertifiedIssuer, KeyPair};
use secure_router::{SecureRouter, serve_secure_router};
use serde_json::json;
use server_security_pki::{Leaf, Pem, authority};
use support::{
    DenyingQueryAuthorizer, current_unix_epoch_nanos, proto_logs_request_at_ns, test_service_config,
};
use tempfile::TempDir;
use token_sha256::sha256_hex;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_util::sync::DropGuard;

const GRAFANA_TOKEN: &str = "grafana-3f1d9a7c5e2b4d6f8a0c1e3b5d7f9a1c";
const OPS_TOKEN: &str = "ops-8e6c4a2f0d1b3e5a7c9f1d3b5e7a9c0e";
const WAL_TOPIC: &str = "__krabka_observability_logs_wal";
const START_MS: i64 = 1_700_000_000_000;
const SELECTOR: &str = "%7Bapp%3D%22api%22%7D";
const JSON: &str = "application/json";
const YAML: &str = "application/yaml";

// A CA, a server certificate for `127.0.0.1` that it signed, and a credentials
// file, all in one temporary directory.
struct SecretsForTest {
    dir: TempDir,
    authority: CertifiedIssuer<'static, KeyPair>,
}

impl SecretsForTest {
    fn new() -> Self {
        Self {
            dir: TempDir::new().expect("a temporary directory"),
            authority: authority("krabka logs test ca"),
        }
    }

    fn write(&self, name: &str, contents: impl AsRef<[u8]>) -> String {
        let path = self.dir.path().join(name);
        std::fs::write(&path, contents).expect("the temporary file is writable");
        path.display().to_string()
    }

    // The flags that turn on TLS with a server certificate that this CA signed.
    fn tls_flags(&self) -> Vec<String> {
        let Pem { certificate, key } = Leaf::LOCAL_SERVER.signed_by(&self.authority);
        vec![
            format!(
                "--server-tls-cert-path={}",
                self.write("server.pem", certificate)
            ),
            format!(
                "--server-tls-key-path={}",
                self.write("server-key.pem", key)
            ),
        ]
    }

    // `grafana` may use `tenant-a` only and is not an admin. `ops` may use
    // every tenant and is an admin.
    fn credentials_flags(&self) -> Vec<String> {
        let yaml = format!(
            "principals:\n  - name: grafana\n    token_sha256: [{}]\n    tenants: [tenant-a]\n  - name: ops\n    token_sha256: [{}]\n    tenants: ['*']\n    admin: true\n",
            sha256_hex(GRAFANA_TOKEN),
            sha256_hex(OPS_TOKEN),
        );
        vec![format!(
            "--auth-credentials-config={}",
            self.write("credentials.yaml", yaml)
        )]
    }

    fn https_client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .tls_certs_only([
                reqwest::Certificate::from_pem(self.authority.pem().as_bytes())
                    .expect("the CA parses"),
            ])
            .build()
            .expect("the client builds")
    }
}

// The `ServiceConfig` that `flags` give, parsed as the binary parses them.
fn config_for_test(target: Role, flags: &[String]) -> ServiceConfig {
    let argv = [
        "krabka-observability".to_owned(),
        format!("--target={}", target.kind().as_str()),
    ]
    .into_iter()
    .chain(flags.iter().cloned());
    ServiceConfig::try_parse_from(argv).expect("the flags parse")
}

fn security_for_test(flags: &[String]) -> ServerSecurity {
    config_for_test(Role::Distributor, flags)
        .server_security
        .load()
        .expect("the flags load")
}

// A router served on `127.0.0.1:0` through the security listener, and stopped
// when the value drops.
struct ServedForTest {
    addr: SocketAddr,
    _stop: DropGuard,
}

async fn serve_for_test(router: Router, security: &ServerSecurity) -> ServedForTest {
    let SecureRouter { addr, stop } = serve_secure_router(router, security).await;
    ServedForTest {
        addr,
        _stop: stop.drop_guard(),
    }
}

#[derive(Clone)]
struct TestHeader {
    name: &'static str,
    value: String,
}

struct TestBody {
    content_type: &'static str,
    contents: String,
}

struct HttpsRequestCase<'a> {
    name: &'static str,
    method: &'static str,
    path: &'a str,
    tenant: Option<&'a str>,
    token: Option<&'a str>,
    body: TestBody,
    expected_status: u16,
}

#[derive(Debug)]
struct HttpsRequestAnswer {
    name: &'static str,
    actual_status: u16,
    expected_status: u16,
}

struct TenantCredentialCase {
    token: Option<&'static str>,
    tenant: &'static str,
}

struct AdminOperationCase {
    method: &'static str,
    path: &'static str,
    then_read: Option<&'static str>,
}

struct RuleChangeCase {
    method: &'static str,
    path: &'static str,
    body: &'static str,
}

#[derive(Debug, PartialEq, Eq)]
struct AdminRouteStep {
    description: String,
    status: u16,
    response_body: String,
}

fn admin_route_step(description: &str, status: u16, response_body: &str) -> AdminRouteStep {
    AdminRouteStep {
        description: description.to_owned(),
        status,
        response_body: response_body.to_owned(),
    }
}

fn test_body(content_type: &'static str, contents: &str) -> TestBody {
    TestBody {
        content_type,
        contents: contents.to_owned(),
    }
}

fn bearer_for_test(token: &str) -> TestHeader {
    TestHeader {
        name: "authorization",
        value: format!("Bearer {token}"),
    }
}

fn tenant_for_test(tenant: &str) -> TestHeader {
    TestHeader {
        name: "x-scope-orgid",
        value: tenant.to_owned(),
    }
}

fn push_body_for_test() -> String {
    json!({
        "streams": [{
            "stream": {"app": "api"},
            "values": [[current_unix_epoch_nanos().to_string(), "api error"]]
        }]
    })
    .to_string()
}

// One HTTP/1.1 request on a new plain connection. It gives the status, the
// body, and the client address that the server saw.
struct TestHttpResponse {
    status: u16,
    body: String,
    client: SocketAddr,
}

async fn send_for_test(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[TestHeader],
    body: &str,
) -> TestHttpResponse {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("the listener accepts");
    let client = stream.local_addr().expect("a local address");
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for header in headers {
        write!(request, "{}: {}\r\n", header.name, header.value)
            .expect("writing to a String cannot fail");
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream
        .write_all(request.as_bytes())
        .await
        .expect("the request is sent");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("the response is read");
    let response = String::from_utf8_lossy(&response).into_owned();
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .expect("a status line");
    let body = response
        .split_once("\r\n\r\n")
        .map_or_else(String::new, |parts| parts.1.to_owned());
    TestHttpResponse {
        status,
        body,
        client,
    }
}

fn product_for_test() -> ProductInfo {
    krabka_product("krabka-observability-test", "0.0.0")
}

// An enabled audit layer over a memory sink, whose clock stays at `START_MS`.
struct AuditForTest {
    _time: Arc<ManualMonotonicClock>,
    sink: Arc<MemorySink>,
    handle: AuditHandle,
    writer: JoinHandle<()>,
    stop: CancellationToken,
}

impl AuditForTest {
    fn start() -> Self {
        let time = ManualMonotonicClock::new_shared();
        let sink = Arc::new(MemorySink::default());
        let stop = CancellationToken::new();
        let parts = AuditService::start_with_sink(
            &AuditArgs::default(),
            product_for_test(),
            sink.clone(),
            AuditClocks {
                clock: Arc::new(ManualWallClock::from_clock(
                    std::time::UNIX_EPOCH
                        + std::time::Duration::from_millis(START_MS.unsigned_abs()),
                    Arc::clone(&time),
                )),
                timer: time.new_timer(),
            },
            stop.clone(),
        )
        .expect("the audit layer starts")
        .into_parts();
        let handle = parts.handle;
        let writer = parts.writer;
        Self {
            _time: time,
            sink,
            handle,
            writer: writer.expect("an enabled audit layer has a writer"),
            stop,
        }
    }

    // Stops the writer after it writes every queued event, and gives the
    // records that reached the sink.
    async fn records(self) -> Vec<AuditRecord> {
        self.stop.cancel();
        self.writer.await.expect("the writer does not panic");
        self.sink.records()
    }
}

// The records that the writer gives the sink for `events`, on a new chain.
fn expected_records_for_test(events: &[AuditEvent]) -> Vec<AuditRecord> {
    let mut chain = ChainState::new();
    events
        .iter()
        .map(|event| {
            let mut record = AuditRecord::from_event(event, &product_for_test());
            let chain_position = chain.extend(&record.value);
            record.push_chain_headers(chain_position.0, &chain_position.1);
            record
        })
        .collect()
}

// The test configuration of `target` over `data_root`. A querier reads its
// local index manifest when it builds, so this writes an empty one for it.
fn role_config_for_test(target: Role, data_root: &Path) -> ServiceConfig {
    if target == Role::Querier {
        write_log_index_manifest(data_root, &LabelIndex::default(), &LogBlockIndex::default())
            .expect("the empty manifest writes");
    }
    test_service_config(target, data_root)
}

// A role built with `dependencies` and `audit`, served over plain HTTP with the
// credentials of `secrets`, and reporting its security events to `audit`.
async fn audited_role_for_test(
    target: Role,
    dependencies: ServiceDependencies,
    data_root: &Path,
    audit: &AuditForTest,
    secrets: &SecretsForTest,
) -> ServedForTest {
    let security = security_for_test(&secrets.credentials_flags())
        .with_security_events(Arc::new(audit.handle.clone()));
    let router = build_service_router(
        &role_config_for_test(target, data_root),
        dependencies.with_audit(audit.handle.clone()),
        None,
    )
    .await
    .expect("the router builds");
    serve_for_test(router, &security).await
}

// A limiter that refuses every push, as a broker ACL refusal does.
struct RefusingIngestLimiter;

#[async_trait]
impl LogIngestLimiter for RefusingIngestLimiter {
    async fn check(
        &self,
        _principal: &Principal,
        tenant: &TenantId,
        _records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        Err(IngestLimitError::Unauthorized {
            tenant: tenant.to_string(),
            reason: "tenant write ACL denied".to_owned(),
        })
    }
}

// With no security flag set, the runtime serves plain HTTP and asks for no
// credential, as Loki does.
#[tokio::test]
async fn a_default_service_config_serves_plain_http_without_authentication() {
    let sink = InMemoryWalSink::default();
    let dir = TempDir::new().expect("a temporary directory");
    let config = test_service_config(Role::Distributor, dir.path());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
    let addr = listener.local_addr().expect("a local address");
    let server = tokio::spawn(serve_service_listener(
        listener,
        config,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        None,
    ));

    let ready = send_for_test(addr, "GET", "/ready", &[], "").await.status;
    let pushed = send_for_test(
        addr,
        "POST",
        "/loki/api/v1/push",
        &[
            tenant_for_test("tenant-a"),
            TestHeader {
                name: "content-type",
                value: "application/json".to_owned(),
            },
        ],
        &push_body_for_test(),
    )
    .await
    .status;
    server.abort();

    check!((ready, pushed) == (200, 204));
    check!(
        sink.records()
            .iter()
            .map(|record| record.tenant.as_str())
            .collect::<Vec<_>>()
            == ["tenant-a"]
    );
}

// The runtime reads TLS and the credentials file from the `ServiceConfig`. The
// probe needs no credential, and a push needs a credential that is granted
// its tenant.
#[tokio::test]
async fn the_service_runtime_serves_tls_and_credentials_from_the_service_config() {
    install_crypto_provider();
    let secrets = SecretsForTest::new();
    let dir = TempDir::new().expect("a temporary directory");
    let sink = InMemoryWalSink::default();
    let flags = [secrets.tls_flags(), secrets.credentials_flags()].concat();
    let config = ServiceConfig {
        server_security: config_for_test(Role::Distributor, &flags).server_security,
        ..test_service_config(Role::Distributor, dir.path())
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
    let addr = listener.local_addr().expect("a local address");
    let server = tokio::spawn(serve_service_listener(
        listener,
        config,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        None,
    ));
    let client = secrets.https_client();
    let push = |token: Option<&str>, tenant: &str| {
        let mut request = client
            .post(format!("https://{addr}/loki/api/v1/push"))
            .header("x-scope-orgid", tenant)
            .header("content-type", "application/json")
            .body(push_body_for_test());
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        request.send()
    };

    let ready = client
        .get(format!("https://{addr}/ready"))
        .send()
        .await
        .expect("the probe is answered")
        .status()
        .as_u16();
    let mut pushes = Vec::new();
    for case in [
        TenantCredentialCase {
            token: None,
            tenant: "tenant-a",
        },
        TenantCredentialCase {
            token: Some(GRAFANA_TOKEN),
            tenant: "tenant-a",
        },
        TenantCredentialCase {
            token: Some(GRAFANA_TOKEN),
            tenant: "tenant-b",
        },
    ] {
        pushes.push(
            push(case.token, case.tenant)
                .await
                .expect("the push is answered")
                .status()
                .as_u16(),
        );
    }
    server.abort();

    check!(ready == 200);
    check!(pushes == vec![401, 204, 403]);
    check!(
        sink.records()
            .iter()
            .map(|record| record.tenant.as_str())
            .collect::<Vec<_>>()
            == ["tenant-a"]
    );
}

// A role built with `dependencies` and served over TLS, where a request needs
// a bearer token from the credentials file of `secrets`.
async fn tls_role_for_test(
    target: Role,
    dependencies: ServiceDependencies,
    data_root: &Path,
    secrets: &SecretsForTest,
) -> ServedForTest {
    install_crypto_provider();
    let security = security_for_test(&[secrets.tls_flags(), secrets.credentials_flags()].concat());
    let router = build_service_router(&role_config_for_test(target, data_root), dependencies, None)
        .await
        .expect("the role builds");
    serve_for_test(router, &security).await
}

// One request over TLS, with a body and its content type, as
// `tests/tenant_header.rs` gives them. It gives the status.
async fn send_https_for_test(
    client: &reqwest::Client,
    addr: SocketAddr,
    method: &str,
    path: &str,
    tenant: Option<&str>,
    token: Option<&str>,
    body: TestBody,
) -> u16 {
    let mut request = client
        .request(
            method.parse().expect("a valid method"),
            format!("https://{addr}{path}"),
        )
        .body(body.contents);
    if !body.content_type.is_empty() {
        request = request.header("content-type", body.content_type);
    }
    if let Some(tenant) = tenant {
        request = request.header("x-scope-orgid", tenant);
    }
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    request
        .send()
        .await
        .expect("the request is answered")
        .status()
        .as_u16()
}

// Over TLS with bearer tokens, a push reaches only a tenant in the grant of its
// principal, and a refused push reaches no WAL. A missing or unknown token gets
// 401, and the probe needs no token.
#[tokio::test]
async fn tls_and_bearer_tokens_guard_the_push_routes() {
    let secrets = SecretsForTest::new();
    let dir = TempDir::new().expect("a temporary directory");
    let sink = InMemoryWalSink::default();
    let distributor = tls_role_for_test(
        Role::Distributor,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        dir.path(),
        &secrets,
    )
    .await;
    let client = secrets.https_client();
    let push = push_body_for_test();
    let path = "/loki/api/v1/push";
    let cases = [
        HttpsRequestCase {
            name: "granted",
            method: "POST",
            path,
            tenant: Some("tenant-a"),
            token: Some(GRAFANA_TOKEN),
            body: test_body(JSON, push.as_str()),
            expected_status: 204,
        },
        HttpsRequestCase {
            name: "outside the grant",
            method: "POST",
            path,
            tenant: Some("tenant-b"),
            token: Some(GRAFANA_TOKEN),
            body: test_body(JSON, push.as_str()),
            expected_status: 403,
        },
        HttpsRequestCase {
            name: "no token",
            method: "POST",
            path,
            tenant: Some("tenant-a"),
            token: None,
            body: test_body(JSON, push.as_str()),
            expected_status: 401,
        },
        HttpsRequestCase {
            name: "unknown token",
            method: "POST",
            path,
            tenant: Some("tenant-a"),
            token: Some("not-a-token"),
            body: test_body(JSON, push.as_str()),
            expected_status: 401,
        },
        HttpsRequestCase {
            name: "admin, another tenant",
            method: "POST",
            path,
            tenant: Some("tenant-b"),
            token: Some(OPS_TOKEN),
            body: test_body(JSON, push.as_str()),
            expected_status: 204,
        },
        HttpsRequestCase {
            name: "probe, no token",
            method: "GET",
            path: "/ready",
            tenant: None,
            token: None,
            body: test_body("", ""),
            expected_status: 200,
        },
    ];

    let mut answers = Vec::new();
    for case in cases {
        let actual_status = send_https_for_test(
            &client,
            distributor.addr,
            case.method,
            case.path,
            case.tenant,
            case.token,
            case.body,
        )
        .await;
        answers.push(HttpsRequestAnswer {
            name: case.name,
            actual_status,
            expected_status: case.expected_status,
        });
    }

    for answer in &answers {
        check!(
            answer.actual_status == answer.expected_status,
            "{}: expected {}, got {}",
            answer.name,
            answer.expected_status,
            answer.actual_status
        );
    }
    check!(
        sink.records()
            .iter()
            .map(|record| record.tenant.as_str())
            .collect::<Vec<_>>()
            == ["tenant-a", "tenant-b"]
    );
}

// Over TLS with bearer tokens, a read and a ruler call reach only the tenants
// in the grant of the principal. A federated read that names one tenant
// outside the grant is refused as a whole.
#[tokio::test]
async fn tls_and_bearer_tokens_guard_the_query_and_ruler_routes() {
    let secrets = SecretsForTest::new();
    let dir = TempDir::new().expect("a temporary directory");
    let querier = tls_role_for_test(
        Role::Querier,
        ServiceDependencies::default(),
        dir.path(),
        &secrets,
    )
    .await;
    let client = secrets.https_client();
    let rule_group = "name: api-errors\nrules:\n  - alert: ApiErrors\n    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0\n";
    let labels = "/loki/api/v1/labels?start=0.000000000&end=0.000000100".to_owned();
    let federated =
        format!("/loki/api/v1/query_range?query={SELECTOR}&start=0.000000000&end=0.000000100");
    let token = Some(GRAFANA_TOKEN);
    let cases = [
        HttpsRequestCase {
            name: "probe, no token",
            method: "GET",
            path: "/ready",
            tenant: None,
            token: None,
            body: test_body("", ""),
            expected_status: 200,
        },
        HttpsRequestCase {
            name: "query granted",
            method: "GET",
            path: labels.as_str(),
            tenant: Some("tenant-a"),
            token,
            body: test_body("", ""),
            expected_status: 200,
        },
        HttpsRequestCase {
            name: "query outside the grant",
            method: "GET",
            path: labels.as_str(),
            tenant: Some("tenant-b"),
            token,
            body: test_body("", ""),
            expected_status: 403,
        },
        HttpsRequestCase {
            name: "query, no token",
            method: "GET",
            path: labels.as_str(),
            tenant: Some("tenant-a"),
            token: None,
            body: test_body("", ""),
            expected_status: 401,
        },
        HttpsRequestCase {
            name: "federated query",
            method: "GET",
            path: federated.as_str(),
            tenant: Some("tenant-a|tenant-b"),
            token,
            body: test_body("", ""),
            expected_status: 403,
        },
        HttpsRequestCase {
            name: "ruler read outside the grant",
            method: "GET",
            path: "/loki/api/v1/rules",
            tenant: Some("tenant-b"),
            token,
            body: test_body("", ""),
            expected_status: 403,
        },
        HttpsRequestCase {
            name: "ruler write outside the grant",
            method: "POST",
            path: "/loki/api/v1/rules/default",
            tenant: Some("tenant-b"),
            token,
            body: test_body(YAML, rule_group),
            expected_status: 403,
        },
    ];

    let mut answers = Vec::new();
    for case in cases {
        let actual_status = send_https_for_test(
            &client,
            querier.addr,
            case.method,
            case.path,
            case.tenant,
            case.token,
            case.body,
        )
        .await;
        answers.push(HttpsRequestAnswer {
            name: case.name,
            actual_status,
            expected_status: case.expected_status,
        });
    }

    for answer in &answers {
        check!(
            answer.actual_status == answer.expected_status,
            "{}: expected {}, got {}",
            answer.name,
            answer.expected_status,
            answer.actual_status
        );
    }
}

// Over TLS with bearer tokens, the delete-request API reaches only the tenants
// in the grant of the principal.
#[tokio::test]
async fn tls_and_bearer_tokens_guard_the_delete_routes() {
    let secrets = SecretsForTest::new();
    let dir = TempDir::new().expect("a temporary directory");
    let block_builder = tls_role_for_test(
        Role::BlockBuilder,
        ServiceDependencies::default(),
        dir.path(),
        &secrets,
    )
    .await;
    let client = secrets.https_client();
    let create = format!("/loki/api/v1/delete?query={SELECTOR}&start=1");
    let list = "/loki/api/v1/delete";
    let token = Some(GRAFANA_TOKEN);
    let cases = [
        HttpsRequestCase {
            name: "probe, no token",
            method: "GET",
            path: "/ready",
            tenant: None,
            token: None,
            body: test_body("", ""),
            expected_status: 200,
        },
        HttpsRequestCase {
            name: "list outside the grant",
            method: "GET",
            path: list,
            tenant: Some("tenant-b"),
            token,
            body: test_body("", ""),
            expected_status: 403,
        },
        HttpsRequestCase {
            name: "create outside the grant",
            method: "POST",
            path: create.as_str(),
            tenant: Some("tenant-b"),
            token,
            body: test_body("", ""),
            expected_status: 403,
        },
        HttpsRequestCase {
            name: "list granted",
            method: "GET",
            path: list,
            tenant: Some("tenant-a"),
            token,
            body: test_body("", ""),
            expected_status: 200,
        },
    ];

    let mut answers = Vec::new();
    for case in cases {
        let actual_status = send_https_for_test(
            &client,
            block_builder.addr,
            case.method,
            case.path,
            case.tenant,
            case.token,
            case.body,
        )
        .await;
        answers.push(HttpsRequestAnswer {
            name: case.name,
            actual_status,
            expected_status: case.expected_status,
        });
    }

    for answer in &answers {
        check!(
            answer.actual_status == answer.expected_status,
            "{}: expected {}, got {}",
            answer.name,
            answer.expected_status,
            answer.actual_status
        );
    }
}

// The OTLP export is served through the router, so the authentication layer
// has already given it a principal. A tenant outside the grant gets
// `PERMISSION_DENIED`, and its logs reach no WAL.
#[tokio::test]
async fn the_grpc_log_export_answers_a_tenant_outside_the_grant_with_permission_denied() {
    let secrets = SecretsForTest::new();
    let security = security_for_test(&secrets.credentials_flags());
    let dir = TempDir::new().expect("a temporary directory");
    let sink = InMemoryWalSink::default();
    let distributor = serve_for_test(
        build_service_router(
            &test_service_config(Role::Distributor, dir.path()),
            ServiceDependencies::default().with_wal_sink(sink.clone()),
            None,
        )
        .await
        .expect("the distributor builds"),
        &security,
    )
    .await;
    let channel = tonic::transport::Channel::from_shared(format!("http://{}", distributor.addr))
        .expect("a valid URI")
        .connect()
        .await
        .expect("the channel connects");
    let mut client =
        LogsServiceClient::with_interceptor(channel, |mut request: tonic::Request<()>| {
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {GRAFANA_TOKEN}")
                    .parse()
                    .expect("a valid metadata value"),
            );
            Ok(request)
        });
    let now_ns = u64::try_from(current_unix_epoch_nanos()).expect("a timestamp that fits");

    let mut codes = Vec::new();
    for tenant in ["tenant-b", "tenant-a"] {
        let mut request = tonic::Request::new(proto_logs_request_at_ns(now_ns));
        request.metadata_mut().insert(
            "x-scope-orgid",
            tenant.parse().expect("a valid metadata value"),
        );
        codes.push(
            client
                .export(request)
                .await
                .map_or_else(|status| status.code(), |_| tonic::Code::Ok),
        );
    }

    check!(codes == [tonic::Code::PermissionDenied, tonic::Code::Ok]);
    check!(
        sink.records()
            .iter()
            .map(|record| record.tenant.as_str())
            .collect::<Vec<_>>()
            == ["tenant-a"]
    );
}

// An authenticated principal without `admin: true` gets 403 on every admin
// route, and the route changes nothing. The read-only ops routes still answer
// it. The admin principal passes every route. Its `POST /log_level` answers
// 501 here, because this test process installs no reloadable logging;
// `tests/log_level.rs` checks the level change itself.
#[tokio::test]
async fn an_admin_route_needs_an_admin_principal_and_a_refusal_changes_nothing() {
    let secrets = SecretsForTest::new();
    let security = security_for_test(&secrets.credentials_flags());
    let dir = TempDir::new().expect("a temporary directory");
    let distributor = serve_for_test(
        build_service_router(
            &test_service_config(Role::Distributor, dir.path()),
            ServiceDependencies::default().with_wal_sink(InMemoryWalSink::default()),
            None,
        )
        .await
        .expect("the distributor builds"),
        &security,
    )
    .await;
    let addr = distributor.addr;
    let grafana = [bearer_for_test(GRAFANA_TOKEN)];
    let ops = [bearer_for_test(OPS_TOKEN)];
    let level_before = send_for_test(addr, "GET", "/log_level", &grafana, "")
        .await
        .status;

    let mut steps = Vec::new();
    for operation in [
        AdminOperationCase {
            method: "POST",
            path: "/log_level?log_level=debug",
            then_read: None,
        },
        AdminOperationCase {
            method: "POST",
            path: "/flush",
            then_read: None,
        },
        AdminOperationCase {
            method: "POST",
            path: "/ingester/prepare_shutdown",
            then_read: None,
        },
        AdminOperationCase {
            method: "DELETE",
            path: "/ingester/prepare_shutdown",
            then_read: None,
        },
        AdminOperationCase {
            method: "GET",
            path: "/ingester/shutdown",
            then_read: None,
        },
        AdminOperationCase {
            method: "POST",
            path: "/ingester/shutdown",
            then_read: None,
        },
    ] {
        let status = send_for_test(addr, operation.method, operation.path, &grafana, "")
            .await
            .status;
        steps.push(AdminRouteStep {
            description: format!("grafana {} {}", operation.method, operation.path),
            status,
            response_body: String::new(),
        });
    }
    let drain = send_for_test(addr, "GET", "/ingester/prepare_shutdown", &grafana, "").await;
    steps.push(AdminRouteStep {
        description: "grafana reads the drain".to_owned(),
        status: drain.status,
        response_body: drain.body,
    });
    let ready = send_for_test(addr, "GET", "/ready", &[], "").await.status;
    steps.push(AdminRouteStep {
        description: "the probe after grafana".to_owned(),
        status: ready,
        response_body: String::new(),
    });
    let level_after = send_for_test(addr, "GET", "/log_level", &grafana, "")
        .await
        .status;
    for operation in [
        AdminOperationCase {
            method: "POST",
            path: "/log_level?log_level=debug",
            then_read: None,
        },
        AdminOperationCase {
            method: "POST",
            path: "/flush",
            then_read: None,
        },
        AdminOperationCase {
            method: "POST",
            path: "/ingester/prepare_shutdown",
            then_read: Some("/ingester/prepare_shutdown"),
        },
        AdminOperationCase {
            method: "DELETE",
            path: "/ingester/prepare_shutdown",
            then_read: Some("/ingester/prepare_shutdown"),
        },
        AdminOperationCase {
            method: "GET",
            path: "/ingester/shutdown",
            then_read: Some("/ready"),
        },
    ] {
        let status = send_for_test(addr, operation.method, operation.path, &ops, "")
            .await
            .status;
        let read = match operation.then_read {
            Some(read) => send_for_test(addr, "GET", read, &ops, "").await.body,
            None => String::new(),
        };
        steps.push(AdminRouteStep {
            description: format!("ops {} {}", operation.method, operation.path),
            status,
            response_body: read,
        });
    }

    check!(level_after == level_before);
    check!(
        steps
            == [
                admin_route_step("grafana POST /log_level?log_level=debug", 403, ""),
                admin_route_step("grafana POST /flush", 403, ""),
                admin_route_step("grafana POST /ingester/prepare_shutdown", 403, ""),
                admin_route_step("grafana DELETE /ingester/prepare_shutdown", 403, ""),
                admin_route_step("grafana GET /ingester/shutdown", 403, ""),
                admin_route_step("grafana POST /ingester/shutdown", 403, ""),
                admin_route_step("grafana reads the drain", 200, "unset"),
                admin_route_step("the probe after grafana", 200, ""),
                admin_route_step("ops POST /log_level?log_level=debug", 501, ""),
                admin_route_step("ops POST /flush", 204, ""),
                admin_route_step("ops POST /ingester/prepare_shutdown", 204, "set"),
                admin_route_step("ops DELETE /ingester/prepare_shutdown", 204, "unset"),
                admin_route_step(
                    "ops GET /ingester/shutdown",
                    204,
                    "not ready: accepting-writes\n"
                ),
            ]
    );
}

// Creating and cancelling a delete request each record one admin operation,
// with the tenant, the request id, the principal and the client address.
#[tokio::test]
async fn a_delete_request_create_and_cancel_each_record_one_admin_operation() {
    let secrets = SecretsForTest::new();
    let audit = AuditForTest::start();
    let dir = TempDir::new().expect("a temporary directory");
    let block_builder = audited_role_for_test(
        Role::BlockBuilder,
        ServiceDependencies::default(),
        dir.path(),
        &audit,
        &secrets,
    )
    .await;
    let headers = [tenant_for_test("tenant-a"), bearer_for_test(GRAFANA_TOKEN)];

    let created = send_for_test(
        block_builder.addr,
        "POST",
        &format!("/loki/api/v1/delete?query={SELECTOR}&start=1"),
        &headers,
        "",
    )
    .await;
    let cancelled = send_for_test(
        block_builder.addr,
        "DELETE",
        "/loki/api/v1/delete?request_id=delete-1",
        &headers,
        "",
    )
    .await;

    let grafana = principal("grafana", MECHANISM_BEARER);
    let resources = vec![
        resource(RESOURCE_TENANT, "tenant-a"),
        resource(RESOURCE_DELETE_REQUEST, "delete-1"),
    ];
    check!((created.status, cancelled.status) == (204, 204));
    check!(
        audit.records().await
            == expected_records_for_test(&[
                admin_operation(
                    grafana.clone(),
                    source_endpoint(created.client),
                    OPERATION_DELETE_REQUEST_CREATE,
                    resources.clone(),
                    AuditOutcome::Success,
                    EpochMs(START_MS),
                ),
                admin_operation(
                    grafana,
                    source_endpoint(cancelled.client),
                    OPERATION_DELETE_REQUEST_CANCEL,
                    resources,
                    AuditOutcome::Success,
                    EpochMs(START_MS),
                ),
            ])
    );
}

// Storing two rule groups, deleting one group, and deleting the namespace each
// record one admin operation.
#[tokio::test]
async fn every_rule_group_change_records_one_admin_operation() {
    let secrets = SecretsForTest::new();
    let audit = AuditForTest::start();
    let dir = TempDir::new().expect("a temporary directory");
    let querier = audited_role_for_test(
        Role::Querier,
        ServiceDependencies::default(),
        dir.path(),
        &audit,
        &secrets,
    )
    .await;
    let headers = [
        tenant_for_test("tenant-a"),
        bearer_for_test(GRAFANA_TOKEN),
        TestHeader {
            name: "content-type",
            value: "application/yaml".to_owned(),
        },
    ];
    let errors = "name: api-errors\nrules:\n  - alert: ApiErrors\n    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0\n";
    let rates = "name: api-rates\nrules:\n  - record: job:api_errors:rate5m\n    expr: sum(rate({app=\"api\"} |= \"error\" [5m]))\n";

    let mut answers = Vec::new();
    for change in [
        RuleChangeCase {
            method: "POST",
            path: "/loki/api/v1/rules/default",
            body: errors,
        },
        RuleChangeCase {
            method: "POST",
            path: "/loki/api/v1/rules/default",
            body: rates,
        },
        RuleChangeCase {
            method: "DELETE",
            path: "/loki/api/v1/rules/default/api-errors",
            body: "",
        },
        RuleChangeCase {
            method: "DELETE",
            path: "/loki/api/v1/rules/default",
            body: "",
        },
    ] {
        answers.push(
            send_for_test(
                querier.addr,
                change.method,
                change.path,
                &headers,
                change.body,
            )
            .await,
        );
    }

    let grafana = principal("grafana", MECHANISM_BEARER);
    let event = |index: usize, operation, name: &str, resource_type| {
        let client = &answers[index];
        admin_operation(
            grafana.clone(),
            source_endpoint(client.client),
            operation,
            vec![
                resource(RESOURCE_TENANT, "tenant-a"),
                resource(resource_type, name),
            ],
            AuditOutcome::Success,
            EpochMs(START_MS),
        )
    };
    let expected = expected_records_for_test(&[
        event(
            0,
            OPERATION_RULE_GROUP_SET,
            "default/api-errors",
            RESOURCE_RULE_GROUP,
        ),
        event(
            1,
            OPERATION_RULE_GROUP_SET,
            "default/api-rates",
            RESOURCE_RULE_GROUP,
        ),
        event(
            2,
            OPERATION_RULE_GROUP_DELETE,
            "default/api-errors",
            RESOURCE_RULE_GROUP,
        ),
        event(
            3,
            OPERATION_RULE_NAMESPACE_DELETE,
            "default",
            RESOURCE_RULE_NAMESPACE,
        ),
    ]);
    check!(
        answers
            .iter()
            .map(|answer| answer.status)
            .collect::<Vec<_>>()
            == [202, 202, 202, 202]
    );
    check!(audit.records().await == expected);
}

// Every ingester admin operation records one admin operation that names the
// ingester of the role. The log level change fails in this process, which has
// no reloadable logging, so its outcome is a failure.
#[tokio::test]
async fn every_ingester_admin_operation_records_one_admin_operation() {
    let secrets = SecretsForTest::new();
    let audit = AuditForTest::start();
    let dir = TempDir::new().expect("a temporary directory");
    let distributor = audited_role_for_test(
        Role::Distributor,
        ServiceDependencies::default().with_wal_sink(InMemoryWalSink::default()),
        dir.path(),
        &audit,
        &secrets,
    )
    .await;
    let headers = [bearer_for_test(OPS_TOKEN)];

    let mut answers = Vec::new();
    for operation in [
        AdminOperationCase {
            method: "POST",
            path: "/log_level?log_level=debug",
            then_read: None,
        },
        AdminOperationCase {
            method: "POST",
            path: "/flush",
            then_read: None,
        },
        AdminOperationCase {
            method: "POST",
            path: "/ingester/prepare_shutdown",
            then_read: None,
        },
        AdminOperationCase {
            method: "DELETE",
            path: "/ingester/prepare_shutdown",
            then_read: None,
        },
        AdminOperationCase {
            method: "POST",
            path: "/ingester/shutdown",
            then_read: None,
        },
    ] {
        answers.push(
            send_for_test(
                distributor.addr,
                operation.method,
                operation.path,
                &headers,
                "",
            )
            .await,
        );
    }

    let ops = principal("ops", MECHANISM_BEARER);
    let ingester = || vec![resource(RESOURCE_INGESTER, "krabka-distributor")];
    let event = |index: usize, operation, resources, outcome| {
        let client = &answers[index];
        admin_operation(
            ops.clone(),
            source_endpoint(client.client),
            operation,
            resources,
            outcome,
            EpochMs(START_MS),
        )
    };
    let expected = expected_records_for_test(&[
        event(
            0,
            OPERATION_LOG_LEVEL_SET,
            vec![resource(RESOURCE_LOG_LEVEL, "debug")],
            AuditOutcome::Failure,
        ),
        event(
            1,
            OPERATION_INGESTER_FLUSH,
            ingester(),
            AuditOutcome::Success,
        ),
        event(
            2,
            OPERATION_INGESTER_PREPARE_SHUTDOWN_SET,
            ingester(),
            AuditOutcome::Success,
        ),
        event(
            3,
            OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET,
            ingester(),
            AuditOutcome::Success,
        ),
        event(
            4,
            OPERATION_INGESTER_SHUTDOWN,
            ingester(),
            AuditOutcome::Success,
        ),
    ]);
    check!(
        answers
            .iter()
            .map(|answer| answer.status)
            .collect::<Vec<_>>()
            == [501, 204, 204, 204, 204]
    );
    check!(audit.records().await == expected);
}

// A request with no credential records one failed authentication. A tenant
// outside the grant records one refusal, and that refusal is not recorded a
// second time as an operation. Neither push reaches the WAL.
#[tokio::test]
async fn a_failed_authentication_and_a_refused_tenant_each_record_one_event() {
    let secrets = SecretsForTest::new();
    let audit = AuditForTest::start();
    let dir = TempDir::new().expect("a temporary directory");
    let sink = InMemoryWalSink::default();
    let distributor = audited_role_for_test(
        Role::Distributor,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        dir.path(),
        &audit,
        &secrets,
    )
    .await;
    let body = push_body_for_test();
    let content_type = TestHeader {
        name: "content-type",
        value: "application/json".to_owned(),
    };

    let anonymous = send_for_test(
        distributor.addr,
        "POST",
        "/loki/api/v1/push",
        &[tenant_for_test("tenant-a"), content_type.clone()],
        &body,
    )
    .await;
    let refused = send_for_test(
        distributor.addr,
        "POST",
        "/loki/api/v1/push",
        &[
            tenant_for_test("tenant-b"),
            bearer_for_test(GRAFANA_TOKEN),
            content_type,
        ],
        &body,
    )
    .await;

    check!((anonymous.status, refused.status) == (401, 403));
    check!(sink.records().is_empty());
    check!(
        audit.records().await
            == expected_records_for_test(&[
                authentication(
                    AuditOutcome::Failure,
                    MECHANISM_NONE,
                    unauthenticated_principal(),
                    source_endpoint(anonymous.client),
                    Some("missing_credential".to_owned()),
                    EpochMs(START_MS),
                ),
                authorization_denied(
                    principal("grafana", MECHANISM_BEARER),
                    unknown_source_endpoint(),
                    RESOURCE_TENANT,
                    "tenant-b",
                    OPERATION_TENANT_ACCESS,
                    EpochMs(START_MS),
                ),
            ])
    );
}

// A read and a write that the broker ACLs refuse each record one refusal on
// the WAL topic, with the principal and the client address.
#[tokio::test]
async fn a_broker_acl_refusal_records_one_authorization_denied_on_the_wal_topic() {
    let secrets = SecretsForTest::new();
    let audit = AuditForTest::start();
    let dir = TempDir::new().expect("a temporary directory");
    let querier = audited_role_for_test(
        Role::Querier,
        ServiceDependencies::default().with_query_authorizer(DenyingQueryAuthorizer),
        dir.path(),
        &audit,
        &secrets,
    )
    .await;
    let distributor = audited_role_for_test(
        Role::Distributor,
        ServiceDependencies::default()
            .with_wal_sink(InMemoryWalSink::default())
            .with_ingest_limiter(RefusingIngestLimiter),
        dir.path(),
        &audit,
        &secrets,
    )
    .await;

    let read = send_for_test(
        querier.addr,
        "GET",
        "/loki/api/v1/labels?start=0.000000000&end=0.000000100",
        &[tenant_for_test("tenant-a"), bearer_for_test(GRAFANA_TOKEN)],
        "",
    )
    .await;
    let write = send_for_test(
        distributor.addr,
        "POST",
        "/loki/api/v1/push",
        &[
            tenant_for_test("tenant-a"),
            bearer_for_test(GRAFANA_TOKEN),
            TestHeader {
                name: "content-type",
                value: "application/json".to_owned(),
            },
        ],
        &push_body_for_test(),
    )
    .await;

    let grafana = principal("grafana", MECHANISM_BEARER);
    check!((read.status, write.status) == (403, 403));
    check!(
        audit.records().await
            == expected_records_for_test(&[
                authorization_denied(
                    grafana.clone(),
                    source_endpoint(read.client),
                    RESOURCE_WAL_TOPIC,
                    WAL_TOPIC,
                    OPERATION_TENANT_READ,
                    EpochMs(START_MS),
                ),
                authorization_denied(
                    grafana,
                    source_endpoint(write.client),
                    RESOURCE_WAL_TOPIC,
                    WAL_TOPIC,
                    OPERATION_TENANT_WRITE,
                    EpochMs(START_MS),
                ),
            ])
    );
}
