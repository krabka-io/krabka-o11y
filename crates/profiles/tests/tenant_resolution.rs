//! Every profiles door resolves `X-Scope-OrgID` the same way.
//!
//! The distributor serves a plain HTTP door and Connect methods, and so does
//! the querier. Each one reads the tenant through the one resolver in
//! `krabka-blockstore`. This suite sends one table of header values to a door
//! of each kind, and it expects one answer per header value. A request without
//! a tenant goes to `anonymous`, as Pyroscope with multi-tenancy off does. A
//! malformed tenant gets a 400 or `invalid_argument` with the message Grafana's
//! `dskit` sends, and nothing reaches the WAL.
//!
//! With TLS and a credentials file, the same doors also check the resolved
//! tenant against the grant of the request's principal. A missing or wrong
//! credential gets a 401. A tenant outside the grant gets a 403 or
//! `permission_denied`, and nothing reaches the WAL. The audit log records one
//! event for each refusal. The certificates are generated for each test with
//! `rcgen`, so no key material is in the repository.

use std::{
    fmt::Write as _,
    io::Write as _,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use assert2::check;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use flate2::{Compression, write::GzEncoder};
use krabka_audit::{AuditLog, AuditStats};
use krabka_blockstore::{TENANT_HEADER, TenantId, TenantPolicy};
use krabka_observability::{
    CancellationToken, RoleReadiness,
    audit::{
        AuditHandle, AuditOutcome, EpochMs, MECHANISM_BEARER, MECHANISM_NONE,
        OPERATION_TENANT_ACCESS, RESOURCE_TENANT, authentication, authorization_denied, principal,
        source_endpoint, unauthenticated_principal, unknown_source_endpoint,
    },
    server_security::{ClientAuth, ServerSecurity, ServerSecurityArgs, install_crypto_provider},
};
use krabka_pprof::{FunctionRec, InMemoryProfileStore, LineRec, LocationRec, PprofProfile, proto};
use krabka_profiles::{
    ProfileRecord, ProfilesError,
    distributor::{self, DistributorState, WalSink},
    ingest::LegacyDecodeLimits,
    limits::{Limits, OverridesProvider},
    metrics::ServiceMetrics,
    query::{self, QuerierState},
};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpSocket,
};
use tokio_util::sync::DropGuard;

const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const PLAIN_TEXT: &str = "text/plain; charset=utf-8";
/// The bearer token of the `grafana` principal, which may use `tenant-a` only.
const GRAFANA_TOKEN: &str = "grafana-5d2c8a1f7e3b9c4d6a0e2f8b1c7d3e9a";
/// The time that the test clock gives every audit event.
const EVENT_TIME_MS: i64 = 1_700_000_000_000;

#[derive(Default)]
struct CapturingSink(Mutex<Vec<ProfileRecord>>);

impl CapturingSink {
    fn take_tenants(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().expect("the sink lock is held"))
            .into_iter()
            .map(|record| record.tenant)
            .collect()
    }
}

#[async_trait::async_trait]
impl WalSink for CapturingSink {
    async fn append(&self, record: ProfileRecord) -> Result<(), ProfilesError> {
        self.0.lock().expect("the sink lock is held").push(record);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Door {
    /// `POST /ingest`, the legacy Pyroscope ingest route.
    DistributorHttp,
    /// `push.v1.PusherService/Push`.
    DistributorConnect,
    /// `GET /pyroscope/render`.
    QuerierHttp,
    /// `querier.v1.QuerierService/LabelValues`.
    QuerierConnect,
}

const DOORS: [Door; 4] = [
    Door::DistributorHttp,
    Door::DistributorConnect,
    Door::QuerierHttp,
    Door::QuerierConnect,
];

#[derive(Debug, PartialEq, Eq)]
enum Answer {
    /// The distributor took the push.
    Accepted,
    /// The querier answered from the data of this tenant.
    Served(String),
    /// A plain HTTP route rejected the request.
    HttpRejection {
        status: u16,
        content_type: String,
        body: String,
    },
    /// A Connect method rejected the request.
    ConnectRejection {
        status: u16,
        code: String,
        message: String,
    },
}

/// One row of the header table: a name, the `X-Scope-OrgID` bytes if the
/// request carries the header, and the tenant or the message it resolves to.
type Case<'a> = (&'a str, Option<&'a [u8]>, Result<&'a str, &'a str>);

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    answer: Answer,
    /// The tenant of each record that reached the WAL sink.
    wal_tenants: Vec<String>,
}

impl Outcome {
    fn expected(door: Door, resolution: Result<&str, &str>) -> Self {
        let answer = match (door, resolution) {
            (Door::DistributorHttp | Door::DistributorConnect, Ok(_)) => Answer::Accepted,
            (Door::QuerierHttp | Door::QuerierConnect, Ok(tenant)) => {
                Answer::Served(tenant.to_string())
            }
            (Door::DistributorHttp | Door::QuerierHttp, Err(message)) => Answer::HttpRejection {
                status: 400,
                content_type: PLAIN_TEXT.to_string(),
                body: message.to_string(),
            },
            (Door::DistributorConnect | Door::QuerierConnect, Err(message)) => {
                Answer::ConnectRejection {
                    status: 400,
                    code: "invalid_argument".to_string(),
                    message: message.to_string(),
                }
            }
        };
        let wal_tenants = match (door, resolution) {
            (Door::DistributorHttp | Door::DistributorConnect, Ok(tenant)) => {
                vec![tenant.to_string()]
            }
            _ => Vec::new(),
        };
        Self {
            answer,
            wal_tenants,
        }
    }

    /// What a door answers when the principal may not use the tenant, and
    /// `message` is the text of that denial.
    fn denied(door: Door, message: &str) -> Self {
        let answer = match door {
            Door::DistributorHttp | Door::QuerierHttp => Answer::HttpRejection {
                status: 403,
                content_type: PLAIN_TEXT.to_string(),
                body: format!("{message}\n"),
            },
            Door::DistributorConnect | Door::QuerierConnect => Answer::ConnectRejection {
                status: 403,
                code: "permission_denied".to_string(),
                message: message.to_string(),
            },
        };
        Self {
            answer,
            wal_tenants: Vec::new(),
        }
    }
}

struct Stack {
    client: reqwest::Client,
    scheme: &'static str,
    distributor: SocketAddr,
    querier: SocketAddr,
    sink: Arc<CapturingSink>,
    _stop: DropGuard,
}

impl Stack {
    /// Starts a distributor that resolves under `policy`, and a querier with
    /// one frame for `anonymous` and one for `tenant-a`, with no security.
    async fn start(policy: TenantPolicy) -> Self {
        Self::start_with(
            policy,
            &ServerSecurity::default(),
            reqwest::Client::new(),
            "http",
        )
        .await
    }

    /// Starts the same two roles behind `security`, and reaches them with
    /// `client` over `scheme`.
    async fn start_with(
        policy: TenantPolicy,
        security: &ServerSecurity,
        client: reqwest::Client,
        scheme: &'static str,
    ) -> Self {
        let sink = Arc::new(CapturingSink::default());
        let distributor_state = Arc::new(DistributorState {
            sink: Arc::clone(&sink) as Arc<dyn WalSink>,
            overrides: OverridesProvider::new(Limits::default()),
            tenant_policy: policy,
            active_series: Mutex::default(),
            cumulative_profiles: tokio::sync::Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: Vec::new(),
            max_decompressed: krabka_units::mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });
        let stop = CancellationToken::new();
        let (distributor, _) = distributor::serve_supervised(
            "127.0.0.1:0".parse().unwrap(),
            distributor_state,
            RoleReadiness::new(),
            security,
            stop.clone(),
        )
        .await
        .expect("the distributor binds");

        let querier_state = Arc::new(QuerierState::new(Arc::new(
            store_with_one_frame_per_tenant(),
        )));
        let (querier, _) = query::serve_supervised(
            "127.0.0.1:0".parse().unwrap(),
            querier_state,
            RoleReadiness::new(),
            security,
            stop.clone(),
        )
        .await
        .expect("the querier binds");

        Self {
            client,
            scheme,
            distributor,
            querier,
            sink,
            _stop: stop.drop_guard(),
        }
    }

    fn request(&self, door: Door, tenant: Option<&[u8]>) -> reqwest::RequestBuilder {
        let (scheme, distributor, querier) = (self.scheme, self.distributor, self.querier);
        let request = match door {
            Door::DistributorHttp => self
                .client
                .post(format!(
                    "{scheme}://{distributor}/ingest?name=myapp{{service_name=\"api\"}}&format=groups&units=samples&until=1700000000000"
                ))
                .header(CONTENT_TYPE, "text/plain")
                .body("main;work 3\n"),
            Door::DistributorConnect => self
                .client
                .post(format!(
                    "{scheme}://{distributor}/push.v1.PusherService/Push"
                ))
                .json(&push_body()),
            Door::QuerierHttp => {
                let query = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("query", &format!("{PROFILE_TYPE}{{}}"))
                    .append_pair("from", "0")
                    .append_pair("until", "100")
                    .finish();
                self.client
                    .get(format!("{scheme}://{querier}/pyroscope/render?{query}"))
            }
            Door::QuerierConnect => self
                .client
                .post(format!(
                    "{scheme}://{querier}/querier.v1.QuerierService/LabelValues"
                ))
                .json(&json!({ "name": "service_name" })),
        };
        match tenant {
            Some(value) => request.header(
                TENANT_HEADER,
                HeaderValue::from_bytes(value).expect("a header value holds these bytes"),
            ),
            None => request,
        }
    }

    async fn call(&self, door: Door, tenant: Option<&[u8]>) -> Outcome {
        self.outcome(door, self.request(door, tenant)).await
    }

    /// The outcome of a request that presents `token` as its bearer credential.
    async fn call_as(&self, door: Door, tenant: Option<&[u8]>, token: &str) -> Outcome {
        self.outcome(door, self.request(door, tenant).bearer_auth(token))
            .await
    }

    async fn outcome(&self, door: Door, request: reqwest::RequestBuilder) -> Outcome {
        let response = request.send().await.expect("the door answers");
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = response.text().await.expect("the body is text");

        let answer = match (door, status.is_success()) {
            (Door::DistributorHttp | Door::DistributorConnect, true) => Answer::Accepted,
            (Door::QuerierHttp, true) => Answer::Served(render_tenant(&body)),
            (Door::QuerierConnect, true) => Answer::Served(label_values_tenant(&body)),
            (Door::DistributorHttp | Door::QuerierHttp, false) => Answer::HttpRejection {
                status: status.as_u16(),
                content_type,
                body,
            },
            (Door::DistributorConnect | Door::QuerierConnect, false) => {
                let error: Value = serde_json::from_str(&body).expect("a Connect error is JSON");
                let field = |name: &str| {
                    error
                        .get(name)
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string()
                };
                Answer::ConnectRejection {
                    status: status.as_u16(),
                    code: field("code"),
                    message: field("message"),
                }
            }
        };
        Outcome {
            answer,
            wal_tenants: self.sink.take_tenants(),
        }
    }

    /// The status, the body, and the WAL tenants of `request`, read before
    /// any door gives the answer its shape.
    async fn listener_answer(
        &self,
        request: reqwest::RequestBuilder,
    ) -> (u16, String, Vec<String>) {
        let response = request.send().await.expect("the listener answers");
        let status = response.status().as_u16();
        let body = response.text().await.expect("the body is text");
        (status, body, self.sink.take_tenants())
    }
}

#[tokio::test]
async fn every_door_resolves_the_same_header_to_the_same_answer() {
    let stack = Stack::start(TenantPolicy::anonymous()).await;
    let too_long = "x".repeat(151);
    let cases: [Case<'_>; 7] = [
        ("absent", None, Ok("anonymous")),
        ("empty", Some(b""), Ok("anonymous")),
        ("named", Some(b"tenant-a"), Ok("tenant-a")),
        (
            "separator",
            Some(b"a/b"),
            Err("tenant ID 'a/b' contains unsupported character '/'"),
        ),
        ("parent", Some(b".."), Err("tenant ID is '.' or '..'")),
        (
            "one byte too long",
            Some(too_long.as_bytes()),
            Err("tenant ID is too long: max 150 characters"),
        ),
        (
            "not UTF-8",
            Some(b"a\xff"),
            Err("tenant ID 'a\u{fffd}' contains unsupported character '\u{ff}'"),
        ),
    ];

    for door in DOORS {
        for (name, header, resolution) in cases {
            check!(
                stack.call(door, header).await == Outcome::expected(door, resolution),
                "{door:?}, {name}"
            );
        }
    }
}

// A value that is not UTF-8 once collapsed into a request without a tenant.
// The `anonymous` tenant has data on this querier, so that collapse would
// answer from it, and a push would land in the WAL under it.
#[tokio::test]
async fn a_malformed_tenant_is_never_served_as_anonymous() {
    let stack = Stack::start(TenantPolicy::anonymous()).await;

    for door in DOORS {
        for header in [&b"\xff"[..], b"anonymous\xff", b"a\x80b", b"anonymous/"] {
            let outcome = stack.call(door, Some(header)).await;
            check!(outcome.wal_tenants.is_empty(), "{door:?}, {header:?}");
            check!(
                !matches!(outcome.answer, Answer::Accepted | Answer::Served(_)),
                "{door:?}, {header:?}: {outcome:?}"
            );
        }
    }
}

// The distributor doors resolve under the policy on `DistributorState`, and
// not under a policy of their own.
#[tokio::test]
async fn the_distributor_resolves_a_push_without_a_tenant_under_the_state_policy() {
    let single = TenantId::new("single").expect("a valid tenant id");
    let fallback = Stack::start(TenantPolicy::Fallback(single)).await;
    let required = Stack::start(TenantPolicy::Required).await;

    for door in [Door::DistributorHttp, Door::DistributorConnect] {
        check!(
            fallback.call(door, None).await == Outcome::expected(door, Ok("single")),
            "{door:?}"
        );
        check!(
            required.call(door, None).await == Outcome::expected(door, Err("no org id")),
            "{door:?}"
        );
    }
}

// The grant covers the tenant after resolution. A request without the header
// resolves to `anonymous`, and the grant of `grafana` does not list it, so the
// fallback is no way around the grant. A denied push reaches no WAL.
#[tokio::test]
async fn a_secured_door_serves_the_granted_tenant_and_denies_every_other() {
    let pki = Pki::new();
    let stack = Stack::start_with(
        TenantPolicy::anonymous(),
        &pki.security(true),
        pki.https_client(),
        "https",
    )
    .await;
    let cases: [Case<'_>; 3] = [
        ("granted", Some(b"tenant-a"), Ok("tenant-a")),
        (
            "absent",
            None,
            Err(r#"principal "grafana" is not allowed to access tenant "anonymous""#),
        ),
        (
            "ungranted",
            Some(b"tenant-b"),
            Err(r#"principal "grafana" is not allowed to access tenant "tenant-b""#),
        ),
    ];

    for door in DOORS {
        for (name, header, answer) in cases {
            let expected = match answer {
                Ok(tenant) => Outcome::expected(door, Ok(tenant)),
                Err(message) => Outcome::denied(door, message),
            };
            check!(
                stack.call_as(door, header, GRAFANA_TOKEN).await == expected,
                "{door:?}, {name}"
            );
        }
    }
}

#[tokio::test]
async fn a_secured_door_refuses_a_missing_or_wrong_credential_and_ready_needs_none() {
    let pki = Pki::new();
    let stack = Stack::start_with(
        TenantPolicy::anonymous(),
        &pki.security(true),
        pki.https_client(),
        "https",
    )
    .await;
    let unauthorized = || (401, "unauthorized\n".to_string(), Vec::<String>::new());

    for door in DOORS {
        let missing = stack.request(door, Some(b"tenant-a"));
        let wrong = stack
            .request(door, Some(b"tenant-a"))
            .bearer_auth("grafana-0000000000000000000000000000000a");
        check!(
            stack.listener_answer(missing).await == unauthorized(),
            "{door:?}, missing"
        );
        check!(
            stack.listener_answer(wrong).await == unauthorized(),
            "{door:?}, wrong"
        );
    }
    for role in [stack.distributor, stack.querier] {
        let ready = stack.client.get(format!("https://{role}/ready"));
        check!(
            stack.listener_answer(ready).await == (200, "ready\n".to_string(), Vec::new()),
            "{role}"
        );
    }
}

// The listeners report through the audit handle that the binary installs. A
// request that succeeds records nothing.
#[tokio::test]
async fn a_refused_credential_and_a_denied_tenant_each_become_one_audit_event() {
    let pki = Pki::new();
    let (log, mut queue) = AuditLog::new(8);
    let audit = AuditHandle::new(log, Arc::new(AuditStats::new()), Arc::new(FixedClock));
    let security = pki.security(false).with_security_events(Arc::new(audit));
    let stack = Stack::start_with(
        TenantPolicy::anonymous(),
        &security,
        reqwest::Client::new(),
        "http",
    )
    .await;

    // A raw connection from a port that the test knows, so the source of the
    // event is exact.
    let socket = TcpSocket::new_v4().expect("a socket");
    socket
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("a free port");
    let source = socket.local_addr().expect("the socket has an address");
    let mut connection = socket
        .connect(stack.querier)
        .await
        .expect("the querier accepts");
    connection
        .write_all(b"GET /pyroscope/render HTTP/1.1\r\nhost: profiles\r\nconnection: close\r\n\r\n")
        .await
        .expect("the request is written");
    let mut refused = String::new();
    connection
        .read_to_string(&mut refused)
        .await
        .expect("the response is read");
    let denied = stack
        .call_as(Door::QuerierHttp, Some(b"tenant-b"), GRAFANA_TOKEN)
        .await;
    let served = stack
        .call_as(Door::QuerierHttp, Some(b"tenant-a"), GRAFANA_TOKEN)
        .await;

    check!(refused.starts_with("HTTP/1.1 401 "));
    check!(
        denied
            == Outcome::denied(
                Door::QuerierHttp,
                r#"principal "grafana" is not allowed to access tenant "tenant-b""#
            )
    );
    check!(served == Outcome::expected(Door::QuerierHttp, Ok("tenant-a")));
    let mut events = Vec::new();
    while let Ok(event) = queue.try_recv() {
        events.push(event);
    }
    check!(
        events
            == vec![
                authentication(
                    AuditOutcome::Failure,
                    MECHANISM_NONE,
                    unauthenticated_principal(),
                    source_endpoint(source),
                    Some("missing_credential".to_owned()),
                    EpochMs(EVENT_TIME_MS),
                ),
                authorization_denied(
                    principal("grafana", MECHANISM_BEARER),
                    unknown_source_endpoint(),
                    RESOURCE_TENANT,
                    "tenant-b",
                    OPERATION_TENANT_ACCESS,
                    EpochMs(EVENT_TIME_MS),
                ),
            ]
    );
}

/// A clock that gives every audit event the same time.
struct FixedClock;

impl qubit_clock::Clock for FixedClock {
    fn millis(&self) -> i64 {
        EVENT_TIME_MS
    }
}

/// A CA and a directory for the files that the security flags name.
struct Pki {
    dir: TempDir,
    authority: CertifiedIssuer<'static, KeyPair>,
}

impl Pki {
    fn new() -> Self {
        install_crypto_provider();
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("valid parameters");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(DnType::CommonName, "krabka profiles test ca");
        Self {
            dir: TempDir::new().expect("a temporary directory"),
            authority: CertifiedIssuer::self_signed(params, KeyPair::generate().expect("a key"))
                .expect("a CA"),
        }
    }

    fn write(&self, name: &str, contents: impl AsRef<[u8]>) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, contents).expect("the temporary file is writable");
        path
    }

    /// A credentials file in which `grafana` may use `tenant-a` only, and TLS
    /// with a server certificate from this CA when `tls` is set.
    fn security(&self, tls: bool) -> ServerSecurity {
        let credentials = format!(
            "principals:\n  - name: grafana\n    token_sha256: [{}]\n    tenants: [tenant-a]\n",
            sha256_hex(GRAFANA_TOKEN),
        );
        let (certificate, key) = if tls {
            let mut params =
                CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
                    .expect("valid parameters");
            params
                .distinguished_name
                .push(DnType::CommonName, "krabka-profiles");
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            let key = KeyPair::generate().expect("a key");
            let certificate = params
                .signed_by(&key, &self.authority)
                .expect("a signed server certificate");
            (
                Some(self.write("server.pem", certificate.pem())),
                Some(self.write("server-key.pem", key.serialize_pem())),
            )
        } else {
            (None, None)
        };
        ServerSecurityArgs {
            server_tls_cert_path: certificate,
            server_tls_key_path: key,
            server_tls_client_ca_path: None,
            server_tls_client_auth: ClientAuth::NoClientCert,
            server_tls_handshake_timeout: krabka_units::secs(10),
            auth_credentials_config: Some(self.write("credentials.yaml", credentials)),
            internal_client_token_path: None,
            internal_client_tls_cert_path: None,
            internal_client_tls_key_path: None,
            internal_client_tls_ca_path: None,
        }
        .load()
        .expect("the security flags load")
    }

    /// A client that trusts only this CA.
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

fn sha256_hex(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
            hex
        })
}

/// The tenant whose frame the render flamebearer holds.
fn render_tenant(body: &str) -> String {
    let render: Value = serde_json::from_str(body).expect("the render body is JSON");
    let names = render
        .pointer("/flamebearer/names")
        .and_then(Value::as_array)
        .expect("the flamebearer has names");
    names
        .iter()
        .filter_map(Value::as_str)
        .filter(|name| *name != "total")
        .collect::<Vec<_>>()
        .join(",")
}

/// The tenant whose `service_name` the label values hold.
fn label_values_tenant(body: &str) -> String {
    let values: Value = serde_json::from_str(body).expect("the label values body is JSON");
    values
        .get("names")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

/// A store with one sample for `anonymous` and one for `tenant-a`. Each
/// sample's frame and `service_name` are the tenant's own name, so a query
/// answer names the tenant it came from.
fn store_with_one_frame_per_tenant() -> InMemoryProfileStore {
    let mut store = InMemoryProfileStore::new();
    for tenant in ["anonymous", "tenant-a"] {
        let name = store.symbols_mut().intern_string(tenant);
        let function_id = store.symbols_mut().intern_function(FunctionRec {
            name,
            system_name: name,
            filename: 0,
            start_line: 0,
        });
        let location_id = store.symbols_mut().intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        });
        let stacktrace = store.symbols_mut().intern_stacktrace(0, &[location_id]);
        store.push_sample(
            (tenant, PROFILE_TYPE),
            vec![("service_name".to_string(), tenant.to_string())],
            (0, stacktrace),
            7,
            10,
        );
    }
    store
}

fn push_body() -> Value {
    json!({
        "series": [{
            "labels": [
                { "name": "__name__", "value": "process_cpu" },
                { "name": "service_name", "value": "api" }
            ],
            "samples": [{ "rawProfile": BASE64.encode(gzipped_cpu_profile()), "ID": "tenant-resolution" }]
        }]
    })
}

/// A one-sample CPU profile, gzipped as the `push.v1` door expects it.
fn gzipped_cpu_profile() -> Vec<u8> {
    let profile = proto::Profile {
        sample_type: vec![proto::ValueType { r#type: 1, unit: 2 }],
        sample: vec![proto::Sample {
            location_id: vec![1],
            value: vec![5],
            label: Vec::new(),
        }],
        location: vec![proto::Location {
            id: 1,
            line: vec![proto::Line {
                function_id: 1,
                line: 1,
                column: 0,
            }],
            ..Default::default()
        }],
        function: vec![proto::Function {
            id: 1,
            name: 3,
            system_name: 3,
            filename: 0,
            start_line: 1,
        }],
        string_table: vec![
            String::new(),
            "cpu".to_string(),
            "nanoseconds".to_string(),
            "main.work".to_string(),
        ],
        time_nanos: 1_700_000_000_000_000_000,
        period_type: Some(proto::ValueType { r#type: 1, unit: 2 }),
        period: 10_000_000,
        ..Default::default()
    };
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&PprofProfile::from(profile).encode())
        .expect("gzip write");
    encoder.finish().expect("gzip finish")
}
