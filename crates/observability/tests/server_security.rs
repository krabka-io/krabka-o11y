//! The server security layer against real sockets.
//!
//! Every test here binds `127.0.0.1:0`, serves through `ServerListener`, and
//! talks to it with a real client: `reqwest` for HTTP, tonic for gRPC, and a
//! raw `TcpStream` for the client that never finishes its handshake. The
//! certificates are generated per test with `rcgen`, so no key material is in
//! the repository.

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    future::IntoFuture as _,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use assert2::assert;
use async_trait::async_trait;
use axum::{
    Router,
    extract::{ConnectInfo, Request, State},
    http::{StatusCode, header},
    routing::get,
};
use clap::Parser;
use krabka_blockstore::TenantId;
use krabka_observability::{
    CancellationToken,
    server_security::{
        AuthFailureReason, AuthMethod, ClientIdentity, GrpcAuthenticationLayer, PeerAddr,
        Principal, SecurityEventSink, SecurityEvents, ServerListener, ServerSecurity,
        ServerSecurityArgs, TenantGrant, grpc_incoming, serve_router,
    },
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_client::LogsServiceClient,
    logs_service_server::{LogsService, LogsServiceServer},
};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{io::AsyncReadExt as _, net::TcpListener};

const GRAFANA_TOKEN: &str = "grafana-7c1f0e9a4b2d8e6f3a5c7b9d1e0f2a4c";
const OPS_TOKEN: &str = "ops-2b4d6f8a0c1e3a5b7c9d0e2f4a6b8c0d";

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    security: ServerSecurityArgs,
}

fn load(flags: &[String]) -> ServerSecurity {
    let argv = std::iter::once("test".to_owned()).chain(flags.iter().cloned());
    Cli::try_parse_from(argv)
        .expect("the flags parse")
        .security
        .load()
        .expect("the flags load")
}

fn sha256_hex(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, byte| {
            write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
            hex
        })
}

fn tenant(id: &str) -> TenantId {
    TenantId::new(id).expect("a valid tenant id")
}

/// A CA, a server certificate signed by it, and a directory of PEM files.
struct Pki {
    dir: TempDir,
    authority: CertifiedIssuer<'static, KeyPair>,
}

struct Pem {
    certificate: String,
    key: String,
}

impl Pki {
    fn new() -> Self {
        Self {
            dir: TempDir::new().expect("a temporary directory"),
            authority: authority("krabka test ca"),
        }
    }

    fn write(&self, name: &str, contents: impl AsRef<[u8]>) -> String {
        let path: PathBuf = self.dir.path().join(name);
        std::fs::write(&path, contents).expect("the temporary file is writable");
        path.display().to_string()
    }

    fn client(&self, common_name: &str, names: &[&str]) -> Pem {
        leaf(
            &self.authority,
            common_name,
            names,
            ExtendedKeyUsagePurpose::ClientAuth,
        )
    }

    /// The TLS flags for a server certificate signed by this CA, which also
    /// trusts this CA for client certificates unless `client_auth` is
    /// `NoClientCert`.
    fn server_flags(&self, client_auth: &str, handshake_timeout: &str) -> Vec<String> {
        let server = leaf(
            &self.authority,
            "server",
            &["localhost", "127.0.0.1"],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let mut flags = vec![
            "--server-tls-cert-path".to_owned(),
            self.write("server.pem", server.certificate),
            "--server-tls-key-path".to_owned(),
            self.write("server-key.pem", server.key),
            "--server-tls-client-auth".to_owned(),
            client_auth.to_owned(),
            "--server-tls-handshake-timeout".to_owned(),
            handshake_timeout.to_owned(),
        ];
        if client_auth != "NoClientCert" {
            flags.push("--server-tls-client-ca-path".to_owned());
            flags.push(self.write("client-ca.pem", self.authority.pem()));
        }
        flags
    }

    fn https_client(&self, identity: Option<&Pem>) -> reqwest::Client {
        let mut builder =
            reqwest::Client::builder().tls_certs_only([reqwest::Certificate::from_pem(
                self.authority.pem().as_bytes(),
            )
            .expect("the CA parses")]);
        if let Some(pem) = identity {
            let identity =
                reqwest::Identity::from_pem(format!("{}{}", pem.certificate, pem.key).as_bytes())
                    .expect("the identity parses");
            builder = builder.identity(identity);
        }
        builder.build().expect("the client builds")
    }
}

fn authority(common_name: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("valid parameters");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    CertifiedIssuer::self_signed(params, KeyPair::generate().expect("a key")).expect("a CA")
}

fn leaf(
    authority: &CertifiedIssuer<'static, KeyPair>,
    common_name: &str,
    names: &[&str],
    usage: ExtendedKeyUsagePurpose,
) -> Pem {
    let mut params = CertificateParams::new(
        names
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<Vec<_>>(),
    )
    .expect("valid parameters");
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.extended_key_usages = vec![usage];
    let key = KeyPair::generate().expect("a key");
    let certificate = params.signed_by(&key, authority).expect("a signed leaf");
    Pem {
        certificate: certificate.pem(),
        key: key.serialize_pem(),
    }
}

/// The principal and peer that one handler saw.
type Seen = Arc<Mutex<Vec<(Option<Principal>, Option<PeerAddr>)>>>;

async fn record(State(seen): State<Seen>, request: Request) -> &'static str {
    let principal = request.extensions().get::<Principal>().cloned();
    let peer = request
        .extensions()
        .get::<ConnectInfo<PeerAddr>>()
        .map(|ConnectInfo(peer)| peer.clone());
    seen.lock()
        .expect("no handler panics while holding the lock")
        .push((principal, peer));
    "ok"
}

fn take(seen: &Seen) -> Vec<(Option<Principal>, Option<PeerAddr>)> {
    std::mem::take(
        &mut *seen
            .lock()
            .expect("no handler panics while holding the lock"),
    )
}

/// A served router, stopped when the value is dropped.
struct Server {
    addr: SocketAddr,
    seen: Seen,
    stop: CancellationToken,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

async fn serve(security: &ServerSecurity) -> Server {
    let seen = Seen::default();
    let router = Router::new()
        .route("/whoami", get(record))
        .route("/ready", get(record).post(record))
        .route("/metrics", get(record))
        .with_state(seen.clone());
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
    let listener = ServerListener::bind(tcp, security).expect("the listener binds");
    let addr = listener.local_addr();
    let stop = CancellationToken::new();
    tokio::spawn(
        serve_router(listener, router, security)
            .with_graceful_shutdown(stop.clone().cancelled_owned())
            .into_future(),
    );
    Server { addr, seen, stop }
}

/// Every event, as text, so a test can compare whole sequences and search
/// them for credential bytes.
#[derive(Default)]
struct RecordedEvents(Mutex<Vec<String>>);

impl RecordedEvents {
    fn take(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .expect("no test panics while holding the lock"),
        )
    }

    fn push(&self, event: String) {
        self.0
            .lock()
            .expect("no test panics while holding the lock")
            .push(event);
    }
}

impl SecurityEvents for RecordedEvents {
    fn authentication_failed(
        &self,
        source: Option<SocketAddr>,
        attempted: Option<AuthMethod>,
        reason: AuthFailureReason,
    ) {
        let source = source.map(|source| source.ip());
        self.push(format!("failed {source:?} {attempted:?} {reason:?}"));
    }

    fn authentication_succeeded(
        &self,
        source: Option<SocketAddr>,
        principal: &str,
        method: AuthMethod,
    ) {
        let source = source.map(|source| source.ip());
        self.push(format!("succeeded {source:?} {principal} {method:?}"));
    }

    fn tenant_denied(&self, principal: &str, _method: AuthMethod, tenant: &TenantId) {
        self.push(format!("tenant denied {principal} {tenant}"));
    }

    fn admin_denied(&self, principal: &str, _method: AuthMethod) {
        self.push(format!("admin denied {principal}"));
    }
}

fn credentials_flags(pki: &Pki) -> Vec<String> {
    let yaml = format!(
        "principals:\n  - name: grafana\n    token_sha256: [{}]\n    client_certificates: [grafana.internal]\n    tenants: [tenant-a]\n  - name: ops\n    token_sha256: [{}]\n    client_certificates: [ops]\n    tenants: ['*']\n    admin: true\n",
        sha256_hex(GRAFANA_TOKEN),
        sha256_hex(OPS_TOKEN),
    );
    vec![
        "--auth-credentials-config".to_owned(),
        pki.write("credentials.yaml", yaml),
    ]
}

fn grafana(method: AuthMethod, events: &SecurityEventSink) -> Principal {
    Principal::Authenticated {
        name: Arc::from("grafana"),
        method,
        tenants: TenantGrant::Only(Arc::new(BTreeSet::from([tenant("tenant-a")]))),
        admin: false,
        events: events.clone(),
    }
}

fn ops(method: AuthMethod, events: &SecurityEventSink) -> Principal {
    Principal::Authenticated {
        name: Arc::from("ops"),
        method,
        tenants: TenantGrant::All,
        admin: true,
        events: events.clone(),
    }
}

#[tokio::test]
async fn an_unconfigured_listener_serves_plain_http_to_an_unauthenticated_principal() {
    let server = serve(&load(&[])).await;
    let client = reqwest::Client::new();

    let plain = client
        .get(format!("http://{}/whoami", server.addr))
        .send()
        .await
        .expect("plain HTTP is served");
    let with_bogus_credential = client
        .get(format!("http://{}/whoami", server.addr))
        .bearer_auth("not-a-configured-token")
        .send()
        .await
        .expect("plain HTTP is served");

    assert!(plain.status() == StatusCode::OK);
    assert!(with_bogus_credential.status() == StatusCode::OK);
    let seen = take(&server.seen);
    assert!(seen.len() == 2);
    for (principal, peer) in seen {
        let peer = peer.expect("the router is served with connect info");
        assert!(principal == Some(Principal::Unauthenticated));
        assert!(
            peer == PeerAddr {
                socket: SocketAddr::new([127, 0, 0, 1].into(), peer.socket.port()),
                client_identity: None,
            }
        );
    }
}

#[tokio::test]
async fn a_tls_listener_serves_a_client_that_trusts_its_ca_and_refuses_plain_http() {
    let pki = Pki::new();
    let server = serve(&load(&pki.server_flags("NoClientCert", "10s"))).await;

    let over_tls = pki
        .https_client(None)
        .get(format!("https://{}/whoami", server.addr))
        .send()
        .await
        .expect("a client that trusts the CA is served");
    let over_plain_http = reqwest::Client::new()
        .get(format!("http://{}/whoami", server.addr))
        .send()
        .await;
    let untrusting = reqwest::Client::builder()
        .tls_certs_only([
            reqwest::Certificate::from_pem(authority("another ca").pem().as_bytes())
                .expect("the CA parses"),
        ])
        .build()
        .expect("the client builds")
        .get(format!("https://{}/whoami", server.addr))
        .send()
        .await;

    assert!(over_tls.status() == StatusCode::OK);
    assert!(over_plain_http.is_err());
    assert!(untrusting.is_err());
    let seen = take(&server.seen);
    assert!(seen.len() == 1);
    assert!(
        seen[0]
            .1
            .as_ref()
            .and_then(|peer| peer.client_identity.clone())
            == None
    );
}

#[tokio::test]
async fn require_and_verify_client_cert_serves_only_a_certificate_from_the_client_ca() {
    let pki = Pki::new();
    let server = serve(&load(
        &pki.server_flags("RequireAndVerifyClientCert", "10s"),
    ))
    .await;
    let trusted = pki.client("grafana", &["grafana.internal"]);
    let foreign = leaf(
        &authority("another ca"),
        "grafana",
        &["grafana.internal"],
        ExtendedKeyUsagePurpose::ClientAuth,
    );
    let url = format!("https://{}/whoami", server.addr);

    let without_certificate = pki.https_client(None).get(&url).send().await;
    let with_foreign_certificate = pki.https_client(Some(&foreign)).get(&url).send().await;
    let with_trusted_certificate = pki
        .https_client(Some(&trusted))
        .get(&url)
        .send()
        .await
        .expect("a certificate from the client CA is served");

    assert!(without_certificate.is_err());
    assert!(with_foreign_certificate.is_err());
    assert!(with_trusted_certificate.status() == StatusCode::OK);
    let seen = take(&server.seen);
    assert!(seen.len() == 1);
    let identity = seen[0]
        .1
        .as_ref()
        .and_then(|peer| peer.client_identity.as_deref().cloned());
    assert!(
        identity
            == Some(ClientIdentity {
                common_name: Some("grafana".to_owned()),
                dns_names: vec!["grafana.internal".to_owned()],
                uris: Vec::new(),
            })
    );
}

#[tokio::test]
async fn request_client_cert_serves_a_client_without_a_certificate_with_no_identity() {
    let pki = Pki::new();
    let server = serve(&load(&pki.server_flags("RequestClientCert", "10s"))).await;
    let trusted = pki.client("grafana", &[]);
    let url = format!("https://{}/whoami", server.addr);

    let without_certificate = pki
        .https_client(None)
        .get(&url)
        .send()
        .await
        .expect("a client without a certificate is served");
    let with_certificate = pki
        .https_client(Some(&trusted))
        .get(&url)
        .send()
        .await
        .expect("a client with a trusted certificate is served");

    assert!(without_certificate.status() == StatusCode::OK);
    assert!(with_certificate.status() == StatusCode::OK);
    let identities: Vec<_> = take(&server.seen)
        .into_iter()
        .map(|(_, peer)| peer.and_then(|peer| peer.client_identity.as_deref().cloned()))
        .collect();
    assert!(
        identities
            == vec![
                None,
                Some(ClientIdentity {
                    common_name: Some("grafana".to_owned()),
                    dns_names: Vec::new(),
                    uris: Vec::new(),
                }),
            ]
    );
}

#[tokio::test]
async fn a_client_that_never_finishes_its_handshake_does_not_stop_another_client() {
    let pki = Pki::new();
    // A handshake timeout far longer than the test's own deadline: if the
    // listener handshook inside `accept`, the second client would wait for it.
    let server = serve(&load(&pki.server_flags("NoClientCert", "5m"))).await;
    let _stalled = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("the TCP connection opens");

    let second = tokio::time::timeout(
        Duration::from_secs(10),
        pki.https_client(None)
            .get(format!("https://{}/whoami", server.addr))
            .send(),
    )
    .await;

    let response = second
        .expect("the second client is served while the first one stalls")
        .expect("the second client is served");
    assert!(response.status() == StatusCode::OK);
}

#[tokio::test]
async fn a_stalled_handshake_is_dropped_after_the_handshake_timeout() {
    let pki = Pki::new();
    let server = serve(&load(&pki.server_flags("NoClientCert", "300ms"))).await;
    let started = Instant::now();
    let mut stalled = tokio::net::TcpStream::connect(server.addr)
        .await
        .expect("the TCP connection opens");

    let mut buffer = [0_u8; 16];
    let read = tokio::time::timeout(Duration::from_secs(10), stalled.read(&mut buffer))
        .await
        .expect("the server closes the stalled connection");

    // A closed connection reads as end of file, or as a reset.
    assert!(matches!(read, Ok(0) | Err(_)));
    assert!(started.elapsed() >= Duration::from_millis(300));
}

#[tokio::test]
async fn dropping_a_tls_listener_stops_its_accept_task_and_closes_the_socket() {
    let pki = Pki::new();
    let security = load(&pki.server_flags("NoClientCert", "10s"));
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
    let listener = ServerListener::bind(tcp, &security).expect("the listener binds");
    let addr = listener.local_addr();

    drop(listener);

    let deadline = Instant::now() + Duration::from_secs(10);
    let closed = loop {
        if tokio::net::TcpStream::connect(addr).await.is_err() {
            break true;
        }
        if Instant::now() > deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(closed);
}

#[tokio::test]
async fn each_credential_kind_reaches_the_handler_as_its_principal() {
    let pki = Pki::new();
    let events = Arc::new(RecordedEvents::default());
    let mut flags = pki.server_flags("RequestClientCert", "10s");
    flags.extend(credentials_flags(&pki));
    let security = load(&flags).with_security_events(events.clone());
    let sink = SecurityEventSink::new(events.clone());
    let server = serve(&security).await;
    let url = format!("https://{}/whoami", server.addr);
    let ops_certificate = pki.client("ops", &[]);

    let bearer = pki
        .https_client(None)
        .get(&url)
        .bearer_auth(GRAFANA_TOKEN)
        .send();
    let bearer = bearer.await.expect("a bearer request is served");
    let basic = pki
        .https_client(None)
        .get(&url)
        .basic_auth("grafana", Some(GRAFANA_TOKEN))
        .send()
        .await
        .expect("a basic request is served");
    let certificate = pki
        .https_client(Some(&ops_certificate))
        .get(&url)
        .send()
        .await
        .expect("a client certificate request is served");

    assert!(bearer.status() == StatusCode::OK);
    assert!(basic.status() == StatusCode::OK);
    assert!(certificate.status() == StatusCode::OK);
    let principals: Vec<_> = take(&server.seen)
        .into_iter()
        .map(|(principal, _)| principal)
        .collect();
    assert!(
        principals
            == vec![
                Some(grafana(AuthMethod::Bearer, &sink)),
                Some(grafana(AuthMethod::Basic, &sink)),
                Some(ops(AuthMethod::ClientCertificate, &sink)),
            ]
    );
    assert!(
        events.take()
            == vec![
                "succeeded Some(127.0.0.1) grafana Bearer".to_owned(),
                "succeeded Some(127.0.0.1) grafana Basic".to_owned(),
                "succeeded Some(127.0.0.1) ops ClientCertificate".to_owned(),
            ]
    );
}

#[tokio::test]
async fn every_rejected_request_gets_the_same_401_and_one_failure_event() {
    let pki = Pki::new();
    let events = Arc::new(RecordedEvents::default());
    let security = load(&credentials_flags(&pki)).with_security_events(events.clone());
    let server = serve(&security).await;
    let url = format!("http://{}/whoami", server.addr);
    let client = reqwest::Client::new();

    let requests = [
        (
            "wrong token",
            client
                .get(&url)
                .bearer_auth("grafana-7c1f0e9a4b2d8e6f3a5c7b9d1e0f2a4d"),
        ),
        (
            "wrong basic username",
            client.get(&url).basic_auth("ops", Some(GRAFANA_TOKEN)),
        ),
        ("missing credential", client.get(&url)),
        (
            "unknown path",
            client.get(format!("http://{}/no-such-route", server.addr)),
        ),
    ];
    for (name, request) in requests {
        let response = request.send().await.expect("the request is answered");
        let status = response.status();
        let challenge = response.headers().get(header::WWW_AUTHENTICATE).cloned();
        let body = response.text().await.expect("a body");
        assert!(status == StatusCode::UNAUTHORIZED, "{name}");
        assert!(
            challenge.as_ref().map(|value| value.to_str().ok())
                == Some(Some(r#"Basic realm="krabka", Bearer realm="krabka""#)),
            "{name}"
        );
        assert!(body == "unauthorized\n", "{name}");
    }

    assert!(take(&server.seen).is_empty());
    let recorded = events.take();
    assert!(
        recorded
            == vec![
                "failed Some(127.0.0.1) Some(Bearer) UnknownCredential".to_owned(),
                "failed Some(127.0.0.1) Some(Basic) UnknownCredential".to_owned(),
                "failed Some(127.0.0.1) None MissingCredential".to_owned(),
                "failed Some(127.0.0.1) None MissingCredential".to_owned(),
            ]
    );
    assert!(recorded.iter().all(|event| !event.contains("grafana-7c1f")));
}

#[tokio::test]
async fn ready_and_metrics_skip_authentication_but_post_ready_does_not() {
    let pki = Pki::new();
    let server = serve(&load(&credentials_flags(&pki))).await;
    let client = reqwest::Client::new();

    let ready = client
        .get(format!("http://{}/ready", server.addr))
        .send()
        .await;
    let metrics = client
        .get(format!("http://{}/metrics", server.addr))
        .send()
        .await;
    let post_ready = client
        .post(format!("http://{}/ready", server.addr))
        .send()
        .await;

    assert!(ready.expect("answered").status() == StatusCode::OK);
    assert!(metrics.expect("answered").status() == StatusCode::OK);
    assert!(post_ready.expect("answered").status() == StatusCode::UNAUTHORIZED);
    let principals: Vec<_> = take(&server.seen)
        .into_iter()
        .map(|(principal, _)| principal)
        .collect();
    assert!(principals == vec![None, None]);
}

/// Security events that panic, which puts a panic inside the authentication
/// layer itself.
struct PanickingEvents;

impl SecurityEvents for PanickingEvents {
    fn authentication_failed(
        &self,
        _: Option<SocketAddr>,
        _: Option<AuthMethod>,
        _: AuthFailureReason,
    ) {
        panic!("an events sink with a bug");
    }

    fn authentication_succeeded(&self, _: Option<SocketAddr>, _: &str, _: AuthMethod) {
        panic!("an events sink with a bug");
    }

    fn tenant_denied(&self, _: &str, _: AuthMethod, _: &TenantId) {}

    fn admin_denied(&self, _: &str, _: AuthMethod) {}
}

#[tokio::test]
async fn a_panic_inside_the_authentication_layer_is_contained_as_a_500() {
    let pki = Pki::new();
    let security = load(&credentials_flags(&pki)).with_security_events(Arc::new(PanickingEvents));
    let server = serve(&security).await;
    let client = reqwest::Client::new();

    let first = client
        .get(format!("http://{}/whoami", server.addr))
        .bearer_auth(GRAFANA_TOKEN)
        .send()
        .await
        .expect("the panic is answered, not a dropped connection");
    let second = client
        .get(format!("http://{}/ready", server.addr))
        .send()
        .await
        .expect("the listener still serves");

    assert!(first.status() == StatusCode::INTERNAL_SERVER_ERROR);
    assert!(second.status() == StatusCode::OK);
}

/// Answers every export with the principal it saw, in the partial-success
/// message.
struct EchoPrincipal;

#[async_trait]
impl LogsService for EchoPrincipal {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        let error_message = match request.extensions().get::<Principal>() {
            Some(Principal::Authenticated { name, method, .. }) => format!("{name} {method:?}"),
            Some(Principal::Unauthenticated) => "unauthenticated".to_owned(),
            None => "no principal".to_owned(),
        };
        Ok(tonic::Response::new(ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 0,
                error_message,
            }),
        }))
    }
}

async fn serve_grpc(security: &ServerSecurity) -> (SocketAddr, CancellationToken) {
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
    let listener = ServerListener::bind(tcp, security).expect("the listener binds");
    let addr = listener.local_addr();
    let stop = CancellationToken::new();
    let server = tonic::transport::Server::builder()
        .layer(GrpcAuthenticationLayer::new(security))
        .add_service(LogsServiceServer::new(EchoPrincipal))
        .serve_with_incoming_shutdown(grpc_incoming(listener), stop.clone().cancelled_owned());
    tokio::spawn(server);
    (addr, stop)
}

async fn export(
    endpoint: tonic::transport::Endpoint,
    authorization: Option<&str>,
) -> Result<String, tonic::Code> {
    let channel = endpoint.connect().await.expect("the gRPC channel connects");
    let mut request = tonic::Request::new(ExportLogsServiceRequest::default());
    if let Some(authorization) = authorization {
        request.metadata_mut().insert(
            "authorization",
            authorization.parse().expect("valid metadata"),
        );
    }
    let response = LogsServiceClient::new(channel)
        .export(request)
        .await
        .map_err(|status| status.code())?;
    Ok(response
        .into_inner()
        .partial_success
        .map(|success| success.error_message)
        .unwrap_or_default())
}

#[tokio::test]
async fn the_grpc_layer_admits_a_correct_bearer_token_and_refuses_a_wrong_one() {
    let pki = Pki::new();
    let (addr, _stop) = serve_grpc(&load(&credentials_flags(&pki))).await;
    let endpoint = || {
        tonic::transport::Endpoint::from_shared(format!("http://{addr}")).expect("a valid endpoint")
    };

    let correct = export(endpoint(), Some(&format!("Bearer {GRAFANA_TOKEN}"))).await;
    let wrong = export(endpoint(), Some("Bearer not-the-token")).await;
    let missing = export(endpoint(), None).await;

    assert!(correct.as_deref() == Ok("grafana Bearer"));
    assert!(wrong == Err(tonic::Code::Unauthenticated));
    assert!(missing == Err(tonic::Code::Unauthenticated));
}

#[tokio::test]
async fn an_unconfigured_grpc_server_gives_every_request_an_unauthenticated_principal() {
    let (addr, _stop) = serve_grpc(&load(&[])).await;
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("a valid endpoint");

    let answer = export(endpoint, Some("Bearer ignored")).await;

    assert!(answer.as_deref() == Ok("unauthenticated"));
}

#[tokio::test]
async fn the_grpc_layer_reads_the_verified_client_certificate_over_tls() {
    let pki = Pki::new();
    let mut flags = pki.server_flags("RequireAndVerifyClientCert", "10s");
    flags.extend(credentials_flags(&pki));
    let (addr, _stop) = serve_grpc(&load(&flags)).await;
    let ops_certificate = pki.client("ops", &[]);
    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(pki.authority.pem()))
        .identity(tonic::transport::Identity::from_pem(
            ops_certificate.certificate,
            ops_certificate.key,
        ))
        .domain_name("localhost");
    let endpoint = tonic::transport::Endpoint::from_shared(format!("https://{addr}"))
        .expect("a valid endpoint")
        .tls_config(tls)
        .expect("a valid TLS config");

    let answer = export(endpoint, None).await;

    assert!(answer.as_deref() == Ok("ops ClientCertificate"));
}

#[tokio::test]
async fn the_internal_client_is_served_with_its_identity_and_refused_without_it() {
    let pki = Pki::new();
    let events = Arc::new(RecordedEvents::default());
    let mut flags = pki.server_flags("RequireAndVerifyClientCert", "10s");
    flags.extend(credentials_flags(&pki));
    let security = load(&flags).with_security_events(events.clone());
    let sink = SecurityEventSink::new(events.clone());
    let server = serve(&security).await;
    let url = format!("https://{}/whoami", server.addr);

    let ops_certificate = pki.client("ops", &[]);
    let certificate = pki.write("internal.pem", &ops_certificate.certificate);
    let key = pki.write("internal-key.pem", &ops_certificate.key);
    let ca = pki.write("internal-ca.pem", pki.authority.pem());
    let token = pki.write("internal-token", format!("{GRAFANA_TOKEN}\n"));
    let internal = |flags: &[&str]| {
        let flags: Vec<String> = flags.iter().map(|flag| (*flag).to_owned()).collect();
        load(&flags)
            .internal_client()
            .apply(reqwest::Client::builder())
            .build()
            .expect("the internal client builds")
    };
    let identity_only = internal(&[
        "--internal-client-tls-cert-path",
        &certificate,
        "--internal-client-tls-key-path",
        &key,
        "--internal-client-tls-ca-path",
        &ca,
    ]);
    let identity_and_token = internal(&[
        "--internal-client-tls-cert-path",
        &certificate,
        "--internal-client-tls-key-path",
        &key,
        "--internal-client-tls-ca-path",
        &ca,
        "--internal-client-token-path",
        &token,
    ]);
    let without_identity = internal(&[
        "--internal-client-tls-ca-path",
        &ca,
        "--internal-client-token-path",
        &token,
    ]);

    let by_certificate = identity_only.get(&url).send().await.expect("served");
    let by_token = identity_and_token.get(&url).send().await.expect("served");
    let refused = without_identity.get(&url).send().await;

    assert!(by_certificate.status() == StatusCode::OK);
    assert!(by_token.status() == StatusCode::OK);
    assert!(refused.is_err());
    let principals: Vec<_> = take(&server.seen)
        .into_iter()
        .map(|(principal, _)| principal)
        .collect();
    assert!(
        principals
            == vec![
                Some(ops(AuthMethod::ClientCertificate, &sink)),
                Some(grafana(AuthMethod::Bearer, &sink)),
            ]
    );
}
