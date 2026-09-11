use std::{
    collections::BTreeSet,
    fmt::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use assert2::assert;
use axum::{
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use clap::Parser;
use krabka_blockstore::TenantId;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use super::{
    AdminDenied, AuthFailureReason, AuthMethod, ClientIdentity, PeerAddr, Principal,
    SecurityEventSink, SecurityEvents, ServerSecurity, ServerSecurityArgs, ServerSecurityError,
    TenantDenied, TenantGrant, UnauthenticatedRoutes, authenticator::Authenticator,
    authorize_admin, authorize_tenant, credentials::Credentials, token_digest::TokenDigest,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    security: ServerSecurityArgs,
}

fn args(flags: &[&str]) -> ServerSecurityArgs {
    let argv = std::iter::once("test").chain(flags.iter().copied());
    Cli::try_parse_from(argv).expect("the flags parse").security
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
        self.push(format!("failed {source:?} {attempted:?} {reason:?}"));
    }

    fn authentication_succeeded(
        &self,
        source: Option<SocketAddr>,
        principal: &str,
        method: AuthMethod,
    ) {
        self.push(format!("succeeded {source:?} {principal} {method:?}"));
    }

    fn tenant_denied(&self, principal: &str, _method: AuthMethod, tenant: &TenantId) {
        self.push(format!("tenant denied {principal} {tenant}"));
    }

    fn admin_denied(&self, principal: &str, _method: AuthMethod) {
        self.push(format!("admin denied {principal}"));
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

fn tenant(id: &str) -> TenantId {
    TenantId::new(id).expect("a valid tenant id")
}

struct Pki {
    dir: TempDir,
}

impl Pki {
    fn new() -> Self {
        Self {
            dir: TempDir::new().expect("a temporary directory"),
        }
    }

    fn write(&self, name: &str, contents: impl AsRef<[u8]>) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, contents).expect("the temporary file is writable");
        path
    }

    fn authority(common_name: &str) -> CertifiedIssuer<'static, KeyPair> {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("valid parameters");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        CertifiedIssuer::self_signed(params, KeyPair::generate().expect("a key"))
            .expect("a self-signed CA")
    }

    /// A leaf certificate and key signed by `authority`, as PEM.
    fn leaf(
        authority: &CertifiedIssuer<'static, KeyPair>,
        common_name: &str,
        names: &[&str],
        usage: ExtendedKeyUsagePurpose,
    ) -> (String, String) {
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
        (certificate.pem(), key.serialize_pem())
    }

    /// Writes a CA and a server certificate and key, and returns their paths.
    fn server_files(&self) -> (PathBuf, PathBuf, PathBuf) {
        let authority = Self::authority("krabka test ca");
        let (certificate, key) = Self::leaf(
            &authority,
            "server",
            &["localhost", "127.0.0.1"],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        (
            self.write("ca.pem", authority.pem()),
            self.write("server.pem", certificate),
            self.write("server-key.pem", key),
        )
    }
}

/// Whether an error is the variant a case expects.
type Check = fn(&ServerSecurityError) -> bool;

fn display(path: &Path) -> String {
    path.display().to_string()
}

#[test]
fn a_server_config_builds_although_two_crypto_providers_are_compiled_in() {
    // `tests/crypto_provider_absent.rs` proves that rustls's own `builder()`
    // panics in this build. It runs in its own process, because another unit
    // test here installs a provider for the whole process.
    let pki = Pki::new();
    let (ca, certificate, key) = pki.server_files();
    let ca = display(&ca);
    let certificate = display(&certificate);
    let key = display(&key);
    let security = args(&[
        "--server-tls-cert-path",
        &certificate,
        "--server-tls-key-path",
        &key,
        "--server-tls-client-ca-path",
        &ca,
        "--server-tls-client-auth",
        "RequireAndVerifyClientCert",
    ])
    .load()
    .expect("the TLS flags load");

    assert!(security.tls_enabled());
    let tls = security.tls().expect("TLS is configured");
    assert!(tls.verifies_client_certificates);
    assert!(tls.config.alpn_protocols == vec![b"h2".to_vec(), b"http/1.1".to_vec()]);
}

#[test]
fn no_flags_load_the_upstream_default_posture() {
    let security = args(&[]).load().expect("no flags load");

    assert!(!security.tls_enabled());
    assert!(!security.authentication_enabled());
    assert!(!security.internal_client().is_configured());
    assert!(!ServerSecurity::default().tls_enabled());
    assert!(!ServerSecurity::default().authentication_enabled());
    assert!(security.authenticator().is_none());
}

#[test]
fn flag_combinations_that_cannot_work_are_rejected() {
    let pki = Pki::new();
    let (ca, certificate, key) = pki.server_files();
    let (ca, certificate, key) = (display(&ca), display(&certificate), display(&key));

    let cases: Vec<(&str, Vec<&str>, Check)> = vec![
        (
            "certificate without key",
            vec!["--server-tls-cert-path", &certificate],
            |error| matches!(error, ServerSecurityError::CertificateWithoutKey),
        ),
        (
            "key without certificate",
            vec!["--server-tls-key-path", &key],
            |error| matches!(error, ServerSecurityError::KeyWithoutCertificate),
        ),
        (
            "client CA without certificate",
            vec!["--server-tls-client-ca-path", &ca],
            |error| matches!(error, ServerSecurityError::ClientCaWithoutCertificate),
        ),
        (
            "client auth without certificate",
            vec!["--server-tls-client-auth", "RequestClientCert"],
            |error| {
                matches!(
                    error,
                    ServerSecurityError::ClientAuthWithoutCertificate { .. }
                )
            },
        ),
        (
            "RequireAndVerifyClientCert without client CA",
            vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &key,
                "--server-tls-client-auth",
                "RequireAndVerifyClientCert",
            ],
            |error| {
                matches!(
                    error,
                    ServerSecurityError::ClientAuthWithoutClientCa {
                        client_auth: super::ClientAuth::RequireAndVerifyClientCert
                    }
                )
            },
        ),
        (
            "RequestClientCert without client CA",
            vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &key,
                "--server-tls-client-auth",
                "RequestClientCert",
            ],
            |error| {
                matches!(
                    error,
                    ServerSecurityError::ClientAuthWithoutClientCa {
                        client_auth: super::ClientAuth::RequestClientCert
                    }
                )
            },
        ),
        (
            "client CA with NoClientCert",
            vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &key,
                "--server-tls-client-ca-path",
                &ca,
            ],
            |error| matches!(error, ServerSecurityError::ClientCaWithoutClientAuth),
        ),
        (
            "internal client certificate without key",
            vec!["--internal-client-tls-cert-path", &certificate],
            |error| {
                matches!(
                    error,
                    ServerSecurityError::InternalClientCertificateWithoutKey
                )
            },
        ),
        (
            "internal client key without certificate",
            vec!["--internal-client-tls-key-path", &key],
            |error| {
                matches!(
                    error,
                    ServerSecurityError::InternalClientKeyWithoutCertificate
                )
            },
        ),
    ];

    for (name, flags, check) in cases {
        let error = args(&flags).load().expect_err(name);
        assert!(check(&error), "{name}: {error}");
    }
}

#[test]
fn a_file_that_cannot_be_read_or_parsed_is_rejected_with_its_name() {
    let pki = Pki::new();
    let (ca, certificate, key) = pki.server_files();
    let (ca, certificate, key) = (display(&ca), display(&certificate), display(&key));
    let missing = display(&pki.dir.path().join("missing.pem"));
    let not_pem = display(&pki.write("not-pem.pem", "this is not PEM\n"));
    let broken_pem = display(&pki.write(
        "broken.pem",
        "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n",
    ));
    let other_key = display(&pki.write(
        "other-key.pem",
        KeyPair::generate().expect("a key").serialize_pem(),
    ));
    let empty_token = display(&pki.write("empty-token", "\n"));
    let bad_credentials = display(&pki.write("credentials.yaml", "principals: []\n"));

    let cases: Vec<(&str, Vec<&str>, &str, Check)> = vec![
        (
            "unreadable certificate",
            vec![
                "--server-tls-cert-path",
                &missing,
                "--server-tls-key-path",
                &key,
            ],
            &missing,
            |error| matches!(error, ServerSecurityError::ReadFile { .. }),
        ),
        (
            "certificate file with no PEM",
            vec![
                "--server-tls-cert-path",
                &not_pem,
                "--server-tls-key-path",
                &key,
            ],
            &not_pem,
            |error| matches!(error, ServerSecurityError::NoCertificate { .. }),
        ),
        (
            "certificate file with broken PEM",
            vec![
                "--server-tls-cert-path",
                &broken_pem,
                "--server-tls-key-path",
                &key,
            ],
            &broken_pem,
            |error| matches!(error, ServerSecurityError::InvalidPem { .. }),
        ),
        (
            "key file with no key",
            vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &certificate,
            ],
            &certificate,
            |error| matches!(error, ServerSecurityError::NoPrivateKey { .. }),
        ),
        (
            "key that does not match the certificate",
            vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &other_key,
            ],
            &other_key,
            |error| matches!(error, ServerSecurityError::InvalidServerCertificate { .. }),
        ),
        (
            "unreadable client CA",
            vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &key,
                "--server-tls-client-ca-path",
                &missing,
                "--server-tls-client-auth",
                "RequireAndVerifyClientCert",
            ],
            &missing,
            |error| matches!(error, ServerSecurityError::ReadFile { .. }),
        ),
        (
            "invalid credentials file",
            vec!["--auth-credentials-config", &bad_credentials],
            &bad_credentials,
            |error| matches!(error, ServerSecurityError::Credentials { .. }),
        ),
        (
            "empty internal client token",
            vec!["--internal-client-token-path", &empty_token],
            &empty_token,
            |error| {
                matches!(
                    error,
                    ServerSecurityError::InvalidInternalClientToken { .. }
                )
            },
        ),
        (
            "internal client CA with no certificate",
            vec!["--internal-client-tls-ca-path", &not_pem],
            &not_pem,
            |error| matches!(error, ServerSecurityError::NoCertificate { .. }),
        ),
        (
            "internal client identity from a certificate and the CA as key",
            vec![
                "--internal-client-tls-cert-path",
                &certificate,
                "--internal-client-tls-key-path",
                &ca,
            ],
            &ca,
            |error| {
                matches!(
                    error,
                    ServerSecurityError::InvalidInternalClientIdentity { .. }
                )
            },
        ),
    ];

    for (name, flags, named_file, check) in cases {
        let error = args(&flags).load().expect_err(name);
        assert!(check(&error), "{name}: {error}");
        assert!(error.to_string().contains(named_file), "{name}: {error}");
    }
}

#[test]
fn every_credentials_file_rule_rejects_a_file_that_breaks_it() {
    let digest = sha256_hex("a-token-of-at-least-thirty-two-random-bytes");
    let cases = [
        (
            "unknown key",
            "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: [t]\n    tennants: [t]\n"
                .to_owned(),
            "principals[0]: unknown field `tennants`",
        ),
        (
            "no principals",
            "principals: []\n".to_owned(),
            "the file lists no principals",
        ),
        (
            "empty name",
            "principals:\n  - name: ''\n    token_sha256: [DIGEST]\n    tenants: [t]\n".to_owned(),
            "principal 0 has an empty name",
        ),
        (
            "duplicate name",
            "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: [t]\n  - name: a\n    client_certificates: [a.internal]\n    tenants: [t]\n"
                .to_owned(),
            "principal \"a\" appears more than once",
        ),
        (
            "no credential",
            "principals:\n  - name: a\n    tenants: [t]\n".to_owned(),
            "principal \"a\" has no token_sha256 entry and no client_certificates entry",
        ),
        (
            "invalid tenant",
            "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: ['a/b']\n".to_owned(),
            "principal \"a\": tenant \"a/b\" is not a valid tenant id: ",
        ),
        (
            "wildcard with another tenant",
            "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: ['*', t]\n".to_owned(),
            "principal \"a\": `*` must be the only entry in tenants",
        ),
        (
            "uppercase digest",
            "principals:\n  - name: a\n    token_sha256: [UPPER]\n    tenants: [t]\n".to_owned(),
            "principal \"a\": token_sha256 entry 0 is not exactly 64 lowercase hexadecimal characters",
        ),
        (
            "short digest",
            "principals:\n  - name: a\n    token_sha256: [DIGEST, abc123]\n    tenants: [t]\n".to_owned(),
            "principal \"a\": token_sha256 entry 1 is not exactly 64 lowercase hexadecimal characters",
        ),
        (
            "digest shared by two principals",
            "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: [t]\n  - name: b\n    token_sha256: [DIGEST]\n    tenants: [t]\n"
                .to_owned(),
            "a token_sha256 digest belongs to both \"a\" and \"b\"",
        ),
        (
            "empty certificate identity",
            "principals:\n  - name: a\n    client_certificates: ['']\n    tenants: [t]\n".to_owned(),
            "principal \"a\": client_certificates holds an empty identity",
        ),
        (
            "certificate identity shared by two principals",
            "principals:\n  - name: a\n    client_certificates: [x.internal]\n    tenants: [t]\n  - name: b\n    client_certificates: [x.internal]\n    tenants: [t]\n"
                .to_owned(),
            "client certificate identity \"x.internal\" belongs to both \"a\" and \"b\"",
        ),
    ];

    for (name, yaml, expected) in cases {
        let yaml = yaml
            .replace("DIGEST", &digest)
            .replace("UPPER", &digest.to_uppercase());
        let error = Credentials::from_yaml(yaml.as_bytes()).expect_err(name);
        let message = error.to_string();
        assert!(message.starts_with(expected), "{name}: {message}");
        assert!(!message.contains(&digest), "{name}: {message}");
    }
}

#[test]
fn a_wildcard_grant_allows_every_tenant_and_a_list_allows_only_its_tenants() {
    let yaml = format!(
        "principals:\n  - name: ops\n    token_sha256: [{}]\n    tenants: ['*']\n    admin: true\n  - name: grafana\n    token_sha256: [{}]\n    tenants: [tenant-a, tenant-b]\n",
        sha256_hex("ops-token"),
        sha256_hex("grafana-token"),
    );
    let credentials = Credentials::from_yaml(yaml.as_bytes()).expect("a valid file");

    let ops = credentials
        .principal_for_token(b"ops-token")
        .expect("the ops token matches");
    let grafana = credentials
        .principal_for_token(b"grafana-token")
        .expect("the grafana token matches");

    assert!(ops.tenants == TenantGrant::All);
    assert!(ops.admin);
    assert!(
        grafana.tenants
            == TenantGrant::Only(Arc::new(BTreeSet::from([
                tenant("tenant-a"),
                tenant("tenant-b")
            ])))
    );
    assert!(!grafana.admin);
    assert!(ops.tenants.allows(&tenant("anything")));
    assert!(grafana.tenants.allows(&tenant("tenant-b")));
    assert!(!grafana.tenants.allows(&tenant("tenant-c")));
}

#[test]
fn a_token_matches_only_its_own_principal() {
    let yaml = format!(
        "principals:\n  - name: a\n    token_sha256: [{}, {}]\n    tenants: [t]\n  - name: b\n    token_sha256: [{}]\n    tenants: [t]\n",
        sha256_hex("a-first"),
        sha256_hex("a-second"),
        sha256_hex("b-only"),
    );
    let credentials = Credentials::from_yaml(yaml.as_bytes()).expect("a valid file");
    let name = |token: &[u8]| {
        credentials
            .principal_for_token(token)
            .map(|principal| principal.name.to_string())
    };
    let basic = |username: &str, token: &[u8]| {
        credentials
            .principal_for_basic(username, token)
            .map(|principal| principal.name.to_string())
    };

    assert!(name(b"a-first") == Some("a".to_owned()));
    assert!(name(b"a-second") == Some("a".to_owned()));
    assert!(name(b"b-only") == Some("b".to_owned()));
    assert!(name(b"b-onl") == None);
    assert!(name(b"") == None);
    assert!(basic("b", b"b-only") == Some("b".to_owned()));
    assert!(basic("a", b"b-only") == None);
}

#[test]
fn token_digests_parse_only_as_64_lowercase_hex_characters() {
    let good = sha256_hex("token");
    let cases = [
        (good.clone(), true),
        (good.to_uppercase(), false),
        (good[..63].to_owned(), false),
        (format!("{good}0"), false),
        (format!("{}g", &good[..63]), false),
        (String::new(), false),
    ];

    for (hex, parses) in cases {
        assert!(TokenDigest::parse(&hex).is_some() == parses, "{hex}");
    }
    let parsed = TokenDigest::parse(&good).expect("a valid digest");
    assert!(bool::from(parsed.ct_eq(&TokenDigest::of_token(b"token"))));
    assert!(format!("{parsed:?}") == "TokenDigest(..)");
}

#[test]
fn a_client_identity_holds_the_common_name_and_the_names_of_the_certificate() {
    let authority = Pki::authority("krabka test ca");
    let mut params =
        CertificateParams::new(vec!["grafana.internal".to_owned(), "10.0.0.1".to_owned()])
            .expect("valid parameters");
    params.subject_alt_names.push(rcgen::SanType::URI(
        "spiffe://krabka/grafana".try_into().expect("a URI"),
    ));
    params
        .distinguished_name
        .push(DnType::CommonName, "grafana");
    let key = KeyPair::generate().expect("a key");
    let certificate = params.signed_by(&key, &authority).expect("a signed leaf");

    let identity = ClientIdentity::from_der(certificate.der()).expect("the certificate parses");

    assert!(
        identity
            == ClientIdentity {
                common_name: Some("grafana".to_owned()),
                dns_names: vec!["grafana.internal".to_owned()],
                uris: vec!["spiffe://krabka/grafana".to_owned()],
            }
    );
    assert!(
        identity.names().collect::<Vec<_>>()
            == vec!["grafana", "grafana.internal", "spiffe://krabka/grafana"]
    );
    assert!(ClientIdentity::from_der(b"not a certificate").is_err());
}

fn authenticator(events: &Arc<RecordedEvents>) -> Authenticator {
    let yaml = format!(
        "principals:\n  - name: grafana\n    token_sha256: [{}]\n    client_certificates: [grafana.internal]\n    tenants: [tenant-a]\n  - name: ops\n    client_certificates: [ops]\n    tenants: ['*']\n    admin: true\n",
        sha256_hex("grafana-token"),
    );
    Authenticator {
        credentials: Arc::new(Credentials::from_yaml(yaml.as_bytes()).expect("a valid file")),
        events: SecurityEventSink::new(events.clone()),
        unauthenticated_routes: UnauthenticatedRoutes::default(),
    }
}

fn peer_with(identity: Option<ClientIdentity>) -> PeerAddr {
    PeerAddr {
        socket: "127.0.0.1:40000".parse().expect("a socket address"),
        client_identity: identity.map(Arc::new),
    }
}

fn identity(common_name: &str, dns_names: &[&str]) -> ClientIdentity {
    ClientIdentity {
        common_name: Some(common_name.to_owned()),
        dns_names: dns_names.iter().map(|name| (*name).to_owned()).collect(),
        uris: Vec::new(),
    }
}

/// A request's `Authorization` header and client certificate, the principal
/// it should authenticate as, and the one event it should report.
type AuthCase = (
    &'static str,
    Option<String>,
    Option<ClientIdentity>,
    Option<Principal>,
    String,
);

#[test]
fn each_credential_kind_authenticates_its_principal_and_reports_the_outcome() {
    let events = Arc::new(RecordedEvents::default());
    let authenticator = authenticator(&events);
    let sink = authenticator.events.clone();
    let grafana = |method| Principal::Authenticated {
        name: Arc::from("grafana"),
        method,
        tenants: TenantGrant::Only(Arc::new(BTreeSet::from([tenant("tenant-a")]))),
        admin: false,
        events: sink.clone(),
    };
    let basic = |credential: &str| format!("Basic {}", STANDARD.encode(credential));

    let cases: Vec<AuthCase> = vec![
        (
            "bearer",
            Some("Bearer grafana-token".to_owned()),
            None,
            Some(grafana(AuthMethod::Bearer)),
            "succeeded Some(127.0.0.1:40000) grafana Bearer".to_owned(),
        ),
        (
            "bearer with a lowercase scheme",
            Some("bearer grafana-token".to_owned()),
            None,
            Some(grafana(AuthMethod::Bearer)),
            "succeeded Some(127.0.0.1:40000) grafana Bearer".to_owned(),
        ),
        (
            "basic",
            Some(basic("grafana:grafana-token")),
            None,
            Some(grafana(AuthMethod::Basic)),
            "succeeded Some(127.0.0.1:40000) grafana Basic".to_owned(),
        ),
        (
            "client certificate by DNS name",
            None,
            Some(identity("someone", &["grafana.internal"])),
            Some(grafana(AuthMethod::ClientCertificate)),
            "succeeded Some(127.0.0.1:40000) grafana ClientCertificate".to_owned(),
        ),
        (
            "a header wins over the certificate",
            Some("Bearer wrong".to_owned()),
            Some(identity("ops", &[])),
            None,
            "failed Some(127.0.0.1:40000) Some(Bearer) UnknownCredential".to_owned(),
        ),
        (
            "no credential",
            None,
            None,
            None,
            "failed Some(127.0.0.1:40000) None MissingCredential".to_owned(),
        ),
        (
            "wrong bearer token",
            Some("Bearer grafana-tokem".to_owned()),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Bearer) UnknownCredential".to_owned(),
        ),
        (
            "empty bearer token",
            Some("Bearer".to_owned()),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Bearer) MalformedCredential".to_owned(),
        ),
        (
            "wrong basic username",
            Some(basic("ops:grafana-token")),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Basic) UnknownCredential".to_owned(),
        ),
        (
            "basic without a colon",
            Some(basic("grafana-token")),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Basic) MalformedCredential".to_owned(),
        ),
        (
            "basic that is not base64",
            Some("Basic !!!".to_owned()),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Basic) MalformedCredential".to_owned(),
        ),
        (
            "unsupported scheme",
            Some("Digest grafana-token".to_owned()),
            None,
            None,
            "failed Some(127.0.0.1:40000) None UnsupportedScheme".to_owned(),
        ),
        (
            "unknown client certificate",
            None,
            Some(identity("stranger", &["stranger.internal"])),
            None,
            "failed Some(127.0.0.1:40000) Some(ClientCertificate) UnknownCredential".to_owned(),
        ),
        (
            "client certificate that names two principals",
            None,
            Some(identity("ops", &["grafana.internal"])),
            None,
            "failed Some(127.0.0.1:40000) Some(ClientCertificate) AmbiguousClientCertificate"
                .to_owned(),
        ),
    ];

    for (name, authorization, client_identity, expected, event) in cases {
        let mut headers = HeaderMap::new();
        if let Some(authorization) = &authorization {
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(authorization).expect("a valid header"),
            );
        }
        let peer = peer_with(client_identity);

        let principal = authenticator.authenticate(&headers, Some(&peer));

        assert!(principal == expected, "{name}");
        let recorded = events.take();
        assert!(recorded == vec![event], "{name}");
        assert!(
            recorded
                .iter()
                .all(|event| !event.contains("grafana-token") && !event.contains("tokem")),
            "{name}"
        );
    }
}

#[test]
fn two_authorization_headers_are_malformed() {
    let events = Arc::new(RecordedEvents::default());
    let authenticator = authenticator(&events);
    let mut headers = HeaderMap::new();
    headers.append(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer grafana-token"),
    );
    headers.append(
        header::AUTHORIZATION,
        HeaderValue::from_static("Bearer grafana-token"),
    );

    assert!(authenticator.authenticate(&headers, None) == None);
    assert!(events.take() == vec!["failed None None MalformedCredential".to_owned()]);
}

#[test]
fn tenant_and_admin_authorization_over_both_principal_kinds() {
    let events = Arc::new(RecordedEvents::default());
    let sink = SecurityEventSink::new(events.clone());
    let grafana = Principal::Authenticated {
        name: Arc::from("grafana"),
        method: AuthMethod::Bearer,
        tenants: TenantGrant::Only(Arc::new(BTreeSet::from([tenant("tenant-a")]))),
        admin: false,
        events: sink.clone(),
    };
    let ops = Principal::Authenticated {
        name: Arc::from("ops"),
        method: AuthMethod::ClientCertificate,
        tenants: TenantGrant::All,
        admin: true,
        events: sink,
    };

    let tenant_cases = [
        (
            Principal::Unauthenticated,
            "tenant-a",
            Ok(()),
            "User:tenant-a",
            None,
        ),
        (
            Principal::Unauthenticated,
            "tenant-z",
            Ok(()),
            "User:tenant-z",
            None,
        ),
        (grafana.clone(), "tenant-a", Ok(()), "User:grafana", None),
        (
            grafana.clone(),
            "tenant-b",
            Err(TenantDenied {
                principal: Arc::from("grafana"),
                tenant: tenant("tenant-b"),
            }),
            "User:grafana",
            Some("tenant denied grafana tenant-b"),
        ),
        (ops.clone(), "tenant-z", Ok(()), "User:ops", None),
    ];
    for (principal, id, expected, acl, event) in tenant_cases {
        let id = tenant(id);
        assert!(authorize_tenant(&principal, &id) == expected);
        assert!(principal.acl_principal(&id) == acl);
        assert!(events.take() == event.map(str::to_owned).into_iter().collect::<Vec<_>>());
    }

    let admin_cases = [
        (Principal::Unauthenticated, Ok(()), None),
        (
            grafana,
            Err(AdminDenied {
                principal: Arc::from("grafana"),
            }),
            Some("admin denied grafana"),
        ),
        (ops, Ok(()), None),
    ];
    for (principal, expected, event) in admin_cases {
        assert!(authorize_admin(&principal) == expected);
        assert!(events.take() == event.map(str::to_owned).into_iter().collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn a_denial_answers_403_naming_the_principal_but_not_its_grant() {
    let tenant_denied = TenantDenied {
        principal: Arc::from("grafana"),
        tenant: tenant("tenant-b"),
    }
    .into_response();
    let admin_denied = AdminDenied {
        principal: Arc::from("grafana"),
    }
    .into_response();

    let tenant_status = tenant_denied.status();
    let tenant_body = axum::body::to_bytes(tenant_denied.into_body(), usize::MAX)
        .await
        .expect("a body");
    let admin_status = admin_denied.status();
    let admin_body = axum::body::to_bytes(admin_denied.into_body(), usize::MAX)
        .await
        .expect("a body");

    assert!(tenant_status == StatusCode::FORBIDDEN);
    assert!(
        std::str::from_utf8(&tenant_body)
            == Ok("principal \"grafana\" is not allowed to access tenant \"tenant-b\"\n")
    );
    assert!(admin_status == StatusCode::FORBIDDEN);
    assert!(
        std::str::from_utf8(&admin_body)
            == Ok("principal \"grafana\" is not allowed to call admin operations\n")
    );
}

#[test]
fn unauthenticated_routes_match_on_method_and_exact_path() {
    let default = UnauthenticatedRoutes::default();
    let extended = UnauthenticatedRoutes::default().with(Method::GET, "/-/healthy");
    let cases = [
        (&default, Method::GET, "/ready", true),
        (&default, Method::GET, "/metrics", true),
        (&default, Method::POST, "/ready", false),
        (&default, Method::HEAD, "/metrics", false),
        (&default, Method::GET, "/ready/", false),
        (&default, Method::GET, "/-/healthy", false),
        (&extended, Method::GET, "/-/healthy", true),
        (&extended, Method::GET, "/ready", true),
    ];

    for (routes, method, path, expected) in cases {
        assert!(
            routes.contains(&method, path) == expected,
            "{method} {path}"
        );
    }
    assert!(!UnauthenticatedRoutes::none().contains(&Method::GET, "/ready"));
}

#[test]
fn security_events_and_routes_can_be_replaced_on_a_loaded_posture() {
    let pki = Pki::new();
    let credentials = pki.write(
        "credentials.yaml",
        format!(
            "principals:\n  - name: a\n    token_sha256: [{}]\n    tenants: [t]\n",
            sha256_hex("token")
        ),
    );
    let events: Arc<dyn SecurityEvents> = Arc::new(RecordedEvents::default());
    let routes = UnauthenticatedRoutes::none();

    let security = args(&["--auth-credentials-config", &display(&credentials)])
        .load()
        .expect("the credentials load")
        .with_security_events(events.clone())
        .with_unauthenticated_routes(routes.clone());
    let authenticator = security.authenticator().expect("authentication is on");

    assert!(security.authentication_enabled());
    assert!(authenticator.events == SecurityEventSink::new(events));
    assert!(authenticator.unauthenticated_routes == routes);
}
