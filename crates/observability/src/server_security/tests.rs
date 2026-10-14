use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use assert2::assert;
use axum::{
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use clap::Parser;
use krabka_blockstore::TenantId;
use rcgen::{CertificateParams, DnType, KeyPair};
use tempfile::TempDir;

#[path = "../../tests/support/recorded_security_events.rs"]
mod recorded_security_events;
#[macro_use]
#[path = "../../tests/support/recorded_security_events_impl.rs"]
mod recorded_security_events_impl;
#[path = "../../tests/support/server_security_pki.rs"]
mod server_security_pki;
#[path = "../../tests/support/token_sha256.rs"]
mod token_sha256;

use recorded_security_events::{RecordedEvents, tenant};
use server_security_pki::{Leaf, Pem, authority};
use token_sha256::sha256_hex;

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

struct TokenDigestCase {
    value: String,
    parses: bool,
}

struct TenantAuthorizationCase {
    principal: Principal,
    tenant: &'static str,
    expected: Result<(), TenantDenied>,
    acl: &'static str,
    event: Option<&'static str>,
}

struct AdminAuthorizationCase {
    principal: Principal,
    expected: Result<(), AdminDenied>,
    event: Option<&'static str>,
}

struct UnauthenticatedRouteCase<'a> {
    routes: &'a UnauthenticatedRoutes,
    method: Method,
    path: &'static str,
    expected: bool,
}

impl_recorded_security_events!(|source: Option<SocketAddr>| source);

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
        server_security_pki::write_file(self.dir.path(), name, contents)
    }

    /// Writes a CA and a server certificate and key, and returns their paths.
    fn server_files(&self) -> ServerFiles {
        let authority = authority("krabka test ca");
        let Pem { certificate, key } = Leaf::LOCAL_SERVER.signed_by(&authority);
        ServerFiles {
            ca: self.write("ca.pem", authority.pem()),
            certificate: self.write("server.pem", certificate),
            key: self.write("server-key.pem", key),
        }
    }
}

struct ServerFiles {
    ca: PathBuf,
    certificate: PathBuf,
    key: PathBuf,
}

/// Whether an error is the variant a case expects.
type Check = fn(&ServerSecurityError) -> bool;

struct FlagCombinationCase<'a> {
    name: &'a str,
    flags: Vec<&'a str>,
    check: Check,
}

struct FileRejectionCase<'a> {
    name: &'a str,
    flags: Vec<&'a str>,
    named_file: &'a str,
    check: Check,
}

fn display(path: &Path) -> String {
    path.display().to_string()
}

#[test]
fn a_server_config_builds_although_two_crypto_providers_are_compiled_in() {
    // `tests/crypto_provider_absent.rs` proves that rustls's own `builder()`
    // panics in this build. It runs in its own process, because another unit
    // test here installs a provider for the whole process.
    let pki = Pki::new();
    let files = pki.server_files();
    let ca = display(&files.ca);
    let certificate = display(&files.certificate);
    let key = display(&files.key);
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
    let files = pki.server_files();
    let ca = display(&files.ca);
    let certificate = display(&files.certificate);
    let key = display(&files.key);

    let cases: Vec<FlagCombinationCase<'_>> = vec![
        FlagCombinationCase {
            name: "certificate without key",
            flags: vec!["--server-tls-cert-path", &certificate],
            check: |error| matches!(error, ServerSecurityError::CertificateWithoutKey),
        },
        FlagCombinationCase {
            name: "key without certificate",
            flags: vec!["--server-tls-key-path", &key],
            check: |error| matches!(error, ServerSecurityError::KeyWithoutCertificate),
        },
        FlagCombinationCase {
            name: "client CA without certificate",
            flags: vec!["--server-tls-client-ca-path", &ca],
            check: |error| matches!(error, ServerSecurityError::ClientCaWithoutCertificate),
        },
        FlagCombinationCase {
            name: "client auth without certificate",
            flags: vec!["--server-tls-client-auth", "RequestClientCert"],
            check: |error| {
                matches!(
                    error,
                    ServerSecurityError::ClientAuthWithoutCertificate { .. }
                )
            },
        },
        FlagCombinationCase {
            name: "RequireAndVerifyClientCert without client CA",
            flags: vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &key,
                "--server-tls-client-auth",
                "RequireAndVerifyClientCert",
            ],
            check: |error| {
                matches!(
                    error,
                    ServerSecurityError::ClientAuthWithoutClientCa {
                        client_auth: super::ClientAuth::RequireAndVerifyClientCert
                    }
                )
            },
        },
        FlagCombinationCase {
            name: "RequestClientCert without client CA",
            flags: vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &key,
                "--server-tls-client-auth",
                "RequestClientCert",
            ],
            check: |error| {
                matches!(
                    error,
                    ServerSecurityError::ClientAuthWithoutClientCa {
                        client_auth: super::ClientAuth::RequestClientCert
                    }
                )
            },
        },
        FlagCombinationCase {
            name: "client CA with NoClientCert",
            flags: vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &key,
                "--server-tls-client-ca-path",
                &ca,
            ],
            check: |error| matches!(error, ServerSecurityError::ClientCaWithoutClientAuth),
        },
        FlagCombinationCase {
            name: "internal client certificate without key",
            flags: vec!["--internal-client-tls-cert-path", &certificate],
            check: |error| {
                matches!(
                    error,
                    ServerSecurityError::InternalClientCertificateWithoutKey
                )
            },
        },
        FlagCombinationCase {
            name: "internal client key without certificate",
            flags: vec!["--internal-client-tls-key-path", &key],
            check: |error| {
                matches!(
                    error,
                    ServerSecurityError::InternalClientKeyWithoutCertificate
                )
            },
        },
    ];

    for case in cases {
        let error = args(&case.flags).load().expect_err(case.name);
        assert!((case.check)(&error), "{}: {error}", case.name);
    }
}

#[test]
fn a_file_that_cannot_be_read_or_parsed_is_rejected_with_its_name() {
    let pki = Pki::new();
    let files = pki.server_files();
    let ca = display(&files.ca);
    let certificate = display(&files.certificate);
    let key = display(&files.key);
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

    let cases: Vec<FileRejectionCase<'_>> = vec![
        FileRejectionCase {
            name: "unreadable certificate",
            flags: vec![
                "--server-tls-cert-path",
                &missing,
                "--server-tls-key-path",
                &key,
            ],
            named_file: &missing,
            check: |error| matches!(error, ServerSecurityError::ReadFile { .. }),
        },
        FileRejectionCase {
            name: "certificate file with no PEM",
            flags: vec![
                "--server-tls-cert-path",
                &not_pem,
                "--server-tls-key-path",
                &key,
            ],
            named_file: &not_pem,
            check: |error| matches!(error, ServerSecurityError::NoCertificate { .. }),
        },
        FileRejectionCase {
            name: "certificate file with broken PEM",
            flags: vec![
                "--server-tls-cert-path",
                &broken_pem,
                "--server-tls-key-path",
                &key,
            ],
            named_file: &broken_pem,
            check: |error| matches!(error, ServerSecurityError::InvalidPem { .. }),
        },
        FileRejectionCase {
            name: "key file with no key",
            flags: vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &certificate,
            ],
            named_file: &certificate,
            check: |error| matches!(error, ServerSecurityError::NoPrivateKey { .. }),
        },
        FileRejectionCase {
            name: "key that does not match the certificate",
            flags: vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &other_key,
            ],
            named_file: &other_key,
            check: |error| matches!(error, ServerSecurityError::InvalidServerCertificate { .. }),
        },
        FileRejectionCase {
            name: "unreadable client CA",
            flags: vec![
                "--server-tls-cert-path",
                &certificate,
                "--server-tls-key-path",
                &key,
                "--server-tls-client-ca-path",
                &missing,
                "--server-tls-client-auth",
                "RequireAndVerifyClientCert",
            ],
            named_file: &missing,
            check: |error| matches!(error, ServerSecurityError::ReadFile { .. }),
        },
        FileRejectionCase {
            name: "invalid credentials file",
            flags: vec!["--auth-credentials-config", &bad_credentials],
            named_file: &bad_credentials,
            check: |error| matches!(error, ServerSecurityError::Credentials { .. }),
        },
        FileRejectionCase {
            name: "empty internal client token",
            flags: vec!["--internal-client-token-path", &empty_token],
            named_file: &empty_token,
            check: |error| {
                matches!(
                    error,
                    ServerSecurityError::InvalidInternalClientToken { .. }
                )
            },
        },
        FileRejectionCase {
            name: "internal client CA with no certificate",
            flags: vec!["--internal-client-tls-ca-path", &not_pem],
            named_file: &not_pem,
            check: |error| matches!(error, ServerSecurityError::NoCertificate { .. }),
        },
        FileRejectionCase {
            name: "internal client identity from a certificate and the CA as key",
            flags: vec![
                "--internal-client-tls-cert-path",
                &certificate,
                "--internal-client-tls-key-path",
                &ca,
            ],
            named_file: &ca,
            check: |error| {
                matches!(
                    error,
                    ServerSecurityError::InvalidInternalClientIdentity { .. }
                )
            },
        },
    ];

    for case in cases {
        let error = args(&case.flags).load().expect_err(case.name);
        assert!((case.check)(&error), "{}: {error}", case.name);
        assert!(
            error.to_string().contains(case.named_file),
            "{}: {error}",
            case.name
        );
    }
}

#[yare::parameterized(
    unknown_key = { "unknown key", "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: [t]\n    tennants: [t]\n", "principals[0]: unknown field `tennants`" },
    no_principals = { "no principals", "principals: []\n", "the file lists no principals" },
    empty_name = { "empty name", "principals:\n  - name: ''\n    token_sha256: [DIGEST]\n    tenants: [t]\n", "principal 0 has an empty name" },
    duplicate_name = { "duplicate name", "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: [t]\n  - name: a\n    client_certificates: [a.internal]\n    tenants: [t]\n", "principal \"a\" appears more than once" },
    no_credential = { "no credential", "principals:\n  - name: a\n    tenants: [t]\n", "principal \"a\" has no token_sha256 entry and no client_certificates entry" },
    invalid_tenant = { "invalid tenant", "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: ['a/b']\n", "principal \"a\": tenant \"a/b\" is not a valid tenant id: " },
    wildcard_with_another_tenant = { "wildcard with another tenant", "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: ['*', t]\n", "principal \"a\": `*` must be the only entry in tenants" },
    uppercase_digest = { "uppercase digest", "principals:\n  - name: a\n    token_sha256: [UPPER]\n    tenants: [t]\n", "principal \"a\": token_sha256 entry 0 is not exactly 64 lowercase hexadecimal characters" },
    short_digest = { "short digest", "principals:\n  - name: a\n    token_sha256: [DIGEST, abc123]\n    tenants: [t]\n", "principal \"a\": token_sha256 entry 1 is not exactly 64 lowercase hexadecimal characters" },
    shared_digest = { "digest shared by two principals", "principals:\n  - name: a\n    token_sha256: [DIGEST]\n    tenants: [t]\n  - name: b\n    token_sha256: [DIGEST]\n    tenants: [t]\n", "a token_sha256 digest belongs to both \"a\" and \"b\"" },
    empty_certificate_identity = { "empty certificate identity", "principals:\n  - name: a\n    client_certificates: ['']\n    tenants: [t]\n", "principal \"a\": client_certificates holds an empty identity" },
    shared_certificate_identity = { "certificate identity shared by two principals", "principals:\n  - name: a\n    client_certificates: [x.internal]\n    tenants: [t]\n  - name: b\n    client_certificates: [x.internal]\n    tenants: [t]\n", "client certificate identity \"x.internal\" belongs to both \"a\" and \"b\"" },
)]
fn every_credentials_file_rule_rejects_a_file_that_breaks_it(
    name: &str,
    yaml: &str,
    expected: &str,
) {
    let digest = sha256_hex("a-token-of-at-least-thirty-two-random-bytes");
    let yaml = yaml
        .replace("DIGEST", &digest)
        .replace("UPPER", &digest.to_uppercase());
    let error = Credentials::from_yaml(yaml.as_bytes()).expect_err(name);
    let message = error.to_string();
    ::core::assert!(message.starts_with(expected), "{name}: {message}");
    ::core::assert!(!message.contains(&digest), "{name}: {message}");
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
        TokenDigestCase {
            value: good.clone(),
            parses: true,
        },
        TokenDigestCase {
            value: good.to_uppercase(),
            parses: false,
        },
        TokenDigestCase {
            value: good[..63].to_owned(),
            parses: false,
        },
        TokenDigestCase {
            value: format!("{good}0"),
            parses: false,
        },
        TokenDigestCase {
            value: format!("{}g", &good[..63]),
            parses: false,
        },
        TokenDigestCase {
            value: String::new(),
            parses: false,
        },
    ];

    for case in cases {
        assert!(
            TokenDigest::parse(&case.value).is_some() == case.parses,
            "{}",
            case.value
        );
    }
    let parsed = TokenDigest::parse(&good).expect("a valid digest");
    assert!(bool::from(parsed.ct_eq(&TokenDigest::of_token(b"token"))));
    assert!(format!("{parsed:?}") == "TokenDigest(..)");
}

#[test]
fn a_client_identity_holds_the_common_name_and_the_names_of_the_certificate() {
    let authority = authority("krabka test ca");
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
struct AuthCase {
    name: &'static str,
    authorization: Option<String>,
    client_identity: Option<ClientIdentity>,
    expected: Option<Principal>,
    event: String,
}

fn auth_case(
    name: &'static str,
    authorization: Option<String>,
    client_identity: Option<ClientIdentity>,
    expected: Option<Principal>,
    event: String,
) -> AuthCase {
    AuthCase {
        name,
        authorization,
        client_identity,
        expected,
        event,
    }
}

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
        auth_case(
            "bearer",
            Some("Bearer grafana-token".to_owned()),
            None,
            Some(grafana(AuthMethod::Bearer)),
            "succeeded Some(127.0.0.1:40000) grafana Bearer".to_owned(),
        ),
        auth_case(
            "bearer with a lowercase scheme",
            Some("bearer grafana-token".to_owned()),
            None,
            Some(grafana(AuthMethod::Bearer)),
            "succeeded Some(127.0.0.1:40000) grafana Bearer".to_owned(),
        ),
        auth_case(
            "basic",
            Some(basic("grafana:grafana-token")),
            None,
            Some(grafana(AuthMethod::Basic)),
            "succeeded Some(127.0.0.1:40000) grafana Basic".to_owned(),
        ),
        auth_case(
            "client certificate by DNS name",
            None,
            Some(identity("someone", &["grafana.internal"])),
            Some(grafana(AuthMethod::ClientCertificate)),
            "succeeded Some(127.0.0.1:40000) grafana ClientCertificate".to_owned(),
        ),
        auth_case(
            "a header wins over the certificate",
            Some("Bearer wrong".to_owned()),
            Some(identity("ops", &[])),
            None,
            "failed Some(127.0.0.1:40000) Some(Bearer) UnknownCredential".to_owned(),
        ),
        auth_case(
            "no credential",
            None,
            None,
            None,
            "failed Some(127.0.0.1:40000) None MissingCredential".to_owned(),
        ),
        auth_case(
            "wrong bearer token",
            Some("Bearer grafana-tokem".to_owned()),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Bearer) UnknownCredential".to_owned(),
        ),
        auth_case(
            "empty bearer token",
            Some("Bearer".to_owned()),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Bearer) MalformedCredential".to_owned(),
        ),
        auth_case(
            "wrong basic username",
            Some(basic("ops:grafana-token")),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Basic) UnknownCredential".to_owned(),
        ),
        auth_case(
            "basic without a colon",
            Some(basic("grafana-token")),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Basic) MalformedCredential".to_owned(),
        ),
        auth_case(
            "basic that is not base64",
            Some("Basic !!!".to_owned()),
            None,
            None,
            "failed Some(127.0.0.1:40000) Some(Basic) MalformedCredential".to_owned(),
        ),
        auth_case(
            "unsupported scheme",
            Some("Digest grafana-token".to_owned()),
            None,
            None,
            "failed Some(127.0.0.1:40000) None UnsupportedScheme".to_owned(),
        ),
        auth_case(
            "unknown client certificate",
            None,
            Some(identity("stranger", &["stranger.internal"])),
            None,
            "failed Some(127.0.0.1:40000) Some(ClientCertificate) UnknownCredential".to_owned(),
        ),
        auth_case(
            "client certificate that names two principals",
            None,
            Some(identity("ops", &["grafana.internal"])),
            None,
            "failed Some(127.0.0.1:40000) Some(ClientCertificate) AmbiguousClientCertificate"
                .to_owned(),
        ),
    ];

    for case in cases {
        let mut headers = HeaderMap::new();
        if let Some(authorization) = &case.authorization {
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(authorization).expect("a valid header"),
            );
        }
        let peer = peer_with(case.client_identity);

        let principal = authenticator.authenticate(&headers, Some(&peer));

        assert!(principal == case.expected, "{}", case.name);
        let recorded = events.take();
        assert!(recorded == vec![case.event], "{}", case.name);
        assert!(
            recorded
                .iter()
                .all(|event| !event.contains("grafana-token") && !event.contains("tokem")),
            "{}",
            case.name
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
        TenantAuthorizationCase {
            principal: Principal::Unauthenticated,
            tenant: "tenant-a",
            expected: Ok(()),
            acl: "User:tenant-a",
            event: None,
        },
        TenantAuthorizationCase {
            principal: Principal::Unauthenticated,
            tenant: "tenant-z",
            expected: Ok(()),
            acl: "User:tenant-z",
            event: None,
        },
        TenantAuthorizationCase {
            principal: grafana.clone(),
            tenant: "tenant-a",
            expected: Ok(()),
            acl: "User:grafana",
            event: None,
        },
        TenantAuthorizationCase {
            principal: grafana.clone(),
            tenant: "tenant-b",
            expected: Err(TenantDenied {
                principal: Arc::from("grafana"),
                tenant: tenant("tenant-b"),
            }),
            acl: "User:grafana",
            event: Some("tenant denied grafana tenant-b"),
        },
        TenantAuthorizationCase {
            principal: ops.clone(),
            tenant: "tenant-z",
            expected: Ok(()),
            acl: "User:ops",
            event: None,
        },
    ];
    for case in tenant_cases {
        let id = tenant(case.tenant);
        assert!(authorize_tenant(&case.principal, &id) == case.expected);
        assert!(case.principal.acl_principal(&id) == case.acl);
        assert!(
            events.take()
                == case
                    .event
                    .map(str::to_owned)
                    .into_iter()
                    .collect::<Vec<_>>()
        );
    }

    let admin_cases = [
        AdminAuthorizationCase {
            principal: Principal::Unauthenticated,
            expected: Ok(()),
            event: None,
        },
        AdminAuthorizationCase {
            principal: grafana,
            expected: Err(AdminDenied {
                principal: Arc::from("grafana"),
            }),
            event: Some("admin denied grafana"),
        },
        AdminAuthorizationCase {
            principal: ops,
            expected: Ok(()),
            event: None,
        },
    ];
    for case in admin_cases {
        assert!(authorize_admin(&case.principal) == case.expected);
        assert!(
            events.take()
                == case
                    .event
                    .map(str::to_owned)
                    .into_iter()
                    .collect::<Vec<_>>()
        );
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
        UnauthenticatedRouteCase {
            routes: &default,
            method: Method::GET,
            path: "/ready",
            expected: true,
        },
        UnauthenticatedRouteCase {
            routes: &default,
            method: Method::GET,
            path: "/metrics",
            expected: true,
        },
        UnauthenticatedRouteCase {
            routes: &default,
            method: Method::POST,
            path: "/ready",
            expected: false,
        },
        UnauthenticatedRouteCase {
            routes: &default,
            method: Method::HEAD,
            path: "/metrics",
            expected: false,
        },
        UnauthenticatedRouteCase {
            routes: &default,
            method: Method::GET,
            path: "/ready/",
            expected: false,
        },
        UnauthenticatedRouteCase {
            routes: &default,
            method: Method::GET,
            path: "/-/healthy",
            expected: false,
        },
        UnauthenticatedRouteCase {
            routes: &extended,
            method: Method::GET,
            path: "/-/healthy",
            expected: true,
        },
        UnauthenticatedRouteCase {
            routes: &extended,
            method: Method::GET,
            path: "/ready",
            expected: true,
        },
    ];

    for case in cases {
        assert!(
            case.routes.contains(&case.method, case.path) == case.expected,
            "{} {}",
            case.method,
            case.path
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
