use std::{
    fs,
    path::{Path, PathBuf},
};

use assert2::assert;
use clap::Parser;
use krabka_client_core::{
    ClientSecurity, ConnectionOptions, OAuthBearerTokenSource, SaslCredentials, TlsConnectorConfig,
    security::{KeyStore, TrustStore},
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use krabka_units::secs;
use tempfile::TempDir;

use super::{
    WalClientSecurityArgs, WalClientSecurityError, WalSaslMechanism, WalSecurityProtocol,
    read_password_file, with_client_security,
};

const PROGRAM: &str = "krabka-wal-client-security-test";

/// A password that no error, `Debug` output or message may contain.
const SECRET: &str = "correct horse battery staple";

const ENVIRONMENT: [&str; 9] = [
    "KRABKA_WAL_SECURITY_PROTOCOL",
    "KRABKA_WAL_TLS_CA_PATH",
    "KRABKA_WAL_TLS_SERVER_NAME",
    "KRABKA_WAL_TLS_CERT_PATH",
    "KRABKA_WAL_TLS_KEY_PATH",
    "KRABKA_WAL_SASL_MECHANISM",
    "KRABKA_WAL_SASL_USERNAME",
    "KRABKA_WAL_SASL_PASSWORD_PATH",
    "KRABKA_WAL_SASL_OAUTHBEARER_TOKEN_PATH",
];

struct FixtureFile<'a> {
    name: &'a str,
    contents: &'a [u8],
}

struct ProtocolNameCase {
    wire_name: &'static str,
    protocol: WalSecurityProtocol,
}

struct MechanismNameCase {
    wire_name: &'static str,
    mechanism: WalSaslMechanism,
}

struct ProtocolRequirementCase {
    protocol: WalSecurityProtocol,
    tls: bool,
    sasl: bool,
}

struct PasswordLineCase {
    contents: &'static str,
    expected: &'static str,
}

struct SecretRendering {
    name: &'static str,
    rendering: String,
}

struct EnvironmentSetting<'a> {
    name: &'a str,
    value: &'a str,
}

#[derive(Debug, Parser)]
struct Cli {
    #[command(flatten)]
    wal: WalClientSecurityArgs,
}

// Every parse runs with the variables unset, under the lock `temp_env` holds,
// so the environment test cannot leak its values into another test.
fn parse(args: &[&str]) -> Result<WalClientSecurityArgs, clap::Error> {
    temp_env::with_vars_unset(ENVIRONMENT, || {
        Cli::try_parse_from(std::iter::once(PROGRAM).chain(args.iter().copied())).map(|cli| cli.wal)
    })
}

fn load(args: &[&str]) -> Result<Option<ClientSecurity>, WalClientSecurityError> {
    parse(args).expect("the flags parse").load()
}

/// Files for the flags to name, each readable unless its name says otherwise.
struct Files {
    dir: TempDir,
}

impl Files {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temporary directory");
        for fixture in [
            FixtureFile {
                name: "ca.pem",
                contents: b"ca".as_slice(),
            },
            FixtureFile {
                name: "cert.pem",
                contents: b"cert".as_slice(),
            },
            FixtureFile {
                name: "key.pem",
                contents: b"key".as_slice(),
            },
            FixtureFile {
                name: "token",
                contents: b"header.payload.signature\n".as_slice(),
            },
            FixtureFile {
                name: "password",
                contents: format!("{SECRET}\n").as_bytes(),
            },
            FixtureFile {
                name: "blank-password",
                contents: b"\r\n\n".as_slice(),
            },
            FixtureFile {
                name: "not-utf8-password",
                contents: [SECRET.as_bytes(), &[0xff]].concat().as_slice(),
            },
        ] {
            fs::write(dir.path().join(fixture.name), fixture.contents).expect("write fixture");
        }
        fs::create_dir(dir.path().join("directory")).expect("create directory");
        Self { dir }
    }

    fn path(&self, name: &str) -> String {
        self.dir
            .path()
            .join(name)
            .to_str()
            .expect("the temporary directory is UTF-8")
            .to_owned()
    }
}

/// Compares two policies field by field. `ClientSecurity` has no
/// `PartialEq`, and its `Debug` output holds the password, so a comparison
/// through `Debug` would print the password on a failure.
fn assert_same_security(actual: &ClientSecurity, expected: &ClientSecurity) {
    let ClientSecurity {
        protocol,
        tls,
        sasl,
        sasl_host,
    } = actual;
    assert!(*protocol == expected.protocol);
    assert!(*sasl_host == expected.sasl_host);
    assert!(tls.as_ref().map(tls_fields) == expected.tls.as_ref().map(tls_fields));
    assert!(
        sasl.as_ref().map(public_credential_fields)
            == expected.sasl.as_ref().map(public_credential_fields)
    );
    let same_password =
        sasl.as_ref().and_then(password) == expected.sasl.as_ref().and_then(password);
    assert!(same_password, "the SASL passwords differ");
}

#[derive(PartialEq, Eq)]
struct TlsFields<'a> {
    trust_store: &'a TrustStore,
    server_name: &'a str,
    key_store: &'a Option<KeyStore>,
}

fn tls_fields(tls: &TlsConnectorConfig) -> TlsFields<'_> {
    let TlsConnectorConfig {
        trust_store,
        server_name,
        key_store,
        ..
    } = tls;
    TlsFields {
        trust_store,
        server_name: server_name.as_str(),
        key_store,
    }
}

#[derive(PartialEq, Eq)]
struct PublicCredentialFields<'a> {
    mechanism: SaslMechanism,
    principals: Vec<&'a str>,
    key_path: Option<&'a Path>,
}

/// Every credential field except the password, which a failure may print.
fn public_credential_fields(credentials: &SaslCredentials) -> PublicCredentialFields<'_> {
    let mechanism = credentials.mechanism();
    match credentials {
        SaslCredentials::Plain { username, .. } => PublicCredentialFields {
            mechanism,
            principals: vec![username.as_str()],
            key_path: None,
        },
        SaslCredentials::Scram {
            username,
            delegation_token,
            ..
        } => {
            assert!(!delegation_token);
            PublicCredentialFields {
                mechanism,
                principals: vec![username.as_str()],
                key_path: None,
            }
        }
        SaslCredentials::Gssapi {
            keytab_path,
            client_principal,
            service_name,
            kdc_url,
        } => PublicCredentialFields {
            mechanism,
            principals: vec![
                client_principal.as_str(),
                service_name.as_str(),
                kdc_url.as_str(),
            ],
            key_path: Some(keytab_path.as_path()),
        },
        SaslCredentials::OAuthBearer {
            token: OAuthBearerTokenSource::File(token_path),
            extensions,
        } => {
            assert!(extensions.is_empty());
            PublicCredentialFields {
                mechanism,
                principals: Vec::new(),
                key_path: Some(token_path.as_path()),
            }
        }
        SaslCredentials::OAuthBearer {
            token: OAuthBearerTokenSource::Provider(_),
            ..
        } => panic!("WAL OAuth tokens must be read from a file"),
    }
}

fn password(credentials: &SaslCredentials) -> Option<&str> {
    match credentials {
        SaslCredentials::Plain { password, .. } | SaslCredentials::Scram { password, .. } => {
            Some(password.as_str())
        }
        SaslCredentials::Gssapi { .. } | SaslCredentials::OAuthBearer { .. } => None,
    }
}

#[test]
fn every_flag_parses_from_the_command_line_and_from_the_environment() {
    let expected = WalClientSecurityArgs {
        wal_security_protocol: WalSecurityProtocol::SaslSsl,
        wal_tls_ca_path: Some(PathBuf::from("/etc/krabka/ca.pem")),
        wal_tls_server_name: Some("broker.krabka.test".to_string()),
        wal_tls_cert_path: Some(PathBuf::from("/etc/krabka/client.pem")),
        wal_tls_key_path: Some(PathBuf::from("/etc/krabka/client.key")),
        wal_sasl_mechanism: Some(WalSaslMechanism::ScramSha512),
        wal_sasl_username: Some("krabka-metrics".to_string()),
        wal_sasl_password_path: Some(PathBuf::from("/run/secrets/wal-password")),
        wal_sasl_oauthbearer_token_path: Some(PathBuf::from("/run/secrets/wal-token")),
    };
    let values = [
        "SASL_SSL",
        "/etc/krabka/ca.pem",
        "broker.krabka.test",
        "/etc/krabka/client.pem",
        "/etc/krabka/client.key",
        "SCRAM-SHA-512",
        "krabka-metrics",
        "/run/secrets/wal-password",
        "/run/secrets/wal-token",
    ];
    let flags = [
        "--wal-security-protocol",
        "--wal-tls-ca-path",
        "--wal-tls-server-name",
        "--wal-tls-cert-path",
        "--wal-tls-key-path",
        "--wal-sasl-mechanism",
        "--wal-sasl-username",
        "--wal-sasl-password-path",
        "--wal-sasl-oauthbearer-token-path",
    ];

    let command_line: Vec<&str> = (0..flags.len())
        .flat_map(|index| [flags[index], values[index]])
        .collect();
    assert!(let Ok(from_flags) = parse(&command_line));
    assert!(from_flags == expected);

    let mut value_index = 0;
    let environment = ENVIRONMENT
        .iter()
        .map(|name| {
            let setting = EnvironmentSetting {
                name: *name,
                value: values[value_index],
            };
            value_index += 1;
            setting
        })
        .collect::<Vec<_>>();
    let from_environment = temp_env::with_vars(
        environment
            .into_iter()
            .map(|setting| (setting.name, Some(setting.value)))
            .collect::<Vec<_>>(),
        || Cli::try_parse_from([PROGRAM]).map(|cli| cli.wal),
    );
    assert!(let Ok(from_environment) = from_environment);
    assert!(from_environment == expected);
}

#[test]
fn the_protocol_and_the_mechanism_parse_from_their_kafka_names() {
    for case in [
        ProtocolNameCase {
            wire_name: "PLAINTEXT",
            protocol: WalSecurityProtocol::Plaintext,
        },
        ProtocolNameCase {
            wire_name: "SSL",
            protocol: WalSecurityProtocol::Ssl,
        },
        ProtocolNameCase {
            wire_name: "SASL_PLAINTEXT",
            protocol: WalSecurityProtocol::SaslPlaintext,
        },
        ProtocolNameCase {
            wire_name: "SASL_SSL",
            protocol: WalSecurityProtocol::SaslSsl,
        },
        ProtocolNameCase {
            wire_name: "sasl_ssl",
            protocol: WalSecurityProtocol::SaslSsl,
        },
    ] {
        assert!(let Ok(args) = parse(&["--wal-security-protocol", case.wire_name]));
        assert!(args.wal_security_protocol == case.protocol);
        assert!(case.protocol.to_string() == case.wire_name.to_uppercase());
    }
    for case in [
        MechanismNameCase {
            wire_name: "PLAIN",
            mechanism: WalSaslMechanism::Plain,
        },
        MechanismNameCase {
            wire_name: "SCRAM-SHA-256",
            mechanism: WalSaslMechanism::ScramSha256,
        },
        MechanismNameCase {
            wire_name: "SCRAM-SHA-512",
            mechanism: WalSaslMechanism::ScramSha512,
        },
        MechanismNameCase {
            wire_name: "OAUTHBEARER",
            mechanism: WalSaslMechanism::OAuthBearer,
        },
        MechanismNameCase {
            wire_name: "GSSAPI",
            mechanism: WalSaslMechanism::Gssapi,
        },
    ] {
        assert!(let Ok(args) = parse(&["--wal-sasl-mechanism", case.wire_name]));
        assert!(args.wal_sasl_mechanism == Some(case.mechanism));
        assert!(case.mechanism.to_string() == case.wire_name);
        assert!(SaslMechanism::from(case.mechanism).wire_name() == case.wire_name);
    }
    for case in [
        ProtocolRequirementCase {
            protocol: WalSecurityProtocol::Plaintext,
            tls: false,
            sasl: false,
        },
        ProtocolRequirementCase {
            protocol: WalSecurityProtocol::Ssl,
            tls: true,
            sasl: false,
        },
        ProtocolRequirementCase {
            protocol: WalSecurityProtocol::SaslPlaintext,
            tls: false,
            sasl: true,
        },
        ProtocolRequirementCase {
            protocol: WalSecurityProtocol::SaslSsl,
            tls: true,
            sasl: true,
        },
    ] {
        assert!(case.protocol.requires_tls() == case.tls);
        assert!(case.protocol.requires_sasl() == case.sasl);
    }
    // Kafka matches `sasl.mechanism` exactly, and an empty value names nothing.
    for invalid in [
        ["--wal-sasl-mechanism", "scram-sha-512"],
        ["--wal-security-protocol", "TLS"],
        ["--wal-tls-server-name", ""],
        ["--wal-sasl-username", ""],
        ["--wal-tls-ca-path", ""],
    ] {
        assert!(parse(&invalid).is_err(), "{invalid:?} parsed");
    }
}

#[test]
fn no_flag_gives_plaintext_and_the_client_default_of_no_policy() {
    assert!(let Ok(args) = parse(&[]));
    assert!(args == WalClientSecurityArgs::default());
    assert!(args.wal_security_protocol == WalSecurityProtocol::Plaintext);
    assert!(ConnectionOptions::default().security.is_none());
    assert!(let Ok(None) = args.load());
    assert!(let Ok(None) = load(&["--wal-security-protocol", "PLAINTEXT"]));
}

#[test]
fn sasl_ssl_with_scram_sha_512_builds_the_whole_policy() {
    let files = Files::new();
    let ca = files.path("ca.pem");
    let password_path = files.path("password");
    assert!(let
        Ok(Some(actual)) = load(&[
            "--wal-security-protocol",
            "SASL_SSL",
            "--wal-tls-ca-path",
            &ca,
            "--wal-tls-server-name",
            "broker.krabka.test",
            "--wal-sasl-mechanism",
            "SCRAM-SHA-512",
            "--wal-sasl-username",
            "krabka-metrics",
            "--wal-sasl-password-path",
            &password_path,
        ])
        .map_err(|error| error.to_string())
    );

    let mut tls = TlsConnectorConfig::default();
    tls.trust_store = TrustStore::PemFile(PathBuf::from(&ca));
    tls.server_name = "broker.krabka.test".to_string();
    let expected = ClientSecurity {
        protocol: ListenerProtocol::SaslSsl,
        tls: Some(tls),
        sasl: Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username: "krabka-metrics".to_string(),
            password: SECRET.to_string(),
            delegation_token: false,
        }),
        sasl_host: None,
    };
    assert_same_security(&actual, &expected);
}

struct SecurityCase<'a> {
    name: &'static str,
    args: Vec<&'a str>,
    expected: ClientSecurity,
}

macro_rules! security_case {
    ($name:expr, $args:expr, $expected:expr $(,)?) => {
        SecurityCase {
            name: $name,
            args: $args,
            expected: $expected,
        }
    };
}

#[test]
fn each_protocol_and_mechanism_builds_the_policy_it_names() {
    let files = Files::new();
    let ca = files.path("ca.pem");
    let cert = files.path("cert.pem");
    let key = files.path("key.pem");
    let password_path = files.path("password");
    let token = files.path("token");
    let mut tls = TlsConnectorConfig::default();
    tls.trust_store = TrustStore::PemFile(PathBuf::from(&ca));
    tls.server_name = "localhost".to_string();
    let cases: [SecurityCase<'_>; 5] = [
        security_case!(
            "SSL",
            vec![
                "--wal-security-protocol",
                "SSL",
                "--wal-tls-ca-path",
                &ca,
                "--wal-tls-server-name",
                "localhost",
            ],
            ClientSecurity {
                protocol: ListenerProtocol::Ssl,
                tls: Some(tls.clone()),
                sasl: None,
                sasl_host: None,
            },
        ),
        security_case!(
            "SSL with a client certificate",
            vec![
                "--wal-security-protocol",
                "SSL",
                "--wal-tls-ca-path",
                &ca,
                "--wal-tls-server-name",
                "localhost",
                "--wal-tls-cert-path",
                &cert,
                "--wal-tls-key-path",
                &key,
            ],
            ClientSecurity {
                protocol: ListenerProtocol::Ssl,
                tls: Some({
                    let mut tls = tls.clone();
                    tls.key_store = Some(KeyStore::PemFiles {
                        certificate_chain: PathBuf::from(&cert),
                        private_key: PathBuf::from(&key),
                        key_password: None,
                    });
                    tls
                }),
                sasl: None,
                sasl_host: None,
            },
        ),
        security_case!(
            "SASL_PLAINTEXT with PLAIN",
            vec![
                "--wal-security-protocol",
                "SASL_PLAINTEXT",
                "--wal-sasl-mechanism",
                "PLAIN",
                "--wal-sasl-username",
                "alice",
                "--wal-sasl-password-path",
                &password_path,
            ],
            ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Plain {
                    username: "alice".to_string(),
                    password: SECRET.to_string(),
                }),
                sasl_host: None,
            },
        ),
        security_case!(
            "SASL_PLAINTEXT with SCRAM-SHA-256",
            vec![
                "--wal-security-protocol",
                "SASL_PLAINTEXT",
                "--wal-sasl-mechanism",
                "SCRAM-SHA-256",
                "--wal-sasl-username",
                "alice",
                "--wal-sasl-password-path",
                &password_path,
            ],
            ClientSecurity {
                protocol: ListenerProtocol::SaslPlaintext,
                tls: None,
                sasl: Some(SaslCredentials::Scram {
                    mechanism: SaslMechanism::ScramSha256,
                    username: "alice".to_string(),
                    password: SECRET.to_string(),
                    delegation_token: false,
                }),
                sasl_host: None,
            },
        ),
        security_case!(
            "SASL_SSL with OAUTHBEARER",
            vec![
                "--wal-security-protocol",
                "SASL_SSL",
                "--wal-tls-ca-path",
                &ca,
                "--wal-tls-server-name",
                "localhost",
                "--wal-sasl-mechanism",
                "OAUTHBEARER",
                "--wal-sasl-oauthbearer-token-path",
                &token,
            ],
            ClientSecurity {
                protocol: ListenerProtocol::SaslSsl,
                tls: Some(tls.clone()),
                sasl: Some(SaslCredentials::OAuthBearer {
                    token: OAuthBearerTokenSource::File(PathBuf::from(&token)),
                    extensions: std::collections::BTreeMap::default(),
                }),
                sasl_host: None,
            },
        ),
    ];
    for case in &cases {
        let result = load(&case.args).map_err(|error| format!("{}: {error}", case.name));
        assert!(let Ok(Some(actual)) = result);
        assert_same_security(&actual, &case.expected);
    }
}

#[test]
fn a_tls_protocol_installs_a_rustls_default_provider() {
    let files = Files::new();
    let ca = files.path("ca.pem");
    assert!(let
        Ok(Some(_)) = load(&[
            "--wal-security-protocol",
            "SSL",
            "--wal-tls-ca-path",
            &ca,
            "--wal-tls-server-name",
            "localhost",
        ])
        .map_err(|error| error.to_string())
    );
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());
}

/// A test of whether an error is the one a case expects.
type Refusal<'a> = Box<dyn Fn(&WalClientSecurityError) -> bool + 'a>;

/// A case name, the flags it passes, and the error it expects.
struct RefusalCase<'a> {
    name: &'static str,
    args: Vec<&'a str>,
    expected: Refusal<'a>,
}

macro_rules! refusal_case {
    ($name:expr, $args:expr, $expected:expr $(,)?) => {
        RefusalCase {
            name: $name,
            args: $args,
            expected: $expected,
        }
    };
}

macro_rules! refused {
    ($pattern:pat $(if $guard:expr)?) => {
        Box::new(move |error: &WalClientSecurityError| {
            matches!(error, $pattern $(if $guard)?)
        }) as Refusal<'_>
    };
}

const SSL: [&str; 2] = ["--wal-security-protocol", "SSL"];
const SASL_SSL: [&str; 2] = ["--wal-security-protocol", "SASL_SSL"];
const SASL_PLAINTEXT: [&str; 2] = ["--wal-security-protocol", "SASL_PLAINTEXT"];
const PLAIN_USER: [&str; 4] = [
    "--wal-sasl-mechanism",
    "PLAIN",
    "--wal-sasl-username",
    "alice",
];

/// The paths of the [`Files`] fixtures, and of two paths that are not files.
struct FixturePaths {
    ca: String,
    cert: String,
    key: String,
    password: String,
    token: String,
    missing: String,
    directory: String,
    blank: String,
    not_utf8: String,
}

impl FixturePaths {
    fn new(files: &Files) -> Self {
        Self {
            ca: files.path("ca.pem"),
            cert: files.path("cert.pem"),
            key: files.path("key.pem"),
            password: files.path("password"),
            token: files.path("token"),
            missing: files.path("missing"),
            directory: files.path("directory"),
            blank: files.path("blank-password"),
            not_utf8: files.path("not-utf8-password"),
        }
    }
}

/// Cases where a TLS flag is out of place or missing.
fn tls_refusals(paths: &FixturePaths) -> Vec<RefusalCase<'_>> {
    let ca = paths.ca.as_str();
    let cert = paths.cert.as_str();
    let key = paths.key.as_str();
    let password = paths.password.as_str();
    let ca_and_name = [
        "--wal-tls-ca-path",
        ca,
        "--wal-tls-server-name",
        "localhost",
    ];
    let plain = [&PLAIN_USER[..], &["--wal-sasl-password-path", password]].concat();
    vec![
        refusal_case!(
            "a CA file with PLAINTEXT",
            vec!["--wal-tls-ca-path", ca],
            refused!(WalClientSecurityError::TlsFlagWithoutTlsProtocol {
                flag: "--wal-tls-ca-path",
                protocol: WalSecurityProtocol::Plaintext,
            }),
        ),
        refusal_case!(
            "a server name with PLAINTEXT",
            vec!["--wal-tls-server-name", "localhost"],
            refused!(WalClientSecurityError::TlsFlagWithoutTlsProtocol {
                flag: "--wal-tls-server-name",
                protocol: WalSecurityProtocol::Plaintext,
            }),
        ),
        refusal_case!(
            "a certificate with PLAINTEXT",
            vec!["--wal-tls-cert-path", cert],
            refused!(WalClientSecurityError::TlsFlagWithoutTlsProtocol {
                flag: "--wal-tls-cert-path",
                protocol: WalSecurityProtocol::Plaintext,
            }),
        ),
        refusal_case!(
            "a key with SASL_PLAINTEXT",
            [&SASL_PLAINTEXT[..], &plain, &["--wal-tls-key-path", key]].concat(),
            refused!(WalClientSecurityError::TlsFlagWithoutTlsProtocol {
                flag: "--wal-tls-key-path",
                protocol: WalSecurityProtocol::SaslPlaintext,
            }),
        ),
        refusal_case!(
            "SSL without a CA file",
            [&SSL[..], &["--wal-tls-server-name", "localhost"]].concat(),
            refused!(WalClientSecurityError::MissingTlsCaPath {
                protocol: WalSecurityProtocol::Ssl,
            }),
        ),
        refusal_case!(
            "SASL_SSL without a server name",
            [&SASL_SSL[..], &plain, &["--wal-tls-ca-path", ca]].concat(),
            refused!(WalClientSecurityError::MissingTlsServerName {
                protocol: WalSecurityProtocol::SaslSsl,
            }),
        ),
        refusal_case!(
            "a certificate without a key",
            [&SSL[..], &ca_and_name, &["--wal-tls-cert-path", cert]].concat(),
            refused!(WalClientSecurityError::TlsCertWithoutKey),
        ),
        refusal_case!(
            "a key without a certificate",
            [&SSL[..], &ca_and_name, &["--wal-tls-key-path", key]].concat(),
            refused!(WalClientSecurityError::TlsKeyWithoutCert),
        ),
    ]
}

/// Cases where a SASL flag is out of place or missing.
fn sasl_refusals(paths: &FixturePaths) -> Vec<RefusalCase<'_>> {
    let ca = paths.ca.as_str();
    let password = paths.password.as_str();
    let token = paths.token.as_str();
    let ca_and_name = [
        "--wal-tls-ca-path",
        ca,
        "--wal-tls-server-name",
        "localhost",
    ];
    let oauthbearer = [
        "--wal-sasl-mechanism",
        "OAUTHBEARER",
        "--wal-sasl-oauthbearer-token-path",
        token,
    ];
    let scram_512 = [
        "--wal-sasl-mechanism",
        "SCRAM-SHA-512",
        "--wal-sasl-username",
        "alice",
    ];
    vec![
        refusal_case!(
            "a mechanism with PLAINTEXT",
            vec!["--wal-sasl-mechanism", "PLAIN"],
            refused!(WalClientSecurityError::SaslFlagWithoutSaslProtocol {
                flag: "--wal-sasl-mechanism",
                protocol: WalSecurityProtocol::Plaintext,
            }),
        ),
        refusal_case!(
            "a user name with SSL",
            [&SSL[..], &ca_and_name, &["--wal-sasl-username", "alice"]].concat(),
            refused!(WalClientSecurityError::SaslFlagWithoutSaslProtocol {
                flag: "--wal-sasl-username",
                protocol: WalSecurityProtocol::Ssl,
            }),
        ),
        refusal_case!(
            "a password file with SSL",
            [
                &SSL[..],
                &ca_and_name,
                &["--wal-sasl-password-path", password],
            ]
            .concat(),
            refused!(WalClientSecurityError::SaslFlagWithoutSaslProtocol {
                flag: "--wal-sasl-password-path",
                protocol: WalSecurityProtocol::Ssl,
            }),
        ),
        refusal_case!(
            "a token file with SSL",
            [
                &SSL[..],
                &ca_and_name,
                &["--wal-sasl-oauthbearer-token-path", token],
            ]
            .concat(),
            refused!(WalClientSecurityError::SaslFlagWithoutSaslProtocol {
                flag: "--wal-sasl-oauthbearer-token-path",
                protocol: WalSecurityProtocol::Ssl,
            }),
        ),
        refusal_case!(
            "SASL_PLAINTEXT without a mechanism",
            SASL_PLAINTEXT.to_vec(),
            refused!(WalClientSecurityError::MissingSaslMechanism {
                protocol: WalSecurityProtocol::SaslPlaintext,
            }),
        ),
        refusal_case!(
            "GSSAPI",
            [&SASL_PLAINTEXT[..], &["--wal-sasl-mechanism", "GSSAPI"]].concat(),
            refused!(WalClientSecurityError::GssapiUnsupported),
        ),
        refusal_case!(
            "PLAIN without a user name",
            [
                &SASL_PLAINTEXT[..],
                &["--wal-sasl-mechanism", "PLAIN"],
                &["--wal-sasl-password-path", password],
            ]
            .concat(),
            refused!(WalClientSecurityError::MissingSaslUsername {
                mechanism: WalSaslMechanism::Plain,
            }),
        ),
        refusal_case!(
            "SCRAM-SHA-256 without a password file",
            [
                &SASL_PLAINTEXT[..],
                &["--wal-sasl-mechanism", "SCRAM-SHA-256"],
                &["--wal-sasl-username", "alice"],
            ]
            .concat(),
            refused!(WalClientSecurityError::MissingSaslPasswordPath {
                mechanism: WalSaslMechanism::ScramSha256,
            }),
        ),
        refusal_case!(
            "SCRAM-SHA-512 with a token file",
            [
                &SASL_PLAINTEXT[..],
                &scram_512,
                &["--wal-sasl-password-path", password],
                &["--wal-sasl-oauthbearer-token-path", token],
            ]
            .concat(),
            refused!(WalClientSecurityError::SaslFlagNotUsedByMechanism {
                flag: "--wal-sasl-oauthbearer-token-path",
                mechanism: WalSaslMechanism::ScramSha512,
            }),
        ),
        refusal_case!(
            "OAUTHBEARER without a token file",
            [
                &SASL_PLAINTEXT[..],
                &["--wal-sasl-mechanism", "OAUTHBEARER"],
            ]
            .concat(),
            refused!(WalClientSecurityError::MissingOAuthBearerTokenPath),
        ),
        refusal_case!(
            "OAUTHBEARER with a user name",
            [
                &SASL_PLAINTEXT[..],
                &oauthbearer,
                &["--wal-sasl-username", "alice"],
            ]
            .concat(),
            refused!(WalClientSecurityError::SaslFlagNotUsedByMechanism {
                flag: "--wal-sasl-username",
                mechanism: WalSaslMechanism::OAuthBearer,
            }),
        ),
        refusal_case!(
            "OAUTHBEARER with a password file",
            [
                &SASL_PLAINTEXT[..],
                &oauthbearer,
                &["--wal-sasl-password-path", password],
            ]
            .concat(),
            refused!(WalClientSecurityError::SaslFlagNotUsedByMechanism {
                flag: "--wal-sasl-password-path",
                mechanism: WalSaslMechanism::OAuthBearer,
            }),
        ),
    ]
}

/// Cases where a named file cannot be read or holds no usable password.
fn file_refusals(paths: &FixturePaths) -> Vec<RefusalCase<'_>> {
    let cert = paths.cert.as_str();
    let key = paths.key.as_str();
    let missing = paths.missing.as_str();
    let directory = paths.directory.as_str();
    let blank = paths.blank.as_str();
    let not_utf8 = paths.not_utf8.as_str();
    let ca_and_name = [
        "--wal-tls-ca-path",
        &*paths.ca,
        "--wal-tls-server-name",
        "localhost",
    ];
    vec![
        refusal_case!(
            "an unreadable CA file",
            [
                &SSL[..],
                &[
                    "--wal-tls-ca-path",
                    missing,
                    "--wal-tls-server-name",
                    "localhost",
                ],
            ]
            .concat(),
            refused!(WalClientSecurityError::UnreadableFile {
                flag: "--wal-tls-ca-path",
                path,
                ..
            } if path == Path::new(missing)),
        ),
        refusal_case!(
            "a directory as the CA file",
            [
                &SSL[..],
                &[
                    "--wal-tls-ca-path",
                    directory,
                    "--wal-tls-server-name",
                    "localhost",
                ],
            ]
            .concat(),
            refused!(WalClientSecurityError::UnreadableFile {
                flag: "--wal-tls-ca-path",
                path,
                ..
            } if path == Path::new(directory)),
        ),
        refusal_case!(
            "an unreadable certificate",
            [
                &SSL[..],
                &ca_and_name,
                &["--wal-tls-cert-path", missing, "--wal-tls-key-path", key],
            ]
            .concat(),
            refused!(WalClientSecurityError::UnreadableFile {
                flag: "--wal-tls-cert-path",
                path,
                ..
            } if path == Path::new(missing)),
        ),
        refusal_case!(
            "an unreadable key",
            [
                &SSL[..],
                &ca_and_name,
                &["--wal-tls-cert-path", cert, "--wal-tls-key-path", missing],
            ]
            .concat(),
            refused!(WalClientSecurityError::UnreadableFile {
                flag: "--wal-tls-key-path",
                path,
                ..
            } if path == Path::new(missing)),
        ),
        refusal_case!(
            "an unreadable password file",
            [
                &SASL_PLAINTEXT[..],
                &PLAIN_USER,
                &["--wal-sasl-password-path", missing],
            ]
            .concat(),
            refused!(WalClientSecurityError::UnreadableFile {
                flag: "--wal-sasl-password-path",
                path,
                ..
            } if path == Path::new(missing)),
        ),
        refusal_case!(
            "an unreadable token file",
            [
                &SASL_PLAINTEXT[..],
                &["--wal-sasl-mechanism", "OAUTHBEARER"],
                &["--wal-sasl-oauthbearer-token-path", missing],
            ]
            .concat(),
            refused!(WalClientSecurityError::UnreadableFile {
                flag: "--wal-sasl-oauthbearer-token-path",
                path,
                ..
            } if path == Path::new(missing)),
        ),
        refusal_case!(
            "a password file that is not UTF-8",
            [
                &SASL_PLAINTEXT[..],
                &PLAIN_USER,
                &["--wal-sasl-password-path", not_utf8],
            ]
            .concat(),
            refused!(WalClientSecurityError::PasswordFileNotUtf8 { path }
                if path == Path::new(not_utf8)),
        ),
        refusal_case!(
            "a password file of line breaks only",
            [
                &SASL_PLAINTEXT[..],
                &PLAIN_USER,
                &["--wal-sasl-password-path", blank],
            ]
            .concat(),
            refused!(WalClientSecurityError::EmptyPasswordFile { path }
                if path == Path::new(blank)),
        ),
    ]
}

#[test]
fn every_invalid_combination_is_refused_with_its_own_error() {
    let files = Files::new();
    let paths = FixturePaths::new(&files);
    let cases = [tls_refusals, sasl_refusals, file_refusals]
        .into_iter()
        .flat_map(|refusals| refusals(&paths));
    for case in cases {
        // `.err()` keeps an accepted policy, and its password, out of the
        // failure output.
        assert!(let Some(error) = load(&case.args).err(), "{} was accepted", case.name);
        assert!((case.expected)(&error), "{}: {error}", case.name);
    }
}

#[test]
fn gssapi_is_refused_as_not_supported_yet() {
    let result = load(&[
        "--wal-security-protocol",
        "SASL_SSL",
        "--wal-sasl-mechanism",
        "GSSAPI",
    ]);
    assert!(let Some(error) = result.err());
    assert!(error.to_string().contains("GSSAPI is not supported yet"));
}

#[test]
fn the_password_file_loses_only_its_trailing_line_breaks() {
    let files = Files::new();
    for case in [
        PasswordLineCase {
            contents: "hunter2",
            expected: "hunter2",
        },
        PasswordLineCase {
            contents: "hunter2\n",
            expected: "hunter2",
        },
        PasswordLineCase {
            contents: "hunter2\r\n",
            expected: "hunter2",
        },
        PasswordLineCase {
            contents: "hunter2\n\n",
            expected: "hunter2",
        },
        PasswordLineCase {
            contents: " hunter 2 \n",
            expected: " hunter 2 ",
        },
        PasswordLineCase {
            contents: "\nhunter2\n",
            expected: "\nhunter2",
        },
    ] {
        let path = files.dir.path().join("line-breaks");
        fs::write(&path, case.contents).expect("write password file");
        assert!(let Ok(password) = read_password_file(&path));
        assert!(password == case.expected);
    }
}

#[test]
fn the_password_never_appears_in_debug_output_or_in_an_error() {
    let files = Files::new();
    let password_path = files.path("password");
    let not_utf8 = files.path("not-utf8-password");
    let scram = [
        "--wal-security-protocol",
        "SASL_SSL",
        "--wal-sasl-mechanism",
        "SCRAM-SHA-512",
        "--wal-sasl-username",
        "alice",
        "--wal-sasl-password-path",
    ];

    let mut renderings: Vec<SecretRendering> = Vec::new();
    assert!(let Ok(args) = parse(&[&scram[..], &[password_path.as_str()]].concat()));
    renderings.push(SecretRendering {
        name: "the arguments",
        rendering: format!("{args:?}"),
    });

    // The password file reads first, and the missing CA file fails after it.
    assert!(let Some(after_read) = args.load().err());
    assert!(matches!(
        after_read,
        WalClientSecurityError::MissingTlsCaPath { .. }
    ));
    renderings.push(SecretRendering {
        name: "an error after the read",
        rendering: after_read.to_string(),
    });
    renderings.push(SecretRendering {
        name: "an error after the read, as Debug",
        rendering: format!("{after_read:?}"),
    });

    assert!(let Some(utf8_error) = load(&[&scram[..], &[not_utf8.as_str()]].concat()).err());
    renderings.push(SecretRendering {
        name: "a UTF-8 error",
        rendering: utf8_error.to_string(),
    });
    renderings.push(SecretRendering {
        name: "a UTF-8 error, as Debug",
        rendering: format!("{utf8_error:?}"),
    });

    for rendering in renderings {
        let leaked = rendering.rendering.contains(SECRET);
        assert!(!leaked, "{} holds the password", rendering.name);
    }
}

#[test]
fn with_client_security_replaces_only_the_security_policy() {
    let options = ConnectionOptions {
        client_id: "krabka-wal-test".to_string(),
        socket_connection_setup_timeout: secs(3),
        request_timeout: secs(7),
        ..ConnectionOptions::default()
    };
    let security = ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: "alice".to_string(),
            password: SECRET.to_string(),
        }),
        sasl_host: None,
    };

    let secured = with_client_security(options.clone(), Some(&security));
    assert_same_options_apart_from_security(&secured, &options);
    assert!(let Some(applied) = secured.security.as_deref());
    assert_same_security(applied, &security);

    let plaintext = with_client_security(secured, None);
    assert_same_options_apart_from_security(&plaintext, &options);
    assert!(plaintext.security.is_none());
}

fn assert_same_options_apart_from_security(
    actual: &ConnectionOptions,
    expected: &ConnectionOptions,
) {
    let ConnectionOptions {
        client_id,
        dns_timeout,
        socket_connection_setup_timeout,
        request_timeout,
        dispatch_queue_capacity,
        frame_max,
        ..
    } = actual;
    assert!(*client_id == expected.client_id);
    assert!(*dns_timeout == expected.dns_timeout);
    assert!(*socket_connection_setup_timeout == expected.socket_connection_setup_timeout);
    assert!(*request_timeout == expected.request_timeout);
    assert!(*dispatch_queue_capacity == expected.dispatch_queue_capacity);
    assert!(*frame_max == expected.frame_max);
}
