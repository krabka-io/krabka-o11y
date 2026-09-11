use std::{
    fs,
    path::{Path, PathBuf},
};

use assert2::assert;
use clap::Parser;
use krabka_client_core::{ClientSecurity, ConnectionOptions, SaslCredentials, TlsConnectorConfig};
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
        for (name, contents) in [
            ("ca.pem", b"ca".as_slice()),
            ("cert.pem", b"cert".as_slice()),
            ("key.pem", b"key".as_slice()),
            ("token", b"header.payload.signature\n".as_slice()),
            ("password", format!("{SECRET}\n").as_bytes()),
            ("blank-password", b"\r\n\n".as_slice()),
            (
                "not-utf8-password",
                [SECRET.as_bytes(), &[0xff]].concat().as_slice(),
            ),
        ] {
            fs::write(dir.path().join(name), contents).expect("write fixture");
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

type TlsFields<'a> = (&'a Option<PathBuf>, &'a str, &'a Option<(PathBuf, PathBuf)>);

fn tls_fields(tls: &TlsConnectorConfig) -> TlsFields<'_> {
    let TlsConnectorConfig {
        trust_roots_pem,
        server_name,
        client_identity,
    } = tls;
    (trust_roots_pem, server_name.as_str(), client_identity)
}

/// Every credential field except the password, which a failure may print.
fn public_credential_fields(
    credentials: &SaslCredentials,
) -> (SaslMechanism, Vec<&str>, Option<&Path>) {
    let mechanism = credentials.mechanism();
    match credentials {
        SaslCredentials::Plain { username, .. } | SaslCredentials::Scram { username, .. } => {
            (mechanism, vec![username.as_str()], None)
        }
        SaslCredentials::Gssapi {
            keytab_path,
            client_principal,
            service_name,
            kdc_url,
        } => (
            mechanism,
            vec![
                client_principal.as_str(),
                service_name.as_str(),
                kdc_url.as_str(),
            ],
            Some(keytab_path.as_path()),
        ),
        SaslCredentials::OAuthBearer { token_path } => {
            (mechanism, Vec::new(), Some(token_path.as_path()))
        }
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

    let command_line: Vec<&str> = flags
        .iter()
        .zip(values)
        .flat_map(|(flag, value)| [*flag, value])
        .collect();
    assert!(let Ok(from_flags) = parse(&command_line));
    assert!(from_flags == expected);

    let environment: Vec<(&str, Option<&str>)> = ENVIRONMENT
        .iter()
        .zip(values)
        .map(|(name, value)| (*name, Some(value)))
        .collect();
    let from_environment = temp_env::with_vars(environment, || {
        Cli::try_parse_from([PROGRAM]).map(|cli| cli.wal)
    });
    assert!(let Ok(from_environment) = from_environment);
    assert!(from_environment == expected);
}

#[test]
fn the_protocol_and_the_mechanism_parse_from_their_kafka_names() {
    for (value, expected) in [
        ("PLAINTEXT", WalSecurityProtocol::Plaintext),
        ("SSL", WalSecurityProtocol::Ssl),
        ("SASL_PLAINTEXT", WalSecurityProtocol::SaslPlaintext),
        ("SASL_SSL", WalSecurityProtocol::SaslSsl),
        ("sasl_ssl", WalSecurityProtocol::SaslSsl),
    ] {
        assert!(let Ok(args) = parse(&["--wal-security-protocol", value]));
        assert!(args.wal_security_protocol == expected);
        assert!(expected.to_string() == value.to_uppercase());
    }
    for (value, expected) in [
        ("PLAIN", WalSaslMechanism::Plain),
        ("SCRAM-SHA-256", WalSaslMechanism::ScramSha256),
        ("SCRAM-SHA-512", WalSaslMechanism::ScramSha512),
        ("OAUTHBEARER", WalSaslMechanism::OAuthBearer),
        ("GSSAPI", WalSaslMechanism::Gssapi),
    ] {
        assert!(let Ok(args) = parse(&["--wal-sasl-mechanism", value]));
        assert!(args.wal_sasl_mechanism == Some(expected));
        assert!(expected.to_string() == value);
        assert!(SaslMechanism::from(expected).wire_name() == value);
    }
    for (protocol, tls, sasl) in [
        (WalSecurityProtocol::Plaintext, false, false),
        (WalSecurityProtocol::Ssl, true, false),
        (WalSecurityProtocol::SaslPlaintext, false, true),
        (WalSecurityProtocol::SaslSsl, true, true),
    ] {
        assert!(protocol.requires_tls() == tls);
        assert!(protocol.requires_sasl() == sasl);
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
    let (ca, password_path) = (files.path("ca.pem"), files.path("password"));
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

    let expected = ClientSecurity {
        protocol: ListenerProtocol::SaslSsl,
        tls: Some(TlsConnectorConfig {
            trust_roots_pem: Some(PathBuf::from(&ca)),
            server_name: "broker.krabka.test".to_string(),
            client_identity: None,
        }),
        sasl: Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username: "krabka-metrics".to_string(),
            password: SECRET.to_string(),
        }),
        sasl_host: None,
    };
    assert_same_security(&actual, &expected);
}

#[test]
fn each_protocol_and_mechanism_builds_the_policy_it_names() {
    let files = Files::new();
    let (ca, cert, key) = (
        files.path("ca.pem"),
        files.path("cert.pem"),
        files.path("key.pem"),
    );
    let (password_path, token) = (files.path("password"), files.path("token"));
    let tls = TlsConnectorConfig {
        trust_roots_pem: Some(PathBuf::from(&ca)),
        server_name: "localhost".to_string(),
        client_identity: None,
    };
    let cases: [(&str, Vec<&str>, ClientSecurity); 5] = [
        (
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
        (
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
                tls: Some(TlsConnectorConfig {
                    client_identity: Some((PathBuf::from(&cert), PathBuf::from(&key))),
                    ..tls.clone()
                }),
                sasl: None,
                sasl_host: None,
            },
        ),
        (
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
        (
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
                }),
                sasl_host: None,
            },
        ),
        (
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
                    token_path: PathBuf::from(&token),
                }),
                sasl_host: None,
            },
        ),
    ];
    for (name, args, expected) in &cases {
        let result = load(args).map_err(|error| format!("{name}: {error}"));
        assert!(let Ok(Some(actual)) = result);
        assert_same_security(&actual, expected);
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
type RefusalCase<'a> = (&'static str, Vec<&'a str>, Refusal<'a>);

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
    let (ca, cert, key, password) = (&*paths.ca, &*paths.cert, &*paths.key, &*paths.password);
    let ca_and_name = [
        "--wal-tls-ca-path",
        ca,
        "--wal-tls-server-name",
        "localhost",
    ];
    let plain = [&PLAIN_USER[..], &["--wal-sasl-password-path", password]].concat();
    vec![
        (
            "a CA file with PLAINTEXT",
            vec!["--wal-tls-ca-path", ca],
            refused!(WalClientSecurityError::TlsFlagWithoutTlsProtocol {
                flag: "--wal-tls-ca-path",
                protocol: WalSecurityProtocol::Plaintext,
            }),
        ),
        (
            "a server name with PLAINTEXT",
            vec!["--wal-tls-server-name", "localhost"],
            refused!(WalClientSecurityError::TlsFlagWithoutTlsProtocol {
                flag: "--wal-tls-server-name",
                protocol: WalSecurityProtocol::Plaintext,
            }),
        ),
        (
            "a certificate with PLAINTEXT",
            vec!["--wal-tls-cert-path", cert],
            refused!(WalClientSecurityError::TlsFlagWithoutTlsProtocol {
                flag: "--wal-tls-cert-path",
                protocol: WalSecurityProtocol::Plaintext,
            }),
        ),
        (
            "a key with SASL_PLAINTEXT",
            [&SASL_PLAINTEXT[..], &plain, &["--wal-tls-key-path", key]].concat(),
            refused!(WalClientSecurityError::TlsFlagWithoutTlsProtocol {
                flag: "--wal-tls-key-path",
                protocol: WalSecurityProtocol::SaslPlaintext,
            }),
        ),
        (
            "SSL without a CA file",
            [&SSL[..], &["--wal-tls-server-name", "localhost"]].concat(),
            refused!(WalClientSecurityError::MissingTlsCaPath {
                protocol: WalSecurityProtocol::Ssl,
            }),
        ),
        (
            "SASL_SSL without a server name",
            [&SASL_SSL[..], &plain, &["--wal-tls-ca-path", ca]].concat(),
            refused!(WalClientSecurityError::MissingTlsServerName {
                protocol: WalSecurityProtocol::SaslSsl,
            }),
        ),
        (
            "a certificate without a key",
            [&SSL[..], &ca_and_name, &["--wal-tls-cert-path", cert]].concat(),
            refused!(WalClientSecurityError::TlsCertWithoutKey),
        ),
        (
            "a key without a certificate",
            [&SSL[..], &ca_and_name, &["--wal-tls-key-path", key]].concat(),
            refused!(WalClientSecurityError::TlsKeyWithoutCert),
        ),
    ]
}

/// Cases where a SASL flag is out of place or missing.
fn sasl_refusals(paths: &FixturePaths) -> Vec<RefusalCase<'_>> {
    let (ca, password, token) = (&*paths.ca, &*paths.password, &*paths.token);
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
        (
            "a mechanism with PLAINTEXT",
            vec!["--wal-sasl-mechanism", "PLAIN"],
            refused!(WalClientSecurityError::SaslFlagWithoutSaslProtocol {
                flag: "--wal-sasl-mechanism",
                protocol: WalSecurityProtocol::Plaintext,
            }),
        ),
        (
            "a user name with SSL",
            [&SSL[..], &ca_and_name, &["--wal-sasl-username", "alice"]].concat(),
            refused!(WalClientSecurityError::SaslFlagWithoutSaslProtocol {
                flag: "--wal-sasl-username",
                protocol: WalSecurityProtocol::Ssl,
            }),
        ),
        (
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
        (
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
        (
            "SASL_PLAINTEXT without a mechanism",
            SASL_PLAINTEXT.to_vec(),
            refused!(WalClientSecurityError::MissingSaslMechanism {
                protocol: WalSecurityProtocol::SaslPlaintext,
            }),
        ),
        (
            "GSSAPI",
            [&SASL_PLAINTEXT[..], &["--wal-sasl-mechanism", "GSSAPI"]].concat(),
            refused!(WalClientSecurityError::GssapiUnsupported),
        ),
        (
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
        (
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
        (
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
        (
            "OAUTHBEARER without a token file",
            [
                &SASL_PLAINTEXT[..],
                &["--wal-sasl-mechanism", "OAUTHBEARER"],
            ]
            .concat(),
            refused!(WalClientSecurityError::MissingOAuthBearerTokenPath),
        ),
        (
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
        (
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
    let (cert, key, missing, directory) = (
        &*paths.cert,
        &*paths.key,
        &*paths.missing,
        &*paths.directory,
    );
    let (blank, not_utf8) = (&*paths.blank, &*paths.not_utf8);
    let ca_and_name = [
        "--wal-tls-ca-path",
        &*paths.ca,
        "--wal-tls-server-name",
        "localhost",
    ];
    vec![
        (
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
        (
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
        (
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
        (
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
        (
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
        (
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
        (
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
        (
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
    for (name, args, is_expected) in cases {
        // `.err()` keeps an accepted policy, and its password, out of the
        // failure output.
        assert!(let Some(error) = load(&args).err(), "{name} was accepted");
        assert!(is_expected(&error), "{name}: {error}");
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
    for (contents, expected) in [
        ("hunter2", "hunter2"),
        ("hunter2\n", "hunter2"),
        ("hunter2\r\n", "hunter2"),
        ("hunter2\n\n", "hunter2"),
        (" hunter 2 \n", " hunter 2 "),
        ("\nhunter2\n", "\nhunter2"),
    ] {
        let path = files.dir.path().join("line-breaks");
        fs::write(&path, contents).expect("write password file");
        assert!(let Ok(password) = read_password_file(&path));
        assert!(password == expected);
    }
}

#[test]
fn the_password_never_appears_in_debug_output_or_in_an_error() {
    let files = Files::new();
    let (password_path, not_utf8) = (files.path("password"), files.path("not-utf8-password"));
    let scram = [
        "--wal-security-protocol",
        "SASL_SSL",
        "--wal-sasl-mechanism",
        "SCRAM-SHA-512",
        "--wal-sasl-username",
        "alice",
        "--wal-sasl-password-path",
    ];

    let mut renderings = Vec::new();
    assert!(let Ok(args) = parse(&[&scram[..], &[password_path.as_str()]].concat()));
    renderings.push(("the arguments", format!("{args:?}")));

    // The password file reads first, and the missing CA file fails after it.
    assert!(let Some(after_read) = args.load().err());
    assert!(matches!(
        after_read,
        WalClientSecurityError::MissingTlsCaPath { .. }
    ));
    renderings.push(("an error after the read", after_read.to_string()));
    renderings.push((
        "an error after the read, as Debug",
        format!("{after_read:?}"),
    ));

    assert!(let Some(utf8_error) = load(&[&scram[..], &[not_utf8.as_str()]].concat()).err());
    renderings.push(("a UTF-8 error", utf8_error.to_string()));
    renderings.push(("a UTF-8 error, as Debug", format!("{utf8_error:?}")));

    for (name, rendering) in renderings {
        let leaked = rendering.contains(SECRET);
        assert!(!leaked, "{name} holds the password");
    }
}

#[test]
fn with_client_security_replaces_only_the_security_policy() {
    let options = ConnectionOptions {
        client_id: "krabka-wal-test".to_string(),
        connect_timeout: secs(3),
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
        connect_timeout,
        request_timeout,
        dispatch_queue_capacity,
        frame_max,
        security: _,
    } = actual;
    assert!(*client_id == expected.client_id);
    assert!(*dns_timeout == expected.dns_timeout);
    assert!(*connect_timeout == expected.connect_timeout);
    assert!(*request_timeout == expected.request_timeout);
    assert!(*dispatch_queue_capacity == expected.dispatch_queue_capacity);
    assert!(*frame_max == expected.frame_max);
}
