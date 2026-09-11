//! The write-ahead log security flags against a real broker.
//!
//! The unit tests prove the policy that `WalClientSecurityArgs::load` builds.
//! They do not prove that `krabka-client-core` negotiates that policy with a
//! broker that requires it. So each case here starts a `krabka-broker` in
//! process with a secured listener. A client with the loaded policy must get
//! a metadata response, and a client without it must not.

use std::{fs, net::SocketAddr, path::Path, time::Duration};

use assert2::assert;
use clap::Parser;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use krabka_client_admin::AdminClient;
use krabka_client_core::{ClientSecurity, ConnectionOptions};
use krabka_observability::wal_client_security::{WalClientSecurityArgs, with_client_security};
use krabka_security::{ClientAuthMode, ListenerProtocol, SaslMechanism, TlsConfig};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use tempfile::TempDir;

/// How long one connection attempt may take before the test counts it as
/// not served.
const DEADLINE: Duration = Duration::from_secs(20);

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    wal: WalClientSecurityArgs,
}

fn load(args: &[&str]) -> Option<ClientSecurity> {
    Cli::try_parse_from(std::iter::once("krabka-wal-client-security").chain(args.iter().copied()))
        .expect("the flags parse")
        .wal
        .load()
        // The error text holds no secret. A loaded policy holds the password,
        // so nothing here prints one.
        .map_err(|error| error.to_string())
        .expect("the policy loads")
}

fn path(dir: &TempDir, name: &str) -> String {
    dir.path()
        .join(name)
        .to_str()
        .expect("the temporary directory is UTF-8")
        .to_owned()
}

/// Writes a CA, a `localhost` server certificate that the CA signs, the
/// server key, and a second CA that signs nothing.
fn write_certificates(dir: &Path) {
    let (ca_params, ca_key) = certificate_authority("Krabka test CA");
    fs::write(
        dir.join("ca.pem"),
        ca_params.self_signed(&ca_key).expect("sign the CA").pem(),
    )
    .expect("write the CA");
    let issuer = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate().expect("server key");
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("server parameters");
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_cert = server_params
        .signed_by(&server_key, &issuer)
        .expect("sign the server certificate");
    fs::write(dir.join("server.pem"), server_cert.pem()).expect("write the certificate");
    fs::write(dir.join("server.key"), server_key.serialize_pem()).expect("write the key");

    let (other_params, other_key) = certificate_authority("Another CA");
    fs::write(
        dir.join("other-ca.pem"),
        other_params
            .self_signed(&other_key)
            .expect("sign the other CA")
            .pem(),
    )
    .expect("write the other CA");
}

fn certificate_authority(name: &str) -> (CertificateParams, KeyPair) {
    let key = KeyPair::generate().expect("CA key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    (params, key)
}

/// Starts a broker whose only listener speaks `protocol`.
async fn start_broker(
    log_dir: &Path,
    protocol: ListenerProtocol,
    configure: impl FnOnce(&mut BrokerConfig),
) -> BrokerHandle {
    // The advertised address must be the bound one, and the listener cannot
    // bind port zero and advertise the port it got.
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    let addr: SocketAddr = reserved.local_addr().expect("reserved address");
    drop(reserved);

    let name = match protocol {
        ListenerProtocol::Plaintext => "PLAINTEXT",
        ListenerProtocol::Ssl => "SSL",
        ListenerProtocol::SaslPlaintext => "SASL_PLAINTEXT",
        ListenerProtocol::SaslSsl => "SASL_SSL",
    };
    let mut config = BrokerConfig::for_tests(log_dir.to_path_buf());
    config.listeners = vec![ListenerSpec {
        name: name.to_string(),
        bind_addr: addr,
        advertised: addr.to_string(),
        protocol,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    config.inter_broker_listener_name = name.to_string();
    configure(&mut config);
    Broker::start(config).await.expect("broker start")
}

/// Whether the broker answers a metadata request on a connection that
/// `options` configures.
async fn metadata_served(broker: &BrokerHandle, options: ConnectionOptions) -> bool {
    let bootstrap = [broker.listen_addr().to_string()];
    let attempt = async {
        let mut admin = AdminClient::connect_with_options(&bootstrap, options)
            .await
            .ok()?;
        admin.metadata(&[]).await.ok()
    };
    tokio::time::timeout(DEADLINE, attempt)
        .await
        .ok()
        .flatten()
        .is_some()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ssl_listener_serves_the_loaded_policy_and_refuses_plaintext_or_another_ca() {
    let files = tempfile::tempdir().expect("certificate directory");
    write_certificates(files.path());
    // `load` installs the process-wide `rustls` provider, which the broker's
    // TLS listener also needs, so it runs before the broker starts.
    let security = load(&[
        "--wal-security-protocol",
        "SSL",
        "--wal-tls-ca-path",
        &path(&files, "ca.pem"),
        "--wal-tls-server-name",
        "localhost",
    ]);
    let other_ca = load(&[
        "--wal-security-protocol",
        "SSL",
        "--wal-tls-ca-path",
        &path(&files, "other-ca.pem"),
        "--wal-tls-server-name",
        "localhost",
    ]);

    let log_dir = tempfile::tempdir().expect("broker log directory");
    let broker = start_broker(log_dir.path(), ListenerProtocol::Ssl, |config| {
        config.tls_config = Some(TlsConfig {
            cert_chain_path: files.path().join("server.pem"),
            private_key_path: files.path().join("server.key"),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        });
    })
    .await;

    let with_policy = with_client_security(ConnectionOptions::default(), security.as_ref());
    assert!(metadata_served(&broker, with_policy).await);
    assert!(!metadata_served(&broker, ConnectionOptions::default()).await);
    let with_other_ca = with_client_security(ConnectionOptions::default(), other_ca.as_ref());
    assert!(!metadata_served(&broker, with_other_ca).await);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sasl_plain_listener_serves_the_password_file_and_refuses_a_wrong_or_absent_one() {
    let files = tempfile::tempdir().expect("credential directory");
    fs::write(files.path().join("password"), "wal-secret\n").expect("write the password");
    fs::write(files.path().join("wrong-password"), "not-the-secret\n")
        .expect("write the wrong password");
    let plain = |password_file: &str| {
        load(&[
            "--wal-security-protocol",
            "SASL_PLAINTEXT",
            "--wal-sasl-mechanism",
            "PLAIN",
            "--wal-sasl-username",
            "krabka-wal",
            "--wal-sasl-password-path",
            &path(&files, password_file),
        ])
    };
    let (security, wrong) = (plain("password"), plain("wrong-password"));

    let log_dir = tempfile::tempdir().expect("broker log directory");
    let broker = start_broker(log_dir.path(), ListenerProtocol::SaslPlaintext, |config| {
        config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        config
            .plain_credentials
            .insert("krabka-wal".to_string(), "wal-secret".to_string());
    })
    .await;

    let with_policy = with_client_security(ConnectionOptions::default(), security.as_ref());
    assert!(metadata_served(&broker, with_policy).await);
    let with_wrong = with_client_security(ConnectionOptions::default(), wrong.as_ref());
    assert!(!metadata_served(&broker, with_wrong).await);
    assert!(!metadata_served(&broker, ConnectionOptions::default()).await);
    broker.shutdown().await;
}
