use krabka_client_core::{ClientSecurity, SaslCredentials};
use krabka_security::ListenerProtocol;

use super::*;

// The SASL user name and password that a policy carries, or `None`.
fn plain_credentials_for_test(
    security: Option<&ClientSecurity>,
) -> Option<(ListenerProtocol, String, String)> {
    let security = security?;
    match security.sasl.as_ref()? {
        SaslCredentials::Plain { username, password } => {
            Some((security.protocol, username.clone(), password.clone()))
        }
        _ => None,
    }
}

// The WAL client security flags parse through the `ServiceConfig`, and the
// policy they give reaches the admin connection of the query authorizer and
// the ingest limiter, and both deferred connects of the querier. With no
// flag set, every one of them connects in plain text.
#[test]
pub(crate) fn wal_client_security_flags_reach_every_wal_connection() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let password_path = dir.path().join("wal-password");
    std::fs::write(&password_path, "wal-secret\n").expect("the password file writes");
    let password_flag = format!("--wal-sasl-password-path={}", password_path.display());
    let base = [
        "krabka-observability",
        "--target=querier",
        "--wal-bootstrap-server=broker:9092",
    ];
    let secured = ServiceConfig::try_parse_from(base.iter().copied().chain([
        "--wal-security-protocol=SASL_PLAINTEXT",
        "--wal-sasl-mechanism=PLAIN",
        "--wal-sasl-username=krabka-logs",
        password_flag.as_str(),
    ]))
    .expect("the secured flags parse");
    let plain = ServiceConfig::try_parse_from(base).expect("the plain flags parse");
    let policy = ClientResourcePolicy::default();
    let expected = Some((
        ListenerProtocol::SaslPlaintext,
        "krabka-logs".to_string(),
        "wal-secret".to_string(),
    ));

    for (config, expected) in [(&secured, expected), (&plain, None)] {
        let security = config.wal_client_security.load().expect("the flags load");
        let options = admin_connection_options(policy, security.as_ref());
        let dependencies = with_querier_dependencies(
            ServiceDependencies::default(),
            config,
            "group".to_string(),
            policy,
            security.as_ref(),
            crate::wal_consumer_metrics::WalConsumerMetrics::unregistered(),
        )
        .expect("the querier dependencies build");
        let authorizer = dependencies
            .deferred_query_authorizer_connect
            .as_ref()
            .expect("a deferred authorizer connect");
        let consumer = dependencies
            .deferred_wal_consumer_connect
            .as_ref()
            .expect("a deferred consumer connect");

        check!(
            [
                plain_credentials_for_test(security.as_ref()),
                plain_credentials_for_test(options.security.as_deref()),
                plain_credentials_for_test(authorizer.security.as_ref()),
                plain_credentials_for_test(consumer.security.as_ref()),
            ] == [
                expected.clone(),
                expected.clone(),
                expected.clone(),
                expected.clone()
            ],
            "{:?}",
            config.wal_client_security
        );
    }
}
