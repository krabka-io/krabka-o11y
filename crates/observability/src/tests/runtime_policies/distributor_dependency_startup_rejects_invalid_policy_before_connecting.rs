use super::*;

#[tokio::test]
pub(crate) async fn distributor_dependency_startup_rejects_invalid_policy_before_connecting() {
    let config = ServiceConfig::parse_from([
        "krabka-observability",
        "--target",
        "distributor",
        "--wal-bootstrap-server=127.0.0.1:1",
        "--wal-connect-startup-deadline=1s",
        "--wal-connect-attempt-timeout=2s",
    ]);

    let Err(error) = build_service_dependencies(&config).await else {
        panic!("invalid policy must fail before broker connection");
    };
    check!(
        error
            .to_string()
            .contains("must not exceed startup deadline")
    );
}
