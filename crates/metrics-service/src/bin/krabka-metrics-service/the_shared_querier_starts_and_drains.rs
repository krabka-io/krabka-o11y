use assert2::assert;
use clap::Parser;

use super::*;

#[tokio::test]
async fn the_shared_querier_serves_before_its_stage_drains_without_a_process_signal() {
    let directory = tempfile::tempdir().unwrap();
    let url = url::Url::from_directory_path(directory.path()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let cli = Cli::try_parse_from([
        "krabka-metrics-service",
        "--target",
        "querier",
        "--object-store-url",
        url.as_str(),
        "--listen",
        &address.to_string(),
    ])
    .unwrap();
    let security = cli.server_security.load().unwrap();
    let readiness = RoleReadiness::new();
    let role_readiness = readiness.clone();
    let mut drain = krabka_observability::StagedDrain::new(secs(30));
    drain.stage("querier", move |token| {
        let role = run_querier(
            cli,
            krabka_promql::metrics::ServiceMetrics::new(),
            role_readiness,
            security,
            None,
            AuditHandle::disabled(),
            Shutdown::from(token),
        );
        async move {
            role.await.expect("the real querier drains");
        }
    });
    assert!(readiness.pending() == vec!["startup".to_string()]);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !readiness.is_ready() {
            tokio::task::yield_now().await;
        }
        let response = reqwest::Client::new()
            .get(format!(
                "http://{address}/api/v1/query?query=vector(1)&time=123"
            ))
            .header("X-Scope-OrgID", "tenant")
            .send()
            .await
            .unwrap();
        assert!(response.status() == reqwest::StatusCode::OK);
        let response: serde_json::Value = response.json().await.unwrap();
        assert!(
            response["data"]
                == serde_json::json!({
                    "resultType": "vector",
                    "result": [{"metric": {}, "value": [123, "1"]}]
                })
        );
        assert!(drain.drain().await.is_empty());
    })
    .await
    .expect("the querier starts, serves, and drains");
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}
