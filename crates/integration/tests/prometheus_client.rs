use std::{collections::BTreeSet, sync::Arc, time::Duration};

use axum::{Router, extract::State, http::Uri};
use testcontainers::{
    GenericImage, ImageExt,
    core::{Host, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::Mutex;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CONFIG: &str = r"
global:
  scrape_interval: 2s
remote_write:
  - url: http://host.docker.internal:ORACLE_PORT/prom-v1
  - url: http://host.docker.internal:ORACLE_PORT/prom-v2
    protobuf_message: io.prometheus.write.v2.Request
scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: [127.0.0.1:9090]
";

#[tokio::test]
#[ignore = "requires Docker and the digest-pinned Prometheus image"]
async fn official_prometheus_sends_remote_write_v1_and_v2() -> TestResult {
    let seen = Arc::new(Mutex::new(BTreeSet::new()));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let port = listener.local_addr()?.port();
    let app = Router::new()
        .fallback(capture)
        .with_state(Arc::clone(&seen));
    tokio::spawn(async move { axum::serve(listener, app).await });

    let tag = std::env::var("KRABKA_PROMETHEUS_IMAGE_TAG")
        .expect("KRABKA_PROMETHEUS_IMAGE_TAG is set by `bazel test --config=docker`");
    let config = CONFIG.replace("ORACLE_PORT", &port.to_string());
    let _prometheus = GenericImage::new("mirror.gcr.io/prom/prometheus".to_string(), tag)
        .with_wait_for(WaitFor::message_on_stderr(
            "Server is ready to receive web requests.",
        ))
        .with_copy_to("/etc/prometheus/oracle.yml", config.into_bytes())
        .with_host("host.docker.internal", Host::HostGateway)
        .with_cmd(["--config.file=/etc/prometheus/oracle.yml"])
        .start()
        .await?;

    let expected = BTreeSet::from(["/prom-v1".to_string(), "/prom-v2".to_string()]);
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if *seen.lock().await == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    assert2::assert!(*seen.lock().await == expected);
    Ok(())
}

async fn capture(State(seen): State<Arc<Mutex<BTreeSet<String>>>>, uri: Uri) {
    seen.lock().await.insert(uri.path().to_string());
}
