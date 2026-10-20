use std::{collections::BTreeSet, sync::Arc};

use axum::{Router, extract::State, http::Uri};
use testcontainers::core::Host;
use tokio::sync::Mutex;

#[path = "support/client_harness.rs"]
mod client_harness;

use client_harness::{
    AsyncRunner as _, GenericImage, ImageExt, WaitFor, capture_server, expect_seen,
    spawn_capture_server,
};

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
    let (seen, port) = capture_server!(
        Arc::new(Mutex::new(BTreeSet::new())),
        Router::new(),
        capture,
        spawn_capture_server
    );

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
    expect_seen(&seen, &expected).await?;
    Ok(())
}

async fn capture(State(seen): State<Arc<Mutex<BTreeSet<String>>>>, uri: Uri) {
    seen.lock().await.insert(uri.path().to_string());
}
