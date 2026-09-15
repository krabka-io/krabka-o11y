use std::{collections::BTreeSet, sync::Arc, time::Duration};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, Uri, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use testcontainers::{
    GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::Mutex;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const TENANT: &str = "client-oracle";
const CONFIG: &str = r#"
prometheus.remote_write "v1" {
  endpoint {
    url = sys.env("ORACLE_URL") + "/api/v1/push"
    headers = { "X-Scope-OrgID" = "client-oracle" }
  }
}

prometheus.remote_write "v2" {
  endpoint {
    url = sys.env("ORACLE_URL") + "/api/v1/push"
    headers = { "X-Scope-OrgID" = "client-oracle" }
    protobuf_message = "io.prometheus.write.v2.Request"
  }
}

prometheus.scrape "self" {
  targets = [{ "__address__" = "127.0.0.1:12345" }]
  scrape_interval = "2s"
  scrape_timeout = "1s"
  forward_to = [prometheus.remote_write.v1.receiver, prometheus.remote_write.v2.receiver]
}

loki.write "default" {
  endpoint {
    url = sys.env("ORACLE_URL") + "/loki/api/v1/push"
    headers = { "X-Scope-OrgID" = "client-oracle" }
  }
}

loki.source.api "default" {
  http {
    listen_address = "0.0.0.0"
    listen_port = 9999
  }
  forward_to = [loki.write.default.receiver]
}

otelcol.exporter.otlphttp "default" {
  client {
    endpoint = sys.env("ORACLE_URL")
    headers = { "X-Scope-OrgID" = "client-oracle" }
  }
}

otelcol.receiver.otlp "default" {
  http { endpoint = "0.0.0.0:4318" }
  output { traces = [otelcol.exporter.otlphttp.default.input] }
}

pyroscope.write "default" {
  endpoint {
    url = sys.env("ORACLE_URL")
    headers = { "X-Scope-OrgID" = "client-oracle" }
  }
}

pyroscope.scrape "self" {
  targets = [{ "__address__" = "127.0.0.1:12345", "service_name" = "alloy" }]
  scrape_interval = "5s"
  scrape_timeout = "6s"
  delta_profiling_duration = "3s"
  forward_to = [pyroscope.write.default.receiver]
}
"#;

#[tokio::test]
#[ignore = "requires Docker and the digest-pinned Alloy image"]
async fn alloy_sends_every_public_protocol_without_a_krabka_adapter() -> TestResult {
    let seen = Arc::new(Mutex::new(BTreeSet::new()));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let oracle_url = format!(
        "http://host.docker.internal:{}",
        listener.local_addr()?.port()
    );
    let app = Router::new()
        .fallback(capture)
        .with_state(Arc::clone(&seen));
    tokio::spawn(async move { axum::serve(listener, app).await });

    let tag = std::env::var("KRABKA_ALLOY_IMAGE_TAG")
        .expect("KRABKA_ALLOY_IMAGE_TAG is set by `bazel test --config=docker`");
    let alloy = GenericImage::new("mirror.gcr.io/grafana/alloy".to_string(), tag)
        .with_exposed_port(9999.tcp())
        .with_exposed_port(4318.tcp())
        .with_wait_for(WaitFor::message_on_stderr("now listening for http traffic"))
        .with_copy_to("/etc/alloy/client-oracle.alloy", CONFIG.as_bytes().to_vec())
        .with_host("host.docker.internal", Host::HostGateway)
        .with_env_var("ORACLE_URL", oracle_url)
        .with_cmd([
            "run",
            "/etc/alloy/client-oracle.alloy",
            "--server.http.listen-addr=0.0.0.0:12345",
            "--stability.level=experimental",
            "--storage.path=/tmp/alloy",
        ])
        .start()
        .await?;

    let client = reqwest::Client::new();
    let logs_port = alloy.get_host_port_ipv4(9999.tcp()).await?;
    client
        .post(format!("http://127.0.0.1:{logs_port}/loki/api/v1/push"))
        .json(&serde_json::json!({
            "streams": [{"stream": {"job": "alloy-oracle"}, "values": [["1", "hello"]]}]
        }))
        .send()
        .await?
        .error_for_status()?;

    let traces_port = alloy.get_host_port_ipv4(4318.tcp()).await?;
    client
        .post(format!("http://127.0.0.1:{traces_port}/v1/traces"))
        .header(CONTENT_TYPE, "application/json")
        .json(&serde_json::json!({
            "resource_spans": [{"scope_spans": [{"spans": [{
                "trace_id": "5B8EFFF798038103D269B633813FC60C",
                "span_id": "EEE19B7EC3C1B174",
                "name": "alloy-oracle",
                "start_time_unix_nano": 1_544_712_660_300_000_000_i64,
                "end_time_unix_nano": 1_544_712_660_600_000_000_i64
            }]}]}]
        }))
        .send()
        .await?
        .error_for_status()?;

    let expected = BTreeSet::from(["logs", "metrics-v1", "metrics-v2", "profiles", "traces"]);
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

async fn capture(
    State(seen): State<Arc<Mutex<BTreeSet<&'static str>>>>,
    headers: HeaderMap,
    uri: Uri,
    _body: Bytes,
) -> Response {
    if headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        != Some(TENANT)
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let signal = match uri.path() {
        "/api/v1/push" if content_type.contains("io.prometheus.write.v2.Request") => "metrics-v2",
        "/api/v1/push" => "metrics-v1",
        "/loki/api/v1/push" => "logs",
        "/v1/traces" => "traces",
        "/push.v1.PusherService/Push" => "profiles",
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    seen.lock().await.insert(signal);
    if signal == "profiles" {
        ([(CONTENT_TYPE, content_type)], Bytes::new()).into_response()
    } else {
        StatusCode::NO_CONTENT.into_response()
    }
}
