//! Docker-backed Grafana coverage for the Loki-compatible query API.
//!
//! `krabka-metrics-service` reads its Prometheus surface through a real
//! Grafana, and `krabka-traces` reads its Tempo surface the same way. The logs
//! surface had no such suite. That matters, because a Grafana Loki datasource
//! reaches Krabka over two paths that no other suite here drives: the
//! datasource proxy at `/api/datasources/proxy/uid/<uid>/...`, which the
//! Explore log browser uses, and the backend query path at `/api/ds/query`,
//! which every dashboard panel uses. The second path is not a proxy: Grafana's
//! own Loki datasource plugin builds the request, reads the answer, and turns
//! it into data frames, so a field Krabka names differently is dropped there
//! rather than reported.
//!
//! The querier runs in this process and answers out of its hot WAL tail, the
//! same wiring `loki_differential` uses. Grafana runs in a container and dials
//! back to the host through `host.docker.internal`.
//!
//! Cargo ignores this test by default, because it runs
//! `mirror.gcr.io/grafana/grafana` under Docker. Run with:
//!
//! `cargo test -p krabka-observability --test grafana_integration -- --ignored --nocapture`

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use assert2::{assert, check};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{LabelIndex, LogBlockIndex as BlockIndex};
use krabka_observability::{InMemoryWalSink, QuerierState, distributor_router, loki_router};
use serde_json::{Value, json};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::{net::TcpListener, sync::oneshot};
use tower::ServiceExt as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// Grafana's default HTTP port.
const GRAFANA_PORT: u16 = 3000;

/// The tenant the provisioned datasource sends on every request.
const TENANT: &str = "tenant-a";

/// The datasource UID the proxy and `/api/ds/query` calls name.
const DATASOURCE_UID: &str = "krabka-loki";

/// The provisioned datasource, with `{PORT}` replaced at run time.
///
/// `httpHeaderName1` and `httpHeaderValue1` make Grafana send `X-Scope-OrgID`
/// on every call it makes to Krabka, on the proxy path and on the backend
/// path both. Krabka keys its storage by that header, so without it the
/// querier answers for a different tenant than the push wrote to.
const DATASOURCE_YAML_TEMPLATE: &str = r"apiVersion: 1
datasources:
  - name: Krabka
    type: loki
    access: proxy
    uid: krabka-loki
    url: http://host.docker.internal:{PORT}
    isDefault: true
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: tenant-a
";

/// One metadata read through the datasource proxy, and the answer it must give.
struct MetadataCase {
    /// The name the failure report prints.
    name: &'static str,
    /// The path below `/loki/api/v1/`.
    path: &'static str,
    /// Query parameters beyond the window, which every case gets.
    params: &'static [(&'static str, &'static str)],
    /// The whole `data` member of the answer.
    expected: Value,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana image"]
async fn a_grafana_loki_datasource_reads_the_querier_over_the_proxy_and_the_backend_path()
-> TestResult {
    let krabka = start_krabka().await?;
    let client = reqwest::Client::new();

    let datasource_yaml = DATASOURCE_YAML_TEMPLATE.replace("{PORT}", &krabka.host_port.to_string());
    let grafana = start_grafana(&datasource_yaml).await?;
    let base = mapped_base_url(&grafana, GRAFANA_PORT).await?;
    wait_for_http_ok(&client, &base, "/api/health").await?;
    wait_for_datasource(&client, &base, DATASOURCE_UID).await?;

    // Grafana's own verdict on the datasource. It runs the same probe the
    // "Save & test" button runs, so a Krabka answer the plugin cannot read
    // fails here rather than in a panel.
    let health = client
        .get(format!(
            "{base}/api/datasources/uid/{DATASOURCE_UID}/health"
        ))
        .send()
        .await?;
    assert!(health.status().is_success());

    for case in metadata_cases() {
        let answer = proxy_get(&client, &base, case.path, case.params, &krabka).await?;
        check!(answer["data"] == case.expected, "{}", case.name);
    }

    let proxied = proxy_get(
        &client,
        &base,
        "query_range",
        &[
            ("query", r#"{app="api",env="prod"} |= "error""#),
            ("direction", "forward"),
        ],
        &krabka,
    )
    .await?;
    assert!(proxied["data"]["result"] == expected_error_stream(krabka.base_ns));

    // The backend path. Grafana parses the answer into frames and hands back
    // its own shape, so the check is that the line survived the round trip
    // rather than that the shape matches Loki's.
    let frames = backend_query(
        &client,
        &base,
        r#"{app="api",env="prod"} |= "error""#,
        &krabka,
    )
    .await?;
    assert!(json_holds(&frames, "api grafana datasource error"));

    krabka.shutdown();
    Ok(())
}

/// The label, label-values and series reads Grafana's log browser makes.
fn metadata_cases() -> Vec<MetadataCase> {
    vec![
        MetadataCase {
            name: "labels",
            path: "labels",
            params: &[],
            // `service_name` is not pushed. The distributor derives it from
            // the stream's `app` label, the way Loki's own discovery does.
            expected: json!(["app", "env", "service_name"]),
        },
        MetadataCase {
            name: "label_values_app",
            path: "label/app/values",
            params: &[],
            expected: json!(["api"]),
        },
        MetadataCase {
            name: "label_values_env",
            path: "label/env/values",
            params: &[],
            expected: json!(["prod"]),
        },
        MetadataCase {
            name: "series",
            path: "series",
            params: &[("match[]", r#"{app="api"}"#)],
            // One label set, not two. `detected_level` reaches the query
            // answer as structured metadata, and the metadata endpoints strip
            // structured metadata out, so it is not a series of its own here.
            expected: json!([
                {
                    "app": "api",
                    "env": "prod",
                    "service_name": "api",
                },
            ]),
        },
    ]
}

/// The one stream the `|= "error"` query selects.
///
/// `detected_level` is on it and on no other entry, so the pushed stream comes
/// back as two label sets and this query picks the one the filter matched.
fn expected_error_stream(base_ns: i64) -> Value {
    json!([
        {
            "stream": {
                "app": "api",
                "detected_level": "error",
                "env": "prod",
                "service_name": "api",
            },
            "values": [[
                (base_ns + 1_000_000_000).to_string(),
                "api grafana datasource error",
            ]],
        }
    ])
}

// ---------------------------------------------------------------------------
// Grafana.
// ---------------------------------------------------------------------------

/// Reads a Loki path through Grafana's datasource proxy.
async fn proxy_get(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    params: &[(&str, &str)],
    krabka: &KrabkaServer,
) -> TestResult<Value> {
    let mut pairs: Vec<(&str, String)> = vec![
        ("start", krabka.base_ns.to_string()),
        ("end", krabka.end_ns().to_string()),
    ];
    for (name, value) in params {
        pairs.push((*name, (*value).to_string()));
    }
    let url = format!(
        "{base}/api/datasources/proxy/uid/{DATASOURCE_UID}/loki/api/v1/{path}?{}",
        query_string(&pairs)
    );
    let response = client.get(url).send().await?.error_for_status()?;
    Ok(response.json().await?)
}

/// Encodes one query string from its pairs.
///
/// `reqwest` is built here without its `query` feature, which is what
/// `RequestBuilder::query` needs, so the pairs are encoded the way
/// `krabka-metrics-service`'s Grafana suite encodes its own.
fn query_string(pairs: &[(&str, String)]) -> String {
    pairs
        .iter()
        .map(|(name, value)| format!("{}={}", form_encode(name), form_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn form_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// Runs one query through the backend datasource path a dashboard panel uses.
async fn backend_query(
    client: &reqwest::Client,
    base: &str,
    expr: &str,
    krabka: &KrabkaServer,
) -> TestResult<Value> {
    let body = json!({
        "from": (krabka.base_ns / 1_000_000).to_string(),
        "to": (krabka.end_ns() / 1_000_000).to_string(),
        "queries": [{
            "refId": "A",
            "datasource": { "type": "loki", "uid": DATASOURCE_UID },
            "expr": expr,
            "queryType": "range",
            "direction": "forward",
            "maxLines": 1000,
            "intervalMs": 1000,
            "maxDataPoints": 1000,
        }],
    });
    let response = client
        .post(format!("{base}/api/ds/query"))
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    // Grafana answers a datasource error with the datasource's own status and
    // a body that names it. `error_for_status` would drop that body and leave
    // only the number, which does not say which of the two sides refused.
    if !status.is_success() {
        return Err(format!("Grafana answered {status} for /api/ds/query: {text}").into());
    }
    Ok(serde_json::from_str(&text)?)
}

async fn start_grafana(datasource_yaml: &str) -> TestResult<ContainerAsync<GenericImage>> {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagreed
    // testcontainers pulled the image over the network and the suite ran
    // against whatever it got rather than against the pinned bytes.
    let tag = std::env::var("KRABKA_GRAFANA_IMAGE_TAG").expect(
        "KRABKA_GRAFANA_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under cargo, set it to \
         that image's tag in //bazel/images/images.bzl.",
    );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/grafana".to_string(), tag)
            .with_exposed_port(GRAFANA_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("HTTP Server Listen"))
            .with_copy_to(
                "/etc/grafana/provisioning/datasources/krabka.yaml",
                datasource_yaml.as_bytes().to_vec(),
            )
            .with_host("host.docker.internal", Host::HostGateway)
            .with_env_var("GF_AUTH_ANONYMOUS_ENABLED", "true")
            .with_env_var("GF_AUTH_ANONYMOUS_ORG_ROLE", "Admin")
            .with_env_var("GF_AUTH_BASIC_ENABLED", "false")
            .start(),
    )
    .await??)
}

async fn mapped_base_url(
    container: &ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port.tcp()).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, path: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_mins(1);
    while Instant::now() < deadline {
        if client
            .get(format!("{base}{path}"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("{base}{path} did not become ready").into())
}

async fn wait_for_datasource(client: &reqwest::Client, base: &str, uid: &str) -> TestResult {
    let deadline = Instant::now() + Duration::from_mins(1);
    while Instant::now() < deadline {
        if client
            .get(format!("{base}/api/datasources/uid/{uid}"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("datasource {uid} was not provisioned on {base}").into())
}

// ---------------------------------------------------------------------------
// Krabka.
// ---------------------------------------------------------------------------

struct KrabkaServer {
    /// The host port the container dials through `host.docker.internal`.
    host_port: u16,
    /// The timestamp of the first seeded entry.
    base_ns: i64,
    shutdown: oneshot::Sender<()>,
}

impl KrabkaServer {
    /// The end of the query window, which is past the last seeded entry.
    fn end_ns(&self) -> i64 {
        self.base_ns + 2_000_000_000
    }

    fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

/// Seeds the querier through the real push door and serves it on the host.
///
/// The bind address is `0.0.0.0`, not `127.0.0.1`: the container reaches this
/// process over the host gateway, and a loopback-only listener refuses that
/// connection.
async fn start_krabka() -> TestResult<KrabkaServer> {
    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let sink = InMemoryWalSink::default();

    let response = distributor_router(sink.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("content-type", "application/json")
                .header("X-Scope-OrgID", TENANT)
                .body(Body::from(push_body(base_ns).to_string()))?,
        )
        .await?;
    assert!(response.status() == StatusCode::NO_CONTENT);

    // `i64::MIN`: nothing has been compacted, so every record the distributor
    // wrote is in the querier's hot tail.
    let root = tempfile::tempdir()?.keep();
    let state = QuerierState::new(root, LabelIndex::default(), BlockIndex::default())
        .with_hot_tail(sink, i64::MIN);

    let listener = TcpListener::bind(("0.0.0.0", 0)).await?;
    let host_port = listener.local_addr()?.port();
    let (shutdown, stop) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, loki_router(state))
            .with_graceful_shutdown(async move {
                let _ = stop.await;
            })
            .await;
    });

    Ok(KrabkaServer {
        host_port,
        base_ns,
        shutdown,
    })
}

/// Two entries one second apart, one of which says "error".
///
/// The distributor's level discovery puts `detected_level` on that one and on
/// no other, which is what splits the pushed stream into the two label sets
/// the series case expects.
fn push_body(base_ns: i64) -> Value {
    json!({
        "streams": [{
            "stream": { "app": "api", "env": "prod" },
            "values": [
                [base_ns.to_string(), "api grafana datasource ok"],
                [
                    (base_ns + 1_000_000_000).to_string(),
                    "api grafana datasource error",
                ],
            ],
        }],
    })
}

fn current_unix_second_ns() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock is after the unix epoch")
        .as_secs();
    i64::try_from(now).expect("unix seconds fit in i64") * 1_000_000_000
}

/// Whether any string anywhere in `value` holds `needle`.
///
/// Grafana's backend answer is a frame envelope whose depth and field names
/// belong to the plugin, not to Loki, so the line is looked for rather than
/// addressed.
fn json_holds(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text.contains(needle),
        Value::Array(items) => items.iter().any(|item| json_holds(item, needle)),
        Value::Object(members) => members.values().any(|member| json_holds(member, needle)),
        _ => false,
    }
}
