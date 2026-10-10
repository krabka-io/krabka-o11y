//! Loki integration scenarios against real Krabka roles, broker WAL, and `MinIO`.
//! Fixed inputs supply the oracle; a querier tailing an empty WAL proves
//! that stored blocks supply the answer. See `loki_deployment.md` for upstream mapping.

use std::{
    os::unix::fs::MetadataExt as _,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use assert2::assert;
use prost::Message as _;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt as _,
    core::{ContainerPort, Mount},
};

#[path = "../../metrics-service/tests/support/container_deployment.rs"]
mod container_deployment;
mod support;
use container_deployment::{
    TestResult, base_url, deployment_network, image, start, start_broker, start_minio,
    wait_until_ready,
};
use support::{LokiProtoEntry, LokiProtoPushRequest, LokiProtoStream, LokiProtoTimestamp};

const PORT: u16 = 3100;
const START: i64 = 1_700_000_000_000_000_000;
const END: i64 = START + 3_000_000_000;
const TIMEOUT: Duration = Duration::from_secs(45);
const SELECTOR: &str = r#"{service_name="fixture"}"#;

// Drop containers before the directories holding their bind mounts.
struct Deployment {
    distributor: ContainerAsync<GenericImage>,
    hot: ContainerAsync<GenericImage>,
    _minio: ContainerAsync<GenericImage>,
    _broker: ContainerAsync<GenericImage>,
    data: tempfile::TempDir,
    _broker_data: tempfile::TempDir,
    network: String,
    client: Client,
}

impl Deployment {
    async fn start() -> TestResult<Self> {
        Self::with_overrides(None).await
    }

    async fn with_overrides(overrides: Option<&str>) -> TestResult<Self> {
        let broker_data = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        if let Some(overrides) = overrides {
            std::fs::write(data.path().join("overrides.yaml"), overrides)?;
        }
        let network = deployment_network("logs", broker_data.path())?;
        let broker = start_broker(broker_data.path(), &network).await?;
        let minio = start_minio(&network, "logs").await?;
        let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
        let distributor = role(&network, data.path(), "distributor", true).await?;
        let hot = role(&network, data.path(), "querier", true).await?;
        let deployment = Self {
            distributor,
            hot,
            _minio: minio,
            _broker: broker,
            data,
            _broker_data: broker_data,
            network,
            client,
        };
        deployment.ready(&deployment.distributor).await?;
        deployment.ready(&deployment.hot).await?;
        Ok(deployment)
    }

    async fn ready(&self, container: &ContainerAsync<GenericImage>) -> TestResult {
        wait_until_ready(&self.client, container, PORT).await
    }

    async fn builder(&self) -> TestResult<ContainerAsync<GenericImage>> {
        let builder = role(&self.network, self.data.path(), "block-builder", true).await?;
        self.ready(&builder).await?;
        Ok(builder)
    }

    async fn cold(&self) -> TestResult<ContainerAsync<GenericImage>> {
        let cold = role(&self.network, self.data.path(), "querier", false).await?;
        self.ready(&cold).await?;
        Ok(cold)
    }

    async fn push(&self, tenant: &str, streams: Value) -> TestResult {
        accepted(
            self.client
                .post(format!(
                    "{}/loki/api/v1/push",
                    base_url(&self.distributor, PORT).await?
                ))
                .header("X-Scope-OrgID", tenant)
                .json(&json!({"streams":streams}))
                .send()
                .await?,
        )
        .await
    }

    async fn get(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        path: &str,
        query: &str,
        categorize: bool,
    ) -> TestResult<Value> {
        let response = self
            .request(container, tenant, path, query)
            .await?
            .header(
                "X-Loki-Response-Encoding-Flags",
                if categorize { "categorize-labels" } else { "" },
            )
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        assert!(status == StatusCode::OK, "{path}: {status}: {text}");
        Ok(serde_json::from_str(&text)?)
    }

    async fn request(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        path: &str,
        query: &str,
    ) -> TestResult<reqwest::RequestBuilder> {
        let mut url = url::Url::parse(&format!("{}{path}", base_url(container, PORT).await?))?;
        if path != "/loki/api/v1/delete" {
            url.query_pairs_mut().extend_pairs([
                ("query", query),
                ("start", &START.to_string()),
                ("end", &END.to_string()),
            ]);
        }
        if matches!(path, "/loki/api/v1/query_range" | "/loki/api/v1/tail") {
            url.query_pairs_mut()
                .extend_pairs([("direction", "forward"), ("limit", "1000")]);
        }
        Ok(self.client.get(url).header("X-Scope-OrgID", tenant))
    }

    async fn wait(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        query: &str,
        categorize: bool,
        expected: &Value,
    ) -> TestResult {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let result = self
                    .get(
                        container,
                        tenant,
                        "/loki/api/v1/query_range",
                        query,
                        categorize,
                    )
                    .await?;
                if result["data"]["result"] == *expected {
                    return Ok(());
                }
                eprintln!("waiting for {tenant} {query}: {}", result["data"]["result"]);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|error| format!("query {tenant} {query}, expected {expected}: {error}"))?
    }

    async fn stored_and_restarted(
        &self,
        tenant: &str,
        query: &str,
        categorize: bool,
        expected: &Value,
    ) -> TestResult {
        let builder = self.builder().await?;
        let cold = self.cold().await?;
        self.wait(&cold, tenant, query, categorize, expected)
            .await?;
        // Stop every writer. The next querier has neither a hot head nor a
        // process-local index/cache from the first query.
        builder.stop().await?;
        cold.stop().await?;
        let restarted = self.cold().await?;
        self.wait(&restarted, tenant, query, categorize, expected)
            .await
    }
}

async fn role(
    network: &str,
    directory: &std::path::Path,
    target: &str,
    wal: bool,
) -> TestResult<ContainerAsync<GenericImage>> {
    let mut command = vec![
        "krabka-observability".to_string(),
        format!("--target={target}"),
        format!("--listen-addr=0.0.0.0:{PORT}"),
        "--object-store-url=s3://logs".into(),
        "--data-root=/data".into(),
        "--index-prefix=logs".into(),
        "--querier-index-source=tenant-object-store-shards".into(),
        "--reject-old-samples-max-age=1000000h".into(),
        "--compactor-wal-poll-timeout=100ms".into(),
        format!(
            "--compactor-accumulation-window={}",
            if target == "all" { "3s" } else { "100ms" }
        ),
        format!(
            "--compactor-accumulation-poll-timeout={}",
            if target == "all" { "3s" } else { "50ms" }
        ),
        "--querier-dynamic-index-cache-ttl=100ms".into(),
        "--querier-shard-index-cache-ttl=100ms".into(),
        "--querier-frontier-refresh-interval=100ms".into(),
        "--querier-query-frontend-split-interval=1s".into(),
        "--querier-query-frontend-cache-ttl=100ms".into(),
    ];
    command.push(format!("--wal-bootstrap-server={network}-broker:9092"));
    if !wal {
        // The production querier requires broker authorization. Tail a
        // provisioned, empty WAL so only persisted logs can answer.
        command.push("--wal-topic=__krabka_traces_wal".into());
    }
    if target == "querier" {
        static NEXT_QUERIER: AtomicUsize = AtomicUsize::new(0);
        // Every querier replica must see the whole WAL, and a fresh replica
        // must not wait for a stopped member's consumer-group session to expire.
        command.push(format!(
            "--wal-group-id=deployment-querier-{}",
            NEXT_QUERIER.fetch_add(1, Ordering::Relaxed)
        ));
    }

    if directory.join("overrides.yaml").exists() {
        command.push("--logs-limits-overrides-config=/data/overrides.yaml".into());
    }
    let metadata = directory.metadata()?;
    let request: ContainerRequest<GenericImage> = image("KRABKA")
        .with_exposed_port(ContainerPort::Tcp(PORT))
        .with_network(network)
        .with_user(format!("{}:{}", metadata.uid(), metadata.gid()))
        .with_mount(Mount::bind_mount(directory.to_string_lossy(), "/data"))
        .with_env_var("AWS_ACCESS_KEY_ID", "krabkalogs")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "krabkalogs")
        .with_env_var("AWS_ENDPOINT_URL", format!("http://{network}-minio:9000"))
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("AWS_ALLOW_HTTP", "true")
        .with_env_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")
        .with_env_var("AWS_EC2_METADATA_DISABLED", "true")
        .with_cmd(command);
    start(request).await
}

async fn accepted(response: reqwest::Response) -> TestResult {
    let status = response.status();
    let text = response.text().await?;
    assert!(status.is_success(), "push: {status}: {text}");
    Ok(())
}

fn streams(values: Value) -> Value {
    Value::Array(vec![Value::Object(
        [
            ("stream".into(), json!({"service_name":"fixture"})),
            ("values".into(), values),
        ]
        .into_iter()
        .collect(),
    )])
}
fn expected(values: Value) -> Value {
    let categorized = values[0].as_array().is_some_and(|entry| entry.len() == 3);
    let mut result = streams(values);
    if !categorized {
        result[0]["stream"]["detected_level"] = json!("unknown");
    }
    result
}

#[derive(Clone, Copy)]
enum Encoding {
    Json,
    Gzip,
    Snappy,
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn json_push_survives_storage_and_restart() -> TestResult {
    roundtrip(Encoding::Json).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn gzip_push_survives_storage_and_restart() -> TestResult {
    roundtrip(Encoding::Gzip).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn snappy_protobuf_push_survives_storage_and_restart() -> TestResult {
    roundtrip(Encoding::Snappy).await
}

async fn roundtrip(encoding: Encoding) -> TestResult {
    let deployment = Deployment::start().await?;
    let values = json!([
        [START.to_string(), "first"],
        [(START + 1_000_000_000).to_string(), "second"]
    ]);
    let json_body = serde_json::to_vec(&json!({"streams":streams(values.clone())}))?;
    let (content_type, content_encoding, body) = match encoding {
        Encoding::Json => ("application/json", "identity", json_body),
        Encoding::Gzip => {
            use std::io::Write as _;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&json_body)?;
            ("application/json", "gzip", encoder.finish()?)
        }
        Encoding::Snappy => {
            let request = LokiProtoPushRequest {
                streams: vec![LokiProtoStream {
                    labels: r#"{service_name="fixture"}"#.into(),
                    hash: 0,
                    entries: ["first", "second"]
                        .into_iter()
                        .enumerate()
                        .map(|(index, line)| LokiProtoEntry {
                            timestamp: Some(LokiProtoTimestamp {
                                seconds: START / 1_000_000_000
                                    + i64::try_from(index).expect("two entries"),
                                nanos: 0,
                            }),
                            line: line.into(),
                            structured_metadata: vec![],
                            parsed: vec![],
                        })
                        .collect(),
                }],
            };
            (
                "application/x-protobuf",
                "snappy",
                snap::raw::Encoder::new().compress_vec(&request.encode_to_vec())?,
            )
        }
    };
    let mut request = deployment
        .client
        .post(format!(
            "{}/loki/api/v1/push",
            base_url(&deployment.distributor, PORT).await?
        ))
        .header("X-Scope-OrgID", "tenant-a")
        .header("Content-Type", content_type)
        .body(body);
    if content_encoding != "identity" {
        request = request.header("Content-Encoding", content_encoding);
    }
    accepted(request.send().await?).await?;
    let expected = expected(values);
    deployment
        .wait(&deployment.hot, "tenant-a", SELECTOR, false, &expected)
        .await?;
    // No builder has started: the cold role must not have an answer yet.
    let empty = deployment.cold().await?;
    assert!(
        deployment
            .get(
                &empty,
                "tenant-a",
                "/loki/api/v1/query_range",
                SELECTOR,
                false
            )
            .await?["data"]["result"]
            == json!([])
    );
    empty.stop().await?;
    deployment
        .stored_and_restarted("tenant-a", SELECTOR, false, &expected)
        .await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn categorized_metadata_and_parser_labels_survive_storage() -> TestResult {
    let deployment = Deployment::start().await?;
    deployment.push("tenant-a",streams(json!([[START.to_string(),"color=red",{"traceID":"123","user":"alice"}],[(START+1_000_000_000).to_string(),"color=blue",{"traceID":"456","user":"bob"}]]))).await?;
    let query = format!("{SELECTOR} | logfmt");
    let expected = expected(
        json!([[START.to_string(),"color=red",{"structuredMetadata":{"detected_level":"unknown","traceID":"123","user":"alice"},"parsed":{"color":"red"}}],[(START+1_000_000_000).to_string(),"color=blue",{"structuredMetadata":{"detected_level":"unknown","traceID":"456","user":"bob"},"parsed":{"color":"blue"}}]]),
    );
    deployment
        .wait(&deployment.hot, "tenant-a", &query, true, &expected)
        .await?;
    deployment
        .stored_and_restarted("tenant-a", &query, true, &expected)
        .await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn duplicate_identity_includes_timestamp_stream_and_metadata() -> TestResult {
    let deployment = Deployment::start().await?;
    let first = streams(
        json!([[START.to_string(),"same",{"user":"alice"}],[(START+1).to_string(),"same",{"user":"alice"}],[START.to_string(),"same",{"user":"bob"}]]),
    );
    deployment.push("tenant-a", first.clone()).await?;
    let builder = deployment.builder().await?;
    let cold = deployment.cold().await?;
    let expected_first = expected(
        json!([[START.to_string(),"same",{"structuredMetadata":{"detected_level":"unknown","user":"alice"}}],[START.to_string(),"same",{"structuredMetadata":{"detected_level":"unknown","user":"bob"}}],[(START+1).to_string(),"same",{"structuredMetadata":{"detected_level":"unknown","user":"alice"}}]]),
    );
    deployment
        .wait(&cold, "tenant-a", SELECTOR, true, &expected_first)
        .await?;
    // First copy is demonstrably persisted before the second WAL append.
    deployment.push("tenant-a", first).await?;
    deployment.push("tenant-a",json!([{"stream":{"service_name":"fixture","replica":"other"},"values":[[START.to_string(),"same",{"user":"alice"}]]}])).await?;
    let expected = json!([
        {"stream":{"replica":"other","service_name":"fixture"},"values":[[START.to_string(),"same",{"structuredMetadata":{"detected_level":"unknown","user":"alice"}}]]},
        {"stream":{"service_name":"fixture"},"values":expected_first[0]["values"]}
    ]);
    deployment
        .wait(&deployment.hot, "tenant-a", SELECTOR, true, &expected)
        .await?;
    deployment
        .wait(&cold, "tenant-a", SELECTOR, true, &expected)
        .await?;
    builder.stop().await?;
    cold.stop().await?;
    let restarted = deployment.cold().await?;
    deployment
        .wait(&restarted, "tenant-a", SELECTOR, true, &expected)
        .await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn multi_tenant_queries_preserve_isolation_across_restart() -> TestResult {
    let deployment = Deployment::start().await?;
    deployment
        .push("org1", json!([{"stream":{"service_name":"fixture","job":"fake1"},"values":[[START.to_string(),"tenant one"]]}]))
        .await?;
    deployment
        .push("org2", json!([{"stream":{"service_name":"fixture","job":"fake2"},"values":[[START.to_string(),"tenant two"]]}]))
        .await?;
    let all = json!([
        {"stream":{"job":"fake1","service_name":"fixture","detected_level":"unknown"},"values":[[START.to_string(),"tenant one"]]},
        {"stream":{"job":"fake2","service_name":"fixture","detected_level":"unknown"},"values":[[START.to_string(),"tenant two"]]}
    ]);
    deployment
        .wait(
            &deployment.hot,
            "org1",
            SELECTOR,
            false,
            &json!([all[0].clone()]),
        )
        .await?;
    deployment
        .wait(
            &deployment.hot,
            "org2",
            SELECTOR,
            false,
            &json!([all[1].clone()]),
        )
        .await?;
    deployment
        .wait(&deployment.hot, "org1|org2", SELECTOR, false, &all)
        .await?;
    deployment
        .stored_and_restarted("org1|org2", SELECTOR, false, &all)
        .await?;
    let cold = deployment.cold().await?;
    assert!(
        deployment
            .get(&cold, "org3", "/loki/api/v1/query_range", SELECTOR, false)
            .await?["data"]["result"]
            == json!([])
    );
    deployment
        .wait(&cold, "org1", SELECTOR, false, &json!([all[0].clone()]))
        .await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn tenant_query_limits_apply_before_and_after_storage() -> TestResult {
    let deployment =
        Deployment::with_overrides(Some("overrides:\n  tenant-a:\n    max_query_length: 1s\n"))
            .await?;
    let values = json!([[START.to_string(), "first"]]);
    deployment.push("tenant-a", streams(values.clone())).await?;
    deployment.push("tenant-b", streams(values.clone())).await?;
    let expected = expected(values);
    deployment
        .wait(&deployment.hot, "tenant-b", SELECTOR, false, &expected)
        .await?;
    let builder = deployment.builder().await?;
    let cold = deployment.cold().await?;
    deployment
        .wait(&cold, "tenant-b", SELECTOR, false, &expected)
        .await?;
    for container in [&deployment.hot, &cold] {
        for path in ["/loki/api/v1/query_range", "/loki/api/v1/labels"] {
            let response = deployment
                .request(container, "tenant-a", path, SELECTOR)
                .await?
                .send()
                .await?;
            let status = response.status();
            let body = response.text().await?;
            assert!(
                status == StatusCode::BAD_REQUEST,
                "{path}: {status}: {body}"
            );
            assert!(
                body.contains("query time range exceeds the limit"),
                "{body}"
            );
        }
        let mut url = deployment
            .request(container, "tenant-a", "/loki/api/v1/query_range", SELECTOR)
            .await?
            .build()?
            .url()
            .clone();
        url.set_query(None);
        url.query_pairs_mut().extend_pairs([
            ("query", SELECTOR),
            ("start", &START.to_string()),
            ("end", &(START + 1_000_000_000).to_string()),
            ("direction", "forward"),
        ]);
        let response: Value = deployment
            .client
            .get(url)
            .header("X-Scope-OrgID", "tenant-a")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert!(response["data"]["result"] == expected, "{response}");
        deployment
            .wait(container, "tenant-b", SELECTOR, false, &expected)
            .await?;
    }
    builder.stop().await?;
    cold.stop().await?;
    let restarted = deployment.cold().await?;
    deployment
        .wait(&restarted, "tenant-b", SELECTOR, false, &expected)
        .await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn explore_detected_fields_and_label_index_survive_storage() -> TestResult {
    let deployment = Deployment::start().await?;
    let values = json!([
        [START.to_string(), "foo=bar color=red"],
        [(START + 1_000_000_000).to_string(), "foo=bar color=blue"],
        [(START + 2_000_000_000).to_string(), "foo=bar color=red"]
    ]);
    deployment.push("tenant-a", streams(values.clone())).await?;
    deployment
        .wait(
            &deployment.hot,
            "tenant-a",
            SELECTOR,
            false,
            &expected(values.clone()),
        )
        .await?;
    check_explore(&deployment, &deployment.hot).await?;
    deployment
        .stored_and_restarted("tenant-a", SELECTOR, false, &expected(values))
        .await?;
    let cold = deployment.cold().await?;
    check_explore(&deployment, &cold).await
}

async fn check_explore(
    deployment: &Deployment,
    container: &ContainerAsync<GenericImage>,
) -> TestResult {
    let fields = deployment
        .get(
            container,
            "tenant-a",
            "/loki/api/v1/detected_fields",
            SELECTOR,
            false,
        )
        .await?;
    for (name, cardinality) in [("color", 2), ("foo", 1)] {
        let field = fields["fields"]
            .as_array()
            .ok_or("detected fields array")?
            .iter()
            .find(|field| field["label"] == name)
            .ok_or("missing field")?;
        assert!(
            field["type"] == "string" && field["cardinality"] == cardinality,
            "{fields}"
        );
    }
    let colors = deployment
        .get(
            container,
            "tenant-a",
            "/loki/api/v1/detected_field/color/values",
            SELECTOR,
            false,
        )
        .await?;
    assert!(colors["values"] == json!(["blue", "red"]), "{colors}");
    let labels = deployment
        .get(
            container,
            "tenant-a",
            "/loki/api/v1/labels",
            SELECTOR,
            false,
        )
        .await?;
    assert!(labels["data"] == json!(["service_name"]), "{labels}");
    let services = deployment
        .get(
            container,
            "tenant-a",
            "/loki/api/v1/label/service_name/values",
            SELECTOR,
            false,
        )
        .await?;
    assert!(services["data"] == json!(["fixture"]), "{services}");
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn otlp_normalization_and_metadata_survive_storage() -> TestResult {
    let deployment = Deployment::start().await?;
    let body = json!({"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"fixture"}},{"key":"cloud/region","value":{"stringValue":"west"}}]},"scopeLogs":[{"scope":{"attributes":[{"key":"instrumentation.scope","value":{"stringValue":"api"}}]},"logRecords":[{"timeUnixNano":START.to_string(),"body":{"stringValue":"checkout"},"attributes":[{"key":"thread.name","value":{"stringValue":"worker-1"}},{"key":"http.status-code","value":{"intValue":"200"}}]}]}]}]});
    accepted(
        deployment
            .client
            .post(format!(
                "{}/otlp/v1/logs",
                base_url(&deployment.distributor, PORT).await?
            ))
            .header("X-Scope-OrgID", "tenant-a")
            .json(&body)
            .send()
            .await?,
    )
    .await?;
    let expected = expected(
        json!([[START.to_string(),"checkout",{"structuredMetadata":{"detected_level":"unknown","cloud_region":"west","instrumentation_scope":"api","thread_name":"worker-1","http_status_code":"200"}}]]),
    );
    deployment
        .wait(&deployment.hot, "tenant-a", SELECTOR, true, &expected)
        .await?;
    deployment
        .stored_and_restarted("tenant-a", SELECTOR, true, &expected)
        .await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn single_binary_shutdown_drains_logs_to_storage() -> TestResult {
    let deployment = Deployment::start().await?;
    let all = role(&deployment.network, deployment.data.path(), "all", true).await?;
    deployment.ready(&all).await?;
    let empty = deployment.cold().await?;
    accepted(
        deployment
            .client
            .post(format!("{}/loki/api/v1/push", base_url(&all, PORT).await?))
            .header("X-Scope-OrgID", "tenant-a")
            .json(&json!({"streams":streams(json!([[START.to_string(),"drain me"]]))}))
            .send()
            .await?,
    )
    .await?;
    let expected = expected(json!([[START.to_string(), "drain me"]]));
    deployment
        .wait(&all, "tenant-a", SELECTOR, false, &expected)
        .await?;
    // The accumulation delay keeps the accepted row in flight. An empty
    // stored answer proves shutdown is still responsible for its durability.
    assert!(
        deployment
            .get(
                &empty,
                "tenant-a",
                "/loki/api/v1/query_range",
                SELECTOR,
                false
            )
            .await?["data"]["result"]
            == json!([])
    );
    empty.stop().await?;
    all.stop().await?;
    let cold = deployment.cold().await?;
    deployment
        .wait(&cold, "tenant-a", SELECTOR, false, &expected)
        .await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn tail_reads_backlog_then_live_wal_without_cross_tenant_entries() -> TestResult {
    use futures_util::StreamExt as _;
    use tokio_tungstenite::{
        MaybeTlsStream, WebSocketStream, connect_async,
        tungstenite::{Message, client::IntoClientRequest as _},
    };

    async fn frame(
        socket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> TestResult<Value> {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                match socket.next().await.ok_or("tail closed")?? {
                    Message::Text(text) => return Ok(serde_json::from_str(&text)?),
                    Message::Ping(_) | Message::Pong(_) => {}
                    message => return Err(format!("unexpected tail frame: {message:?}").into()),
                }
            }
        })
        .await?
    }

    let deployment = Deployment::start().await?;
    deployment
        .push("tenant-a", streams(json!([[START.to_string(), "history"]])))
        .await?;
    deployment
        .wait(
            &deployment.hot,
            "tenant-a",
            SELECTOR,
            false,
            &expected(json!([[START.to_string(), "history"]])),
        )
        .await?;
    let mut url = deployment
        .request(&deployment.hot, "tenant-a", "/loki/api/v1/tail", SELECTOR)
        .await?
        .build()?
        .url()
        .clone();
    url.set_scheme("ws").map_err(|()| "websocket scheme")?;
    let mut request = url.as_str().into_client_request()?;
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse()?);
    let (mut socket, response) = connect_async(request).await?;
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let history = frame(&mut socket).await?;
    assert!(
        history["streams"] == expected(json!([[START.to_string(), "history"]])),
        "{history}"
    );
    deployment
        .push(
            "tenant-b",
            streams(json!([[
                (START + 1_000_000_000).to_string(),
                "other tenant"
            ]])),
        )
        .await?;
    deployment
        .push(
            "tenant-a",
            streams(json!([[(START + 2_000_000_000).to_string(), "live"]])),
        )
        .await?;
    let live = frame(&mut socket).await?;
    assert!(
        live["streams"] == streams(json!([[(START + 2_000_000_000).to_string(), "live"]])),
        "{live}"
    );
    socket.close(None).await?;
    deployment
        .stored_and_restarted(
            "tenant-a",
            SELECTOR,
            false,
            &expected(json!([
                [START.to_string(), "history"],
                [(START + 2_000_000_000).to_string(), "live"]
            ])),
        )
        .await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn deletes_cancel_and_remove_matching_metadata_from_stored_blocks() -> TestResult {
    let deployment = Deployment::start().await?;
    let builder = deployment.builder().await?;
    let cancel_query = format!(r#"{SELECTOR} |= "cancel me""#);
    create_delete(&deployment, &builder, &cancel_query).await?;
    let requests = deployment
        .get(&builder, "tenant-a", "/loki/api/v1/delete", SELECTOR, false)
        .await?;
    assert!(
        requests
            .as_array()
            .is_some_and(|requests| requests.len() == 1),
        "{requests}"
    );
    assert!(
        deployment
            .get(&builder, "tenant-b", "/loki/api/v1/delete", SELECTOR, false)
            .await?
            == json!([])
    );
    let id = requests[0]["request_id"]
        .as_str()
        .ok_or("delete request ID")?;
    let mut url = url::Url::parse(&format!(
        "{}/loki/api/v1/delete",
        base_url(&builder, PORT).await?
    ))?;
    url.query_pairs_mut().append_pair("request_id", id);
    accepted(
        deployment
            .client
            .delete(url)
            .header("X-Scope-OrgID", "tenant-a")
            .send()
            .await?,
    )
    .await?;
    assert!(
        deployment
            .get(&builder, "tenant-a", "/loki/api/v1/delete", SELECTOR, false)
            .await?
            == json!([])
    );

    let values = json!([[START.to_string(),"cancel me"],[(START+1_000_000_000).to_string(),"remove me",{"user":"secret"}],[(START+2_000_000_000).to_string(),"keep me"]]);
    deployment.push("tenant-a", streams(values)).await?;
    deployment
        .push(
            "tenant-b",
            streams(json!([[(START+1_000_000_000).to_string(),"remove me",{"user":"secret"}]])),
        )
        .await?;
    let before = expected(
        json!([[START.to_string(),"cancel me",{"structuredMetadata":{"detected_level":"unknown"}}],[(START+1_000_000_000).to_string(),"remove me",{"structuredMetadata":{"detected_level":"unknown","user":"secret"}}],[(START+2_000_000_000).to_string(),"keep me",{"structuredMetadata":{"detected_level":"unknown"}}]]),
    );
    let cold = deployment.cold().await?;
    deployment
        .wait(&cold, "tenant-a", SELECTOR, true, &before)
        .await?;
    create_delete(
        &deployment,
        &builder,
        &format!(r#"{SELECTOR} | user="secret""#),
    )
    .await?;
    let after = expected(
        json!([[START.to_string(),"cancel me",{"structuredMetadata":{"detected_level":"unknown"}}],[(START+2_000_000_000).to_string(),"keep me",{"structuredMetadata":{"detected_level":"unknown"}}]]),
    );
    deployment
        .wait(&deployment.hot, "tenant-a", SELECTOR, true, &after)
        .await?;
    // An independent data root has no delete-request file. Its result proves
    // that compaction removed the row physically, rather than filtering it.
    let clean_data = tempfile::tempdir()?;
    let unfiltered = role(&deployment.network, clean_data.path(), "querier", false).await?;
    deployment.ready(&unfiltered).await?;
    deployment
        .wait(&unfiltered, "tenant-a", SELECTOR, true, &after)
        .await?;
    deployment.wait(&unfiltered,"tenant-b",SELECTOR,true,&expected(json!([[(START+1_000_000_000).to_string(),"remove me",{"structuredMetadata":{"detected_level":"unknown","user":"secret"}}]]))).await?;
    builder.stop().await?;
    unfiltered.stop().await?;
    let restarted = role(&deployment.network, clean_data.path(), "querier", false).await?;
    deployment.ready(&restarted).await?;
    deployment
        .wait(&restarted, "tenant-a", SELECTOR, true, &after)
        .await
}

async fn create_delete(
    deployment: &Deployment,
    builder: &ContainerAsync<GenericImage>,
    query: &str,
) -> TestResult {
    let mut url = url::Url::parse(&format!(
        "{}/loki/api/v1/delete",
        base_url(builder, PORT).await?
    ))?;
    url.query_pairs_mut().extend_pairs([
        ("query", query),
        ("start", &(START / 1_000_000_000).to_string()),
        ("end", &(END / 1_000_000_000).to_string()),
    ]);
    accepted(
        deployment
            .client
            .post(url)
            .header("X-Scope-OrgID", "tenant-a")
            .send()
            .await?,
    )
    .await
}
