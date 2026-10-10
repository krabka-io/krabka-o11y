//! Isolated deployment tests following Mimir's container integration pattern.
//!
//! Each case starts the locally built role binaries, a formatted broker, and
//! `MinIO` on its own Docker network. Requests use public HTTP endpoints. A
//! querier without a WAL connection checks that blocks, rather than the hot
//! head, supply the stored answer. Container and network cleanup is automatic.
//!
//! Run `bazel test --config=docker
//! //crates/metrics-service:metrics_deployment_docker_test`.

use std::time::Duration;

use assert2::assert;
use krabka_metrics::wire::pb;
use prost::Message as _;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt as _,
    core::{ContainerPort, Mount},
};

#[path = "support/container_deployment.rs"]
mod container_deployment;
#[path = "support/deployment_evidence.rs"]
mod deployment_evidence;
use container_deployment::{
    DeploymentInfrastructure, TestResult, base_url, image, start, start_infrastructure,
    wait_until_ready,
};
use deployment_evidence::record_evidence;

const QUERY_TIMEOUT: Duration = Duration::from_secs(45);
const DATA_PORT: u16 = 4041;
const ADMIN_PORT: u16 = 9404;
const METRIC: &str = "deployment_temperature";
// Fixed input makes every sample and evaluation timestamp independently known.
const START_MS: i64 = 1_700_000_000_000;

#[path = "support/mimir_deployment.rs"]
mod mimir_deployment;

#[derive(Clone, Copy)]
enum RemoteWrite {
    V1,
    V2,
}

#[tokio::test]
#[ignore = "requires Docker and the Bazel-built application and pinned dependency images"]
async fn remote_write_v1_survives_the_hot_to_block_transition_and_restart() -> TestResult {
    deployment_roundtrip(RemoteWrite::V1).await
}

#[tokio::test]
#[ignore = "requires Docker and the Bazel-built application and pinned dependency images"]
async fn remote_write_v2_survives_the_hot_to_block_transition_and_restart() -> TestResult {
    deployment_roundtrip(RemoteWrite::V2).await
}

/// Containers hold the network alive; the directories outlive the broker's
/// bind mount. Field drop order removes the roles before the infrastructure.
struct Deployment {
    distributor: ContainerAsync<GenericImage>,
    hot: ContainerAsync<GenericImage>,
    infrastructure: DeploymentInfrastructure,
}

impl Deployment {
    async fn start() -> TestResult<Self> {
        Self::with_overrides(None).await
    }

    async fn with_overrides(overrides: Option<&str>) -> TestResult<Self> {
        let infrastructure = Box::pin(start_infrastructure("metrics", overrides)).await?;
        let network = &infrastructure.network;
        let broker_name = format!("{network}-broker");
        let mut distributor_request = image("KRABKA")
            .with_exposed_port(ContainerPort::Tcp(DATA_PORT))
            .with_network(network)
            .with_cmd([
                "krabka-metrics".to_string(),
                "--target=distributor".to_string(),
                format!("--listen=0.0.0.0:{DATA_PORT}"),
                format!("--bootstrap={broker_name}:9092"),
            ]);
        if overrides.is_some() {
            let path = infrastructure.data.path().join("overrides.yaml");
            distributor_request = distributor_request
                .with_mount(Mount::bind_mount(path.to_string_lossy(), "/overrides.yaml"))
                .with_env_var("KRABKA_METRICS_RUNTIME_OVERRIDES", "/overrides.yaml");
        }
        let distributor = start(distributor_request).await?;
        let hot = start(query_role(network).with_cmd([
            "krabka-metrics-service".to_string(),
            "--target=querier".to_string(),
            format!("--listen=0.0.0.0:{DATA_PORT}"),
            "--object-store-url=s3://metrics".to_string(),
            format!("--wal-bootstrap={broker_name}:9092"),
            "--cold-cache-ttl=100ms".to_string(),
        ]))
        .await?;
        let deployment = Self {
            distributor,
            hot,
            infrastructure,
        };
        deployment.ready(&deployment.distributor, DATA_PORT).await?;
        deployment.ready(&deployment.hot, DATA_PORT).await?;
        Ok(deployment)
    }

    async fn ready(&self, container: &ContainerAsync<GenericImage>, port: u16) -> TestResult {
        wait_until_ready(&self.infrastructure.client, container, port).await
    }

    async fn builder(&self) -> TestResult<ContainerAsync<GenericImage>> {
        self.builder_with_rows(krabka_metrics::DEFAULT_FLUSH_MAX_ROWS)
            .await
    }

    async fn builder_with_rows(&self, rows: usize) -> TestResult<ContainerAsync<GenericImage>> {
        let builder = start(s3_role(&self.infrastructure.network, ADMIN_PORT).with_cmd([
            "krabka-metrics".to_string(),
            "--target=block-builder".to_string(),
            format!("--bootstrap={}-broker:9092", self.infrastructure.network),
            "--object-store-url=s3://metrics".to_string(),
            "--block-builder-poll-timeout=100ms".to_string(),
            "--block-builder-flush-max-age=100ms".to_string(),
            format!("--block-builder-flush-max-rows={rows}"),
        ]))
        .await?;
        self.ready(&builder, ADMIN_PORT).await?;
        Ok(builder)
    }

    async fn cold(&self) -> TestResult<ContainerAsync<GenericImage>> {
        let cold = start(query_role(&self.infrastructure.network).with_cmd([
            "krabka-metrics-service",
            "--target=querier",
            "--listen=0.0.0.0:4041",
            "--object-store-url=s3://metrics",
            "--cold-cache-ttl=100ms",
        ]))
        .await?;
        self.ready(&cold, DATA_PORT).await?;
        Ok(cold)
    }
}

fn s3_role(network: &str, port: u16) -> ContainerRequest<GenericImage> {
    let image = image("KRABKA").with_exposed_port(ContainerPort::Tcp(port));
    let image = if port == DATA_PORT {
        image.with_exposed_port(ContainerPort::Tcp(ADMIN_PORT))
    } else {
        image
    };
    image
        .with_network(network)
        .with_env_var("AWS_ACCESS_KEY_ID", "krabkametrics")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "krabkametrics")
        .with_env_var("AWS_ENDPOINT_URL", format!("http://{network}-minio:9000"))
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("AWS_ALLOW_HTTP", "true")
        .with_env_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")
        .with_env_var("AWS_EC2_METADATA_DISABLED", "true")
}

fn query_role(network: &str) -> ContainerRequest<GenericImage> {
    s3_role(network, DATA_PORT).with_env_var(
        "KRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK",
        "1000000h",
    )
}

fn write_body(version: RemoteWrite, value: f64) -> TestResult<Vec<u8>> {
    let labels = [("__name__", METRIC), ("site", "office")];
    let samples = [0_i32, 1, 2].map(|step| pb::v1::Sample {
        timestamp: START_MS + i64::from(step) * 1000,
        value: value + f64::from(step),
    });
    let protobuf = match version {
        RemoteWrite::V1 => pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                labels: labels
                    .into_iter()
                    .map(|(name, value)| pb::v1::Label {
                        name: name.to_string(),
                        value: value.to_string(),
                    })
                    .collect(),
                samples: samples.to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec(),
        RemoteWrite::V2 => pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                METRIC.into(),
                "site".into(),
                "office".into(),
            ],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4],
                samples: samples
                    .into_iter()
                    .map(|sample| pb::v2::Sample {
                        timestamp: sample.timestamp,
                        value: sample.value,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
        }
        .encode_to_vec(),
    };
    Ok(snap::raw::Encoder::new().compress_vec(&protobuf)?)
}

async fn range(client: &Client, base: &str, tenant: &str) -> TestResult<Value> {
    range_query(client, base, tenant, METRIC).await
}

async fn range_query(client: &Client, base: &str, tenant: &str, query: &str) -> TestResult<Value> {
    let mut url = url::Url::parse(&format!("{base}/api/v1/query_range"))?;
    url.query_pairs_mut().extend_pairs([
        ("query", query.to_string()),
        ("start", (START_MS / 1000).to_string()),
        ("end", (START_MS / 1000 + 2).to_string()),
        ("step", "1".to_string()),
    ]);
    let response = client
        .get(url)
        .header("X-Scope-OrgID", tenant)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if status != StatusCode::OK {
        return Err(format!("{base} tenant {tenant}: HTTP {status}: {body}").into());
    }
    let body: Value = serde_json::from_str(&body)?;
    assert!(body["status"] == "success");
    assert!(body["data"]["resultType"] == "matrix");
    Ok(body["data"]["result"].clone())
}

fn expected(value: i32) -> Value {
    json!([{
        "metric": {"__name__": METRIC, "site": "office"},
        "values": [
            [1_700_000_000, value.to_string()],
            [1_700_000_001, (value + 1).to_string()],
            [1_700_000_002, (value + 2).to_string()],
        ],
    }])
}

async fn wait_samples(client: &Client, base: &str, tenant: &str, expected: &Value) -> TestResult {
    wait_range_query(client, base, tenant, METRIC, expected)
        .await
        .map(|_| ())
}

async fn wait_range_query(
    client: &Client,
    base: &str,
    tenant: &str,
    query: &str,
    expected: &Value,
) -> TestResult<Value> {
    let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
    loop {
        let actual = range_query(client, base, tenant, query).await?;
        if actual == *expected {
            return Ok(actual);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "{base} tenant {tenant} query {query}: expected {expected}, last result {actual}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn check_compound_queries(
    client: &Client,
    base: &str,
    tenant: &str,
    value: Option<i32>,
    phase: &str,
    evidence: &mut Vec<Value>,
    filename: &str,
) -> TestResult {
    let compound =
        format!(r#"(sum(sum_over_time({METRIC}{{site=~"off.*",missing!="x"}}[2s])) + 2) * 3"#);
    // A two-second range excludes its left boundary. The fixture has exactly
    // v, v+1, v+2 at the three evaluation timestamps, hence these literal sums.
    let expected_compound = value.map_or_else(
        || json!([]),
        |v| {
            json!([{"metric":{}, "values":[
                [1_700_000_000, ((v+2)*3).to_string()],
                [1_700_000_001, ((2*v+3)*3).to_string()],
                [1_700_000_002, ((2*v+5)*3).to_string()],
            ]}])
        },
    );
    for (query, expected) in [
        (compound, expected_compound),
        (format!(r#"sum({METRIC}{{site="missing"}})"#), json!([])),
    ] {
        let result = wait_range_query(client, base, tenant, &query, &expected).await;
        record_evidence(
            evidence,
            json!({"phase":phase, "tenant":tenant, "query":query,
            "expected":expected, "actual":result.as_ref().ok(),
            "status":if result.is_ok() {"matched"} else {"mismatch"},
            "error":result.as_ref().err().map(ToString::to_string)}),
            filename,
        )?;
        result?;
    }
    Ok(())
}

async fn deployment_roundtrip(version: RemoteWrite) -> TestResult {
    let mut evidence = Vec::new();
    let filename = match version {
        RemoteWrite::V1 => "metrics-v1-query-transitions.json",
        RemoteWrite::V2 => "metrics-v2-query-transitions.json",
    };
    let deployment = Deployment::start().await?;
    let client = &deployment.infrastructure.client;
    let write = base_url(&deployment.distributor, DATA_PORT).await?;
    let hot = base_url(&deployment.hot, DATA_PORT).await?;
    let cold = deployment.cold().await?;
    let cold_url = base_url(&cold, DATA_PORT).await?;
    for (tenant, value) in [("tenant-a", 7.0), ("tenant-b", 19.0)] {
        push(client, &write, version, tenant, value).await?;
    }
    // No builder is running yet. This answer can only come from the WAL head.
    for (tenant, value) in [("tenant-a", 7), ("tenant-b", 19)] {
        wait_samples(client, &hot, tenant, &expected(value)).await?;
        assert!(range(client, &cold_url, tenant).await? == json!([]));
        check_compound_queries(
            client,
            &hot,
            tenant,
            Some(value),
            "hot-only",
            &mut evidence,
            filename,
        )
        .await?;
        check_compound_queries(
            client,
            &cold_url,
            tenant,
            None,
            "cold-before-publication",
            &mut evidence,
            filename,
        )
        .await?;
    }
    assert!(range(client, &hot, "tenant-empty").await? == json!([]));
    check_compound_queries(
        client,
        &hot,
        "tenant-empty",
        None,
        "hot-tenant-negative",
        &mut evidence,
        filename,
    )
    .await?;
    let builder = deployment.builder().await?;
    for (tenant, value) in [("tenant-a", 7), ("tenant-b", 19)] {
        wait_samples(client, &cold_url, tenant, &expected(value)).await?;
        // Expire the hot querier's pre-publication cold snapshot.
        tokio::time::sleep(Duration::from_millis(100)).await;
        // The hot and stored copies must not create duplicate query samples.
        assert!(range(client, &hot, tenant).await? == expected(value));
        check_compound_queries(
            client,
            &hot,
            tenant,
            Some(value),
            "hot-cold-overlap",
            &mut evidence,
            filename,
        )
        .await?;
        check_compound_queries(
            client,
            &cold_url,
            tenant,
            Some(value),
            "persisted",
            &mut evidence,
            filename,
        )
        .await?;
    }
    // Remove both sources of live state. A new cold querier has no WAL head,
    // no query cache and only S3 blocks to read, even after the writer restarts.
    builder.stop_with_timeout(Some(30)).await?;
    deployment.hot.stop_with_timeout(Some(30)).await?;
    cold.rm().await?;
    builder.start().await?;
    deployment.ready(&builder, ADMIN_PORT).await?;
    let reopened = deployment.cold().await?;
    let reopened_url = base_url(&reopened, DATA_PORT).await?;
    // The restarted writer must also consume records after its committed cut.
    push(client, &write, version, "tenant-c", 31.0).await?;
    let restart_result = wait_samples(client, &reopened_url, "tenant-c", &expected(31)).await;
    // The original log consumer ends at stop and start does not reattach it.
    // Read a finite snapshot after the query wait, preserving its timing and
    // result even if diagnostics fail. This includes both writer lifetimes.
    let diagnostics = async {
        for (name, contents) in [
            ("stdout", builder.stdout_to_vec().await?),
            ("stderr", builder.stderr_to_vec().await?),
        ] {
            eprintln!(
                "[block-builder restart {name}] {}",
                String::from_utf8_lossy(&contents)
            );
        }
        let admin = base_url(&builder, ADMIN_PORT).await?;
        for path in ["/ready", "/status/recovery"] {
            let response = deployment
                .infrastructure
                .client
                .get(format!("{admin}{path}"))
                .send()
                .await?;
            let status = response.status();
            let body = response.text().await?;
            eprintln!("[block-builder restart {path}] {status} {body}");
        }
        TestResult::Ok(())
    };
    match tokio::time::timeout(Duration::from_secs(5), diagnostics).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => eprintln!("block-builder restart diagnostics failed: {error}"),
        Err(error) => eprintln!("block-builder restart diagnostics timed out: {error}"),
    }
    restart_result?;
    check_compound_queries(
        client,
        &reopened_url,
        "tenant-c",
        Some(31),
        "resumed-ingest",
        &mut evidence,
        filename,
    )
    .await?;
    for (tenant, value) in [("tenant-a", 7), ("tenant-b", 19)] {
        wait_samples(client, &reopened_url, tenant, &expected(value)).await?;
        check_compound_queries(
            client,
            &reopened_url,
            tenant,
            Some(value),
            "restart",
            &mut evidence,
            filename,
        )
        .await?;
    }
    assert!(range(client, &reopened_url, "tenant-empty").await? == json!([]));
    check_compound_queries(
        client,
        &reopened_url,
        "tenant-empty",
        None,
        "restart-tenant-negative",
        &mut evidence,
        filename,
    )
    .await?;
    // Keep the backing resources alive until every role has been removed.
    reopened.rm().await?;
    builder.rm().await?;
    deployment.distributor.rm().await?;
    deployment.hot.rm().await?;
    drop(deployment.infrastructure);
    Ok(())
}

async fn push(
    client: &Client,
    base: &str,
    version: RemoteWrite,
    tenant: &str,
    value: f64,
) -> TestResult {
    let (content_type, protocol_version) = match version {
        RemoteWrite::V1 => ("application/x-protobuf", "0.1.0"),
        RemoteWrite::V2 => (
            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
            "2.0.0",
        ),
    };
    let response = client
        .post(format!("{base}/api/v1/push"))
        .header("X-Scope-OrgID", tenant)
        .header("Content-Type", content_type)
        .header("Content-Encoding", "snappy")
        .header("X-Prometheus-Remote-Write-Version", protocol_version)
        .body(write_body(version, value)?)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    assert!(status.is_success(), "push: HTTP {status}: {body}");
    Ok(())
}
