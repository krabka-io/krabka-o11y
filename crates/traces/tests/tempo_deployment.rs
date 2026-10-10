//! Tempo deployment scenarios against Krabka roles, the broker WAL, and `MinIO`.
//! Fixed inputs supply expected spans. Cold queriers have no live store.
//! See `tempo_deployment.md` for the pinned upstream scenarios.

use std::{io::Write as _, os::unix::fs::MetadataExt as _, time::Duration};

use assert2::assert;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{ExportTraceServiceRequest, trace_service_client::TraceServiceClient},
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value as OtlpValue},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData},
};
use prost::Message as _;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt as _,
    core::{ContainerPort, Mount},
};

#[path = "../../metrics-service/tests/support/container_deployment.rs"]
mod container_deployment;
use container_deployment::{
    TestResult, base_url, deployment_network, image, start, start_broker, start_minio,
    wait_until_ready,
};

const PORT: u16 = 3200;
const ADMIN: u16 = 9404;
const START: u64 = 1_700_000_000_000_000_000;
const TIMEOUT: Duration = Duration::from_secs(45);
const TENANT: &str = "tenant-a";

// Containers release their mounts before the temporary directories are removed.
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
    async fn start(overrides: Option<&str>) -> TestResult<Self> {
        let broker_data = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        if let Some(overrides) = overrides {
            std::fs::write(data.path().join("overrides.yaml"), overrides)?;
        }
        let network = deployment_network("traces", broker_data.path())?;
        let broker = start_broker(broker_data.path(), &network).await?;
        let minio = start_minio(&network, "traces").await?;
        let client = Client::builder().timeout(Duration::from_secs(5)).build()?;
        let distributor = role(&network, data.path(), "distributor", false, &[]).await?;
        let hot = role(&network, data.path(), "querier", true, &[]).await?;
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
        wait_until_ready(&self.client, container, ADMIN).await
    }

    async fn role(
        &self,
        target: &str,
        args: &[String],
    ) -> TestResult<ContainerAsync<GenericImage>> {
        let container = role(&self.network, self.data.path(), target, false, args).await?;
        self.ready(&container).await?;
        Ok(container)
    }

    async fn push(&self, tenant: &str, data: TracesData) -> TestResult {
        accepted(
            self.client
                .post(format!(
                    "{}/v1/traces",
                    base_url(&self.distributor, 4318).await?
                ))
                .header("X-Scope-OrgID", tenant)
                .header("Content-Type", "application/x-protobuf")
                .body(data.encode_to_vec())
                .send()
                .await?,
        )
        .await
    }

    async fn request(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        path: &str,
        params: &[(&str, &str)],
    ) -> TestResult<reqwest::Response> {
        let mut url = url::Url::parse(&format!("{}{path}", base_url(container, PORT).await?))?;
        url.query_pairs_mut().extend_pairs(params.iter().copied());
        Ok(self
            .client
            .get(url)
            .header("X-Scope-OrgID", tenant)
            .send()
            .await?)
    }

    async fn get(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        path: &str,
        params: &[(&str, &str)],
    ) -> TestResult<Value> {
        let response = self.request(container, tenant, path, params).await?;
        let status = response.status();
        let body = response.text().await?;
        assert!(status == StatusCode::OK, "{path}: {status}: {body}");
        Ok(serde_json::from_str(&body)?)
    }

    async fn trace(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        id: u8,
        expected: &[Value],
    ) -> TestResult {
        let path = format!("/api/v2/traces/{}", hex::encode([id; 16]));
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let response = self.request(container, tenant, &path, &[]).await?;
                let status = response.status();
                let body = response.text().await?;
                if status == StatusCode::OK {
                    let trace: Value = serde_json::from_str(&body)?;
                    let actual = trace_rows(&trace);
                    if actual == expected {
                        assert!(trace["status"] == "COMPLETE" && trace["message"] == "");
                        return Ok(());
                    }
                    eprintln!("{path}: {actual:?}");
                } else {
                    assert!(status == StatusCode::NOT_FOUND, "{path}: {status}: {body}");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|error| format!("{path}, expected {expected:?}: {error}"))?
    }

    async fn missing(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        id: u8,
    ) -> TestResult {
        let path = format!("/api/v2/traces/{}", hex::encode([id; 16]));
        assert!(
            self.request(container, tenant, &path, &[]).await?.status() == StatusCode::NOT_FOUND
        );
        Ok(())
    }

    async fn stored(
        &self,
        tenant: &str,
        id: u8,
        expected: &[Value],
    ) -> TestResult<ContainerAsync<GenericImage>> {
        let cold = self.role("querier", &[]).await?;
        self.missing(&cold, tenant, id).await?;
        let builder = self.role("block-builder", &[]).await?;
        self.trace(&cold, tenant, id, expected).await?;
        // No writer or previous process cache can supply the restarted answer.
        builder.stop().await?;
        cold.stop().await?;
        let restarted = self.role("querier", &[]).await?;
        self.trace(&restarted, tenant, id, expected).await?;
        Ok(restarted)
    }
}

async fn role(
    network: &str,
    directory: &std::path::Path,
    target: &str,
    live: bool,
    extra: &[String],
) -> TestResult<ContainerAsync<GenericImage>> {
    let mut command = vec![
        "krabka-traces".into(),
        format!("--target={target}"),
        format!("--bootstrap={network}-broker:9092"),
        "--object-store-url=s3://traces/traces".into(),
        "--retention=1000000h".into(),
        "--block-retention=0s".into(),
        format!(
            "--block-builder-window={}",
            if target == "all" { "3s" } else { "100ms" }
        ),
        format!(
            "--block-builder-flush-max-age={}",
            if target == "all" { "30s" } else { "100ms" }
        ),
        "--querier-membership-refresh-interval=100ms".into(),
    ];
    if live {
        command.push("--querier-live-store".into());
    }
    if directory.join("overrides.yaml").exists() {
        command.push("--traces-limits-overrides-config=/data/overrides.yaml".into());
    }
    command.extend_from_slice(extra);
    let metadata = directory.metadata()?;
    let mut application = image("KRABKA");
    for port in [PORT, ADMIN, 4317, 4318, 9411, 14250] {
        application = application.with_exposed_port(ContainerPort::Tcp(port));
    }
    let mut request = application
        .with_network(network)
        .with_user(format!("{}:{}", metadata.uid(), metadata.gid()))
        .with_mount(Mount::bind_mount(directory.to_string_lossy(), "/data"))
        .with_env_var("AWS_ACCESS_KEY_ID", "krabkatraces")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "krabkatraces")
        .with_env_var("AWS_ENDPOINT_URL", format!("http://{network}-minio:9000"))
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("AWS_ALLOW_HTTP", "true")
        .with_env_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")
        .with_env_var("AWS_EC2_METADATA_DISABLED", "true")
        .with_cmd(command);
    if live {
        request = request.with_container_name(format!("{network}-hot"));
    }
    start(request).await
}

async fn accepted(response: reqwest::Response) -> TestResult {
    let status = response.status();
    let text = response.text().await?;
    assert!(status.is_success(), "push: {status}: {text}");
    Ok(())
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(OtlpValue::StringValue(value.into())),
        }),
        ..KeyValue::default()
    }
}

fn input(id: u8, ids: &[u8], service: &str) -> TracesData {
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", service)],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "deployment".into(),
                    version: "1.0".into(),
                    ..InstrumentationScope::default()
                }),
                spans: ids
                    .iter()
                    .map(|span_id| Span {
                        trace_id: vec![id; 16],
                        span_id: vec![*span_id; 8],
                        parent_span_id: if *span_id == 2 { vec![1; 8] } else { vec![] },
                        name: if *span_id == 1 {
                            "checkout"
                        } else {
                            "database"
                        }
                        .into(),
                        kind: if *span_id == 1 { 2 } else { 3 },
                        start_time_unix_nano: START + if *span_id == 1 { 0 } else { 100_000_000 },
                        end_time_unix_nano: START
                            + if *span_id == 1 {
                                500_000_000
                            } else {
                                250_000_000
                            },
                        attributes: vec![if *span_id == 1 {
                            kv("http.route", "/checkout")
                        } else {
                            kv("db.system", "postgresql")
                        }],
                        status: if *span_id == 2 {
                            Some(Status {
                                code: 2,
                                message: "failed".into(),
                            })
                        } else {
                            None
                        },
                        ..Span::default()
                    })
                    .collect(),
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
}

fn expected(id: u8, ids: &[u8], service: &str) -> Vec<Value> {
    let mut rows: Vec<_> = ids.iter().map(|span_id| {
        let span = if *span_id == 1 {
            json!({"traceId":STANDARD.encode([id;16]), "spanId":STANDARD.encode([1;8]), "name":"checkout", "kind":"SPAN_KIND_SERVER", "startTimeUnixNano":START.to_string(), "endTimeUnixNano":(START+500_000_000).to_string(), "status":{}, "attributes":[{"key":"http.route", "value":{"stringValue":"/checkout"}}]})
        } else {
            json!({"traceId":STANDARD.encode([id;16]), "spanId":STANDARD.encode([2;8]), "parentSpanId":STANDARD.encode([1;8]), "name":"database", "kind":"SPAN_KIND_CLIENT", "startTimeUnixNano":(START+100_000_000).to_string(), "endTimeUnixNano":(START+250_000_000).to_string(), "status":{"code":"STATUS_CODE_ERROR", "message":"failed"}, "attributes":[{"key":"db.system", "value":{"stringValue":"postgresql"}}]})
        };
        json!({"resource":{"attributes":[{"key":"service.name", "value":{"stringValue":service}}]}, "scope":{"name":"deployment","version":"1.0"}, "span":span})
    }).collect();
    rows.sort_by_key(|row| row["span"]["spanId"].to_string());
    rows
}

fn trace_rows(trace: &Value) -> Vec<Value> {
    let mut rows = Vec::new();
    for resource in trace["trace"]["resourceSpans"]
        .as_array()
        .expect("resourceSpans")
    {
        for scope in resource["scopeSpans"].as_array().expect("scopeSpans") {
            for span in scope["spans"].as_array().expect("spans") {
                rows.push(
                    json!({"resource":resource["resource"], "scope":scope["scope"], "span":span}),
                );
            }
        }
    }
    rows.sort_by_key(|row| row["span"]["spanId"].to_string());
    rows
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn otlp_http_survives_storage_and_restart() -> TestResult {
    receiver(false, false).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn gzip_otlp_http_survives_storage_and_restart() -> TestResult {
    receiver(true, false).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn otlp_grpc_survives_storage_and_restart() -> TestResult {
    receiver(false, true).await
}
async fn receiver(gzip: bool, grpc: bool) -> TestResult {
    let deployment = Deployment::start(None).await?;
    let data = input(1, &[1, 2], "checkout");
    if grpc {
        let mut client =
            TraceServiceClient::connect(base_url(&deployment.distributor, 4317).await?).await?;
        let mut request = tonic::Request::new(ExportTraceServiceRequest {
            resource_spans: data.resource_spans,
        });
        request
            .metadata_mut()
            .insert("x-scope-orgid", TENANT.parse()?);
        let response = client.export(request).await?.into_inner();
        assert!(response.partial_success.is_none());
    } else {
        let body = data.encode_to_vec();
        let mut request = deployment
            .client
            .post(format!(
                "{}/v1/traces",
                base_url(&deployment.distributor, 4318).await?
            ))
            .header("X-Scope-OrgID", TENANT)
            .header("Content-Type", "application/x-protobuf");
        let body = if gzip {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&body)?;
            request = request.header("Content-Encoding", "gzip");
            encoder.finish()?
        } else {
            body
        };
        accepted(request.body(body).send().await?).await?;
    }
    let rows = expected(1, &[1, 2], "checkout");
    deployment.trace(&deployment.hot, TENANT, 1, &rows).await?;
    deployment.stored(TENANT, 1, &rows).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn zipkin_receiver_survives_storage_and_restart() -> TestResult {
    let deployment = Deployment::start(None).await?;
    let body = json!([{"traceId":hex::encode([1;16]), "id":hex::encode([1;8]), "name":"checkout", "kind":"SERVER", "timestamp":START/1000, "duration":500_000, "localEndpoint":{"serviceName":"checkout"}, "tags":{"http.route":"/checkout"}}]);
    accepted(
        deployment
            .client
            .post(format!(
                "{}/api/v2/spans",
                base_url(&deployment.distributor, 9411).await?
            ))
            .header("X-Scope-OrgID", TENANT)
            .json(&body)
            .send()
            .await?,
    )
    .await?;
    let mut rows = expected(1, &[1], "checkout");
    rows[0]["scope"] = json!({});
    deployment.trace(&deployment.hot, TENANT, 1, &rows).await?;
    deployment.stored(TENANT, 1, &rows).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn jaeger_grpc_receiver_survives_storage_and_restart() -> TestResult {
    use krabka_traces::wire::jaeger_grpc::api_v2::{
        Batch, KeyValue as JaegerKeyValue, PostSpansRequest, Process, Span as JaegerSpan,
        collector_service_client::CollectorServiceClient,
    };
    let deployment = Deployment::start(None).await?;
    let mut client =
        CollectorServiceClient::connect(base_url(&deployment.distributor, 14250).await?).await?;
    let mut request = tonic::Request::new(PostSpansRequest {
        batch: Some(Batch {
            process: Some(Process {
                service_name: "checkout".into(),
                tags: vec![],
            }),
            spans: vec![JaegerSpan {
                trace_id: vec![1; 16],
                span_id: vec![1; 8],
                operation_name: "checkout".into(),
                start_time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                duration: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 500_000_000,
                }),
                tags: vec![
                    JaegerKeyValue {
                        key: "http.route".into(),
                        v_str: "/checkout".into(),
                        ..JaegerKeyValue::default()
                    },
                    JaegerKeyValue {
                        key: "span.kind".into(),
                        v_str: "server".into(),
                        ..JaegerKeyValue::default()
                    },
                ],
                ..JaegerSpan::default()
            }],
        }),
    });
    request
        .metadata_mut()
        .insert("x-scope-orgid", TENANT.parse()?);
    client.post_spans(request).await?;
    let mut rows = expected(1, &[1], "checkout");
    rows[0]["scope"] = json!({});
    rows[0]["span"]["attributes"]
        .as_array_mut()
        .ok_or("attributes")?
        .push(json!({"key":"span.kind","value":{"stringValue":"server"}}));
    deployment.trace(&deployment.hot, TENANT, 1, &rows).await?;
    deployment.stored(TENANT, 1, &rows).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn trace_by_id_assembles_separate_persisted_batches_without_duplicates() -> TestResult {
    let deployment = Deployment::start(None).await?;
    deployment.push(TENANT, input(1, &[1], "checkout")).await?;
    let builder = deployment.role("block-builder", &[]).await?;
    let cold = deployment.role("querier", &[]).await?;
    deployment
        .trace(&cold, TENANT, 1, &expected(1, &[1], "checkout"))
        .await?;
    // The first span is already stored before a second batch repeats it.
    deployment
        .push(TENANT, input(1, &[1, 2], "checkout"))
        .await?;
    let rows = expected(1, &[1, 2], "checkout");
    deployment.trace(&deployment.hot, TENANT, 1, &rows).await?;
    deployment.trace(&cold, TENANT, 1, &rows).await?;
    builder.stop().await?;
    cold.stop().await?;
    let restarted = deployment.role("querier", &[]).await?;
    deployment.trace(&restarted, TENANT, 1, &rows).await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn traceql_search_uses_structural_relationships_after_restart() -> TestResult {
    let deployment = Deployment::start(None).await?;
    deployment
        .push(TENANT, input(1, &[1, 2], "checkout"))
        .await?;
    deployment.push(TENANT, input(2, &[1], "other")).await?;
    let rows = expected(1, &[1, 2], "checkout");
    deployment.trace(&deployment.hot, TENANT, 1, &rows).await?;
    let mut evidence = Vec::new();
    let filename = "tempo-structural-query-transitions.json";
    check_trace_queries(
        &deployment,
        &deployment.hot,
        TENANT,
        &[1, 2],
        "hot-only",
        &mut evidence,
        filename,
    )
    .await?;
    check_trace_queries(
        &deployment,
        &deployment.hot,
        "tenant-c",
        &[],
        "hot-tenant-negative",
        &mut evidence,
        filename,
    )
    .await?;
    let cold = deployment.role("querier", &[]).await?;
    deployment.missing(&cold, TENANT, 1).await?;
    check_trace_queries(
        &deployment,
        &cold,
        TENANT,
        &[],
        "cold-before-publication",
        &mut evidence,
        filename,
    )
    .await?;
    let builder = deployment.role("block-builder", &[]).await?;
    deployment.trace(&cold, TENANT, 1, &rows).await?;
    deployment
        .trace(&cold, TENANT, 2, &expected(2, &[1], "other"))
        .await?;
    check_trace_queries(
        &deployment,
        &cold,
        TENANT,
        &[1, 2],
        "persisted",
        &mut evidence,
        filename,
    )
    .await?;
    check_trace_queries(
        &deployment,
        &deployment.hot,
        TENANT,
        &[1, 2],
        "hot-cold-overlap",
        &mut evidence,
        filename,
    )
    .await?;
    builder.stop().await?;
    cold.stop().await?;
    let restarted = deployment.role("querier", &[]).await?;
    deployment.trace(&restarted, TENANT, 1, &rows).await?;
    check_trace_queries(
        &deployment,
        &restarted,
        TENANT,
        &[1, 2],
        "restart",
        &mut evidence,
        filename,
    )
    .await?;
    check_trace_queries(
        &deployment,
        &restarted,
        "tenant-c",
        &[],
        "restart-tenant-negative",
        &mut evidence,
        filename,
    )
    .await
}

fn transition_search_rows(result: &Value) -> TestResult<Vec<(String, String)>> {
    let mut rows = Vec::new();
    for trace in result["traces"].as_array().ok_or("missing search traces")? {
        let trace_id = trace["traceID"].as_str().ok_or("missing trace ID")?;
        for set in trace["spanSets"].as_array().ok_or("missing span sets")? {
            for span in set["spans"].as_array().ok_or("missing selected spans")? {
                rows.push((
                    trace_id.to_owned(),
                    span["spanID"].as_str().ok_or("missing span ID")?.to_owned(),
                ));
            }
        }
    }
    rows.sort();
    Ok(rows)
}

async fn wait_trace_query(
    deployment: &Deployment,
    container: &ContainerAsync<GenericImage>,
    tenant: &str,
    path: &str,
    query: &str,
    expected: &Value,
) -> TestResult<Value> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let response = deployment
                .get(
                    container,
                    tenant,
                    path,
                    &[
                        ("q", query),
                        ("start", "1700000000"),
                        ("end", "1700000001"),
                        ("step", "1s"),
                        ("limit", "10"),
                        ("spss", "10"),
                    ],
                )
                .await?;
            let actual = if path == "/api/search" {
                json!(transition_search_rows(&response)?)
            } else {
                let mut series = response["series"]
                    .as_array()
                    .ok_or("missing metrics series")?
                    .clone();
                series.sort_by_key(|series| series["labels"].to_string());
                json!(series)
            };
            if actual == *expected {
                return Ok(actual);
            }
            eprintln!("{tenant} {query}: expected {expected}, actual {actual}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|error| format!("{tenant} {query}, expected {expected}: {error}"))?
}

async fn check_trace_queries(
    deployment: &Deployment,
    container: &ContainerAsync<GenericImage>,
    tenant: &str,
    span_ids: &[u8],
    phase: &str,
    evidence: &mut Vec<Value>,
    filename: &str,
) -> TestResult {
    let child = if span_ids.contains(&2) {
        vec![2]
    } else {
        vec![]
    };
    let root = if span_ids.contains(&1) {
        vec![1]
    } else {
        vec![]
    };
    let counted = if span_ids.len() > 1 {
        span_ids.to_vec()
    } else {
        vec![]
    };
    let mut cases = Vec::new();
    for (query, ids) in [
        (
            r#"{ resource.service.name = "checkout" }"#,
            span_ids.to_vec(),
        ),
        (
            r#"{ name = "checkout" } >> { status = error }"#,
            child.clone(),
        ),
        (
            r#"{ resource.service.name = "checkout" } | count() > 1"#,
            counted,
        ),
        (
            r#"{ resource.service.name = "checkout" && name = "checkout" && duration >= 500ms } >> { .db.system = "postgresql" && status = error && duration > 100ms && duration < 200ms }"#,
            child,
        ),
        (
            r#"{ resource.service.name = "checkout" && .http.route = "/checkout" && duration >= 500ms }"#,
            root,
        ),
        (r#"{ resource.service.name = "missing" }"#, vec![]),
    ] {
        let expected = ids
            .iter()
            .map(|id| (hex::encode([1; 16]), hex::encode([*id; 8])))
            .collect::<Vec<_>>();
        cases.push(("/api/search", query, json!(expected)));
    }
    if !span_ids.is_empty() {
        // The root starts exactly on the first right-closed bucket boundary;
        // the child starts 100ms later, so it belongs to the following bucket.
        let series = span_ids.iter().map(|id| {
            let (name, counts) = if *id == 1 { ("checkout", [1.0, 0.0]) } else { ("database", [0.0, 1.0]) };
            json!({"labels":[{"key":"name","value":{"stringValue":name}}],
                "promLabels":format!("{{name=\"{name}\"}}"),
                "samples":[{"timestampMs":"1700000000000","value":counts[0]}, {"timestampMs":"1700000001000","value":counts[1]}],
                "exemplars":[]})
        }).collect::<Vec<_>>();
        cases.push(("/api/metrics/query_range", r#"{ resource.service.name = "checkout" && duration > 0ns } | count_over_time() by(name) with(exemplars=false)"#, json!(series)));
    }
    for (path, query, expected) in cases {
        let result = wait_trace_query(deployment, container, tenant, path, query, &expected).await;
        evidence.push(
            json!({"phase":phase, "tenant":tenant, "path":path, "query":query,
            "expected":expected, "actual":result.as_ref().ok(),
            "status":if result.is_ok() {"matched"} else {"mismatch"},
            "error":result.as_ref().err().map(ToString::to_string)}),
        );
        if let Some(output) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
            let output = std::path::PathBuf::from(output);
            std::fs::create_dir_all(&output)?;
            std::fs::write(
                output.join(filename),
                serde_json::to_vec_pretty(&json!({"cases":evidence}))?,
            )?;
        }
        result?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn tag_scopes_and_typed_values_survive_storage_and_restart() -> TestResult {
    tags(false).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn tag_names_with_special_characters_survive_storage_and_restart() -> TestResult {
    tags(true).await
}
async fn tags(special: bool) -> TestResult {
    let deployment = Deployment::start(None).await?;
    let mut data = input(1, &[1], "checkout");
    let key = if special {
        "http/route.with-hyphen"
    } else {
        "http.route"
    };
    data.resource_spans[0].scope_spans[0].spans[0].attributes = vec![kv(key, "/checkout")];
    let mut rows = expected(1, &[1], "checkout");
    rows[0]["span"]["attributes"][0]["key"] = json!(key);
    deployment.push(TENANT, data).await?;
    deployment.trace(&deployment.hot, TENANT, 1, &rows).await?;
    let cold = deployment.stored(TENANT, 1, &rows).await?;
    for container in [&deployment.hot, &cold] {
        let result = deployment
            .get(
                container,
                TENANT,
                "/api/v2/search/tags",
                &[
                    ("scope", "span"),
                    ("start", "1700000000"),
                    ("end", "1700000001"),
                ],
            )
            .await?;
        assert!(result["scopes"] == json!([{"name":"span","tags":[key]}]));
        let result = deployment
            .get(
                container,
                TENANT,
                "/api/v2/search/tags",
                &[
                    ("scope", "resource"),
                    ("start", "1700000000"),
                    ("end", "1700000001"),
                ],
            )
            .await?;
        assert!(result["scopes"] == json!([{"name":"resource","tags":["service.name"]}]));
        let tag = format!("span.{key}");
        let path = format!(
            "/api/v2/search/tag/{}/values",
            url::form_urlencoded::byte_serialize(tag.as_bytes()).collect::<String>()
        );
        let result = deployment
            .get(
                container,
                TENANT,
                &path,
                &[("start", "1700000000"), ("end", "1700000001")],
            )
            .await?;
        assert!(result["tagValues"] == json!([{"type":"string","value":"/checkout"}]));
        let result = deployment
            .get(
                container,
                TENANT,
                "/api/search/tag/service.name/values",
                &[("start", "1700000000"), ("end", "1700000001")],
            )
            .await?;
        assert!(result["tagValues"] == json!(["checkout"]));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn tenants_keep_the_same_trace_id_isolated_across_restart() -> TestResult {
    let deployment = Deployment::start(None).await?;
    deployment.push(TENANT, input(1, &[1], "checkout")).await?;
    deployment.push("tenant-b", input(1, &[1], "other")).await?;
    let rows = expected(1, &[1], "checkout");
    deployment.trace(&deployment.hot, TENANT, 1, &rows).await?;
    deployment
        .trace(&deployment.hot, "tenant-b", 1, &expected(1, &[1], "other"))
        .await?;
    let mut evidence = Vec::new();
    let filename = "tempo-tenant-query-transitions.json";
    check_trace_queries(
        &deployment,
        &deployment.hot,
        TENANT,
        &[1],
        "hot-only",
        &mut evidence,
        filename,
    )
    .await?;
    check_trace_queries(
        &deployment,
        &deployment.hot,
        "tenant-b",
        &[],
        "hot-colliding-tenant-negative",
        &mut evidence,
        filename,
    )
    .await?;
    let cold = deployment.stored(TENANT, 1, &rows).await?;
    deployment
        .trace(&cold, "tenant-b", 1, &expected(1, &[1], "other"))
        .await?;
    for (container, phase) in [(&deployment.hot, "hot-cold-overlap"), (&cold, "restart")] {
        check_trace_queries(
            &deployment,
            container,
            TENANT,
            &[1],
            phase,
            &mut evidence,
            filename,
        )
        .await?;
        check_trace_queries(
            &deployment,
            container,
            "tenant-b",
            &[],
            phase,
            &mut evidence,
            filename,
        )
        .await?;
        check_trace_queries(
            &deployment,
            container,
            "tenant-c",
            &[],
            phase,
            &mut evidence,
            filename,
        )
        .await?;
        deployment.missing(container, "tenant-c", 1).await?;
        for (tenant, service) in [(TENANT, "checkout"), ("tenant-b", "other")] {
            let result = deployment
                .get(
                    container,
                    tenant,
                    "/api/v2/search/tag/resource.service.name/values",
                    &[("start", "1700000000"), ("end", "1700000001")],
                )
                .await?;
            assert!(result["tagValues"] == json!([{"type":"string","value":service}]));
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn otlp_ingest_limit_rejects_a_batch_without_leaking_spans() -> TestResult {
    let deployment = Deployment::start(Some(
        "overrides:\n  tenant-a:\n    max_spans_per_trace: 1\n",
    ))
    .await?;
    let body = input(9, &[1, 2], "rejected").encode_to_vec();
    let response = deployment
        .client
        .post(format!(
            "{}/v1/traces",
            base_url(&deployment.distributor, 4318).await?
        ))
        .header("X-Scope-OrgID", TENANT)
        .header("Content-Type", "application/x-protobuf")
        .body(body)
        .send()
        .await?;
    assert!(response.status() == StatusCode::BAD_REQUEST);
    let mut client =
        TraceServiceClient::connect(base_url(&deployment.distributor, 4317).await?).await?;
    let mut request = tonic::Request::new(ExportTraceServiceRequest {
        resource_spans: input(9, &[1, 2], "rejected").resource_spans,
    });
    request
        .metadata_mut()
        .insert("x-scope-orgid", TENANT.parse()?);
    let error = client
        .export(request)
        .await
        .expect_err("trace exceeds tenant limit");
    assert!(error.code() == tonic::Code::ResourceExhausted);
    deployment.push(TENANT, input(1, &[1], "checkout")).await?;
    deployment
        .push("tenant-b", input(9, &[1, 2], "rejected"))
        .await?;
    let rows = expected(1, &[1], "checkout");
    deployment.trace(&deployment.hot, TENANT, 1, &rows).await?;
    let cold = deployment.stored(TENANT, 1, &rows).await?;
    for container in [&deployment.hot, &cold] {
        deployment.missing(container, TENANT, 9).await?;
        deployment
            .trace(container, "tenant-b", 9, &expected(9, &[1, 2], "rejected"))
            .await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn single_binary_shutdown_drains_an_in_flight_trace_to_storage() -> TestResult {
    let deployment = Deployment::start(None).await?;
    deployment.distributor.stop().await?;
    deployment.hot.stop().await?;
    let all = deployment
        .role("all", &["--block-builder-flush-max-records=10000".into()])
        .await?;
    let cold = deployment.role("querier", &[]).await?;
    accepted(
        deployment
            .client
            .post(format!("{}/v1/traces", base_url(&all, 4318).await?))
            .header("X-Scope-OrgID", TENANT)
            .header("Content-Type", "application/x-protobuf")
            .body(input(1, &[1, 2], "checkout").encode_to_vec())
            .send()
            .await?,
    )
    .await?;
    let rows = expected(1, &[1, 2], "checkout");
    deployment.trace(&all, TENANT, 1, &rows).await?;
    deployment.missing(&cold, TENANT, 1).await?;
    all.stop().await?;
    cold.stop().await?;
    let restarted = deployment.role("querier", &[]).await?;
    deployment.trace(&restarted, TENANT, 1, &rows).await
}
