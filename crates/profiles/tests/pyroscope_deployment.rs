//! Pyroscope scenarios against deployed profile roles, the broker WAL, and `MinIO`.
//! Fixed profiles supply exact expected stack values. See `pyroscope_deployment.md`.

use std::{collections::BTreeMap, io::Write as _, os::unix::fs::MetadataExt as _, time::Duration};

use assert2::assert;
use flate2::{Compression, write::GzEncoder};
use krabka_pprof::proto;
use krabka_profiles::wire::pb;
use prost::Message as _;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt as _,
    core::{ContainerPort, Mount},
};

#[path = "../../metrics-service/tests/support/container_deployment.rs"]
mod container_deployment;
use self::container_deployment::{
    TestResult, base_url, deployment_network, image, start, start_broker, start_minio,
    wait_until_ready,
};

const PORT: u16 = 4040;
const ADMIN: u16 = 9404;
const TIMEOUT: Duration = Duration::from_secs(45);
const TENANT: &str = "tenant-a";
const START: i64 = 1_700_000_000_000;
const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
const OTLP_TYPE: &str = "cpu:cpu:nanoseconds:cpu:nanoseconds";
const SELECTOR: &str = r#"{service_name="checkout"}"#;
const PUSH: &str = "/push.v1.PusherService/Push";
const QUERY: &str = "/querier.v1.QuerierService";

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
    async fn start() -> TestResult<Self> {
        let broker_data = tempfile::tempdir()?;
        let data = tempfile::tempdir()?;
        let network = deployment_network("profiles", broker_data.path())?;
        let broker = start_broker(broker_data.path(), &network).await?;
        let minio = start_minio(&network, "profiles").await?;
        let distributor = role(&network, data.path(), "distributor", false).await?;
        let hot = role(&network, data.path(), "querier", false).await?;
        let deployment = Self {
            distributor,
            hot,
            _minio: minio,
            _broker: broker,
            data,
            _broker_data: broker_data,
            network,
            client: Client::builder().timeout(Duration::from_secs(5)).build()?,
        };
        deployment.ready(&deployment.distributor, PORT).await?;
        deployment.ready(&deployment.hot, PORT).await?;
        Ok(deployment)
    }

    async fn ready(&self, container: &ContainerAsync<GenericImage>, port: u16) -> TestResult {
        wait_until_ready(&self.client, container, port).await
    }

    async fn role(&self, target: &str, cold: bool) -> TestResult<ContainerAsync<GenericImage>> {
        let container = role(&self.network, self.data.path(), target, cold).await?;
        self.ready(
            &container,
            if target == "block-builder" {
                ADMIN
            } else {
                PORT
            },
        )
        .await?;
        Ok(container)
    }

    async fn request(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        path: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> TestResult<reqwest::Response> {
        Ok(self
            .client
            .post(format!("{}{path}", base_url(container, PORT).await?))
            .header("X-Scope-OrgID", tenant)
            .header("Content-Type", content_type)
            .header("Connect-Protocol-Version", "1")
            .body(body)
            .send()
            .await?)
    }

    async fn rpc(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        method: &str,
        body: Value,
    ) -> TestResult<Value> {
        let response = self
            .request(
                container,
                tenant,
                &format!("{QUERY}/{method}"),
                "application/json",
                serde_json::to_vec(&body)?,
            )
            .await?;
        let status = response.status();
        let body = response.text().await?;
        assert!(status == StatusCode::OK, "{method}: {status}: {body}");
        Ok(serde_json::from_str(&body)?)
    }

    async fn profile(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        profile_type: &str,
        selector: &str,
    ) -> TestResult<proto::Profile> {
        let request = pb::querier::v1::SelectMergeProfileRequest {
            profile_type_id: profile_type.into(),
            label_selector: selector.into(),
            start: START - 1000,
            end: START + 10_000,
            ..Default::default()
        };
        let response = self
            .request(
                container,
                tenant,
                &format!("{QUERY}/SelectMergeProfile"),
                "application/proto",
                request.encode_to_vec(),
            )
            .await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        assert!(
            status == StatusCode::OK,
            "profile: {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
        Ok(proto::Profile::decode(bytes)?)
    }

    async fn stacks(
        &self,
        container: &ContainerAsync<GenericImage>,
        tenant: &str,
        profile_type: &str,
        selector: &str,
        expected: &BTreeMap<Vec<String>, Vec<i64>>,
    ) -> TestResult {
        let mut last = BTreeMap::new();
        let result = tokio::time::timeout(TIMEOUT, async {
            loop {
                let profile = self
                    .profile(container, tenant, profile_type, selector)
                    .await?;
                last = collapsed(&profile);
                if &last == expected {
                    assert!(
                        profile
                            .sample_type
                            .iter()
                            .map(|kind| (text(&profile, kind.r#type), text(&profile, kind.unit)))
                            .collect::<Vec<_>>()
                            == vec![("cpu", "nanoseconds")]
                    );
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        assert!(result.is_ok(), "stacks: {last:?}, expected {expected:?}");
        result??;
        Ok(())
    }

    async fn push(
        &self,
        tenant: &str,
        time_ms: i64,
        service: &str,
        values: &[i64],
        binary: bool,
    ) -> TestResult {
        let profile = profile(time_ms, values);
        let request = push_request(service, gzip(&profile.encode_to_vec())?);
        let (content_type, body) = if binary {
            ("application/proto", request.encode_to_vec())
        } else {
            ("application/json", serde_json::to_vec(&request)?)
        };
        accepted(
            self.request(&self.distributor, tenant, PUSH, content_type, body)
                .await?,
        )
        .await
    }

    async fn stored(
        &self,
        tenant: &str,
        profile_type: &str,
        selector: &str,
        expected: &BTreeMap<Vec<String>, Vec<i64>>,
    ) -> TestResult<ContainerAsync<GenericImage>> {
        let cold = self.role("querier", true).await?;
        // The cold tail reads an empty topic, so it cannot replay the input.
        assert!(collapsed(&self.profile(&cold, tenant, profile_type, selector).await?).is_empty());
        let builder = self.role("block-builder", false).await?;
        self.stacks(&cold, tenant, profile_type, selector, expected)
            .await?;
        builder.stop().await?;
        cold.stop().await?;
        let restarted = self.role("querier", true).await?;
        self.stacks(&restarted, tenant, profile_type, selector, expected)
            .await?;
        Ok(restarted)
    }
}

async fn role(
    network: &str,
    directory: &std::path::Path,
    target: &str,
    cold: bool,
) -> TestResult<ContainerAsync<GenericImage>> {
    // Each process tails independently. Reusing a stopped process's group can
    // wait for its old member lease, obscuring the storage restart check.
    static NEXT_GROUP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let group = NEXT_GROUP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut command = vec![
        "krabka-profiles".into(),
        format!("--target={target}"),
        format!("--bootstrap={network}-broker:9092"),
        "--object-store-url=s3://profiles/profiles".into(),
        "--index-refresh-interval=100ms".into(),
        "--wal-poll-timeout=100ms".into(),
        "--hot-store-max-age=1000000h".into(),
        format!("--query-wal-tail-group-id=deployment-{target}-{cold}-{group}"),
        format!(
            "--block-builder-flush-records={}",
            if target == "all" { 10_000 } else { 1 }
        ),
        format!(
            "--block-builder-flush-max-age={}",
            if target == "all" { "30s" } else { "100ms" }
        ),
        "--compactor-interval=1h".into(),
    ];
    if cold {
        command.push(format!(
            "--wal-topic={}",
            krabka_observability::topic_contract::METRICS_WAL_TOPIC
        ));
    }
    let metadata = directory.metadata()?;
    start(
        image("KRABKA")
            .with_exposed_port(ContainerPort::Tcp(PORT))
            .with_exposed_port(ContainerPort::Tcp(ADMIN))
            .with_network(network)
            .with_user(format!("{}:{}", metadata.uid(), metadata.gid()))
            .with_mount(Mount::bind_mount(directory.to_string_lossy(), "/data"))
            .with_env_var("AWS_ACCESS_KEY_ID", "krabkaprofiles")
            .with_env_var("AWS_SECRET_ACCESS_KEY", "krabkaprofiles")
            .with_env_var("AWS_ENDPOINT_URL", format!("http://{network}-minio:9000"))
            .with_env_var("AWS_REGION", "us-east-1")
            .with_env_var("AWS_ALLOW_HTTP", "true")
            .with_env_var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")
            .with_env_var("AWS_EC2_METADATA_DISABLED", "true")
            .with_cmd(command),
    )
    .await
}

async fn accepted(response: reqwest::Response) -> TestResult {
    let status = response.status();
    let body = response.text().await?;
    assert!(status == StatusCode::OK, "ingest: {status}: {body}");
    Ok(())
}

fn gzip(bytes: &[u8]) -> TestResult<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

fn profile(time_ms: i64, values: &[i64]) -> proto::Profile {
    proto::Profile {
        sample_type: vec![proto::ValueType { r#type: 1, unit: 2 }],
        sample: values
            .iter()
            .enumerate()
            .map(|(i, value)| proto::Sample {
                location_id: if i == 0 { vec![2, 1] } else { vec![1] },
                value: vec![*value],
                label: vec![],
            })
            .collect(),
        mapping: vec![proto::Mapping {
            id: 1,
            symbolization: proto::MappingSymbolization::from_parts((true, true, true, false)),
            ..Default::default()
        }],
        location: (1_u64..=2)
            .map(|id| proto::Location {
                id,
                mapping_id: 1,
                address: id * 4096,
                line: vec![proto::Line {
                    function_id: id,
                    line: i64::try_from(id).expect("small ID") * 10,
                    column: 0,
                }],
                is_folded: false,
            })
            .collect(),
        function: (1_u64..=2)
            .map(|id| proto::Function {
                id,
                name: i64::try_from(id).expect("small ID") + 2,
                system_name: i64::try_from(id).expect("small ID") + 2,
                filename: 5,
                start_line: i64::try_from(id).expect("small ID"),
            })
            .collect(),
        string_table: [
            "",
            "cpu",
            "nanoseconds",
            "main.work",
            "main.hotloop",
            "app.go",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
        time_nanos: time_ms * 1_000_000,
        duration_nanos: 1_000_000_000,
        period_type: Some(proto::ValueType { r#type: 1, unit: 2 }),
        period: 10_000_000,
        ..Default::default()
    }
}

fn push_request(service: &str, body: Vec<u8>) -> pb::push::v1::PushRequest {
    pb::push::v1::PushRequest {
        series: vec![pb::push::v1::RawProfileSeries {
            labels: [
                ("__name__", "process_cpu"),
                ("service_name", service),
                ("env", "deployment"),
            ]
            .into_iter()
            .map(|(name, value)| pb::types::v1::LabelPair {
                name: name.into(),
                value: value.into(),
            })
            .collect(),
            samples: vec![pb::push::v1::RawSample {
                raw_profile: body,
                id: "deployment-profile".into(),
            }],
            ..Default::default()
        }],
    }
}

fn text(profile: &proto::Profile, index: i64) -> &str {
    &profile.string_table[usize::try_from(index).expect("nonnegative string index")]
}

// Match upstream's StackCollapseProto oracle: compare every stack and its values.
fn collapsed(profile: &proto::Profile) -> BTreeMap<Vec<String>, Vec<i64>> {
    let mut rows: BTreeMap<Vec<String>, Vec<i64>> = BTreeMap::new();
    for sample in &profile.sample {
        let stack = sample
            .location_id
            .iter()
            .flat_map(|id| {
                profile
                    .location
                    .iter()
                    .find(|location| location.id == *id)
                    .expect("sample location")
                    .line
                    .iter()
                    .map(|line| {
                        let function = profile
                            .function
                            .iter()
                            .find(|function| function.id == line.function_id)
                            .expect("line function");
                        text(profile, function.name).to_string()
                    })
            })
            .collect();
        let row = rows
            .entry(stack)
            .or_insert_with(|| vec![0; sample.value.len()]);
        assert!(row.len() == sample.value.len());
        for (sum, value) in row.iter_mut().zip(&sample.value) {
            *sum += value;
        }
    }
    rows
}

fn expected(values: &[i64]) -> BTreeMap<Vec<String>, Vec<i64>> {
    let mut rows = BTreeMap::new();
    if let Some(value) = values.first() {
        rows.insert(
            vec!["main.hotloop".into(), "main.work".into()],
            vec![*value],
        );
    }
    if let Some(value) = values.get(1) {
        rows.insert(vec!["main.work".into()], vec![*value]);
    }
    rows
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn push_json_survives_storage_and_restart() -> TestResult {
    push(false).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn push_binary_connect_survives_storage_and_restart() -> TestResult {
    push(true).await
}
async fn push(binary: bool) -> TestResult {
    let deployment = Deployment::start().await?;
    deployment
        .push(TENANT, START, "checkout", &[100, 40], binary)
        .await?;
    let rows = expected(&[100, 40]);
    deployment
        .stacks(&deployment.hot, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    deployment
        .stored(TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn legacy_pprof_survives_storage_and_restart() -> TestResult {
    legacy("pprof", false).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn legacy_gzip_pprof_survives_storage_and_restart() -> TestResult {
    legacy("pprof", true).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn speedscope_survives_storage_and_restart() -> TestResult {
    legacy("speedscope", false).await
}
async fn legacy(format: &str, compressed: bool) -> TestResult {
    let deployment = Deployment::start().await?;
    let (content_type, mut body) = if format == "pprof" {
        (
            "binary/octet-stream",
            profile(START, &[100, 40]).encode_to_vec(),
        )
    } else {
        (
            "application/json",
            serde_json::to_vec(&json!({
                "$schema":"https://www.speedscope.app/file-format-schema.json",
                "shared":{"frames":[{"name":"main.work"},{"name":"main.hotloop"}]},
                "profiles":[{"type":"sampled","name":"cpu","unit":"nanoseconds","startValue":0,"endValue":140,"samples":[[0,1],[0]],"weights":[100,40]}]
            }))?,
        )
    };
    if compressed {
        body = gzip(&body)?;
    }
    let mut url = url::Url::parse(&format!(
        "{}/ingest",
        base_url(&deployment.distributor, PORT).await?
    ))?;
    url.query_pairs_mut().extend_pairs([
        ("name", "checkout"),
        ("format", format),
        ("from", "1700000000"),
        ("until", "1700000000"),
        ("sampleRate", "100"),
        ("units", "nanoseconds"),
    ]);
    accepted(
        deployment
            .client
            .post(url)
            .header("X-Scope-OrgID", TENANT)
            .header("Content-Type", content_type)
            .body(body)
            .send()
            .await?,
    )
    .await?;
    // Speedscope's legacy CPU mapping converts its counts by the sample period.
    let rows = if format == "speedscope" {
        expected(&[1_000_000_000, 400_000_000])
    } else {
        expected(&[100, 40])
    };
    deployment
        .stacks(&deployment.hot, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    deployment
        .stored(TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn otlp_http_protobuf_survives_storage_and_restart() -> TestResult {
    otlp(false, false, false).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn otlp_http_json_survives_storage_and_restart() -> TestResult {
    otlp(true, false, false).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn otlp_http_gzip_survives_storage_and_restart() -> TestResult {
    otlp(false, true, false).await
}
#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn otlp_binary_connect_survives_storage_and_restart() -> TestResult {
    otlp(false, false, true).await
}
async fn otlp(json: bool, compressed: bool, connect: bool) -> TestResult {
    let deployment = Deployment::start().await?;
    let request = otlp_request();
    let mut body = if json {
        serde_json::to_vec(&request)?
    } else {
        request.encode_to_vec()
    };
    if compressed {
        body = gzip(&body)?;
    }
    let path = if connect {
        "/opentelemetry.proto.collector.profiles.v1development.ProfilesService/Export"
    } else {
        "/v1development/profiles"
    };
    let content_type = if json {
        "application/json"
    } else if connect {
        "application/proto"
    } else {
        "application/x-protobuf"
    };
    let mut request = deployment
        .client
        .post(format!(
            "{}{path}",
            base_url(&deployment.distributor, PORT).await?
        ))
        .header("X-Scope-OrgID", TENANT)
        .header("Content-Type", content_type)
        .header("Connect-Protocol-Version", "1")
        .body(body);
    if compressed {
        request = request.header("Content-Encoding", "gzip");
    }
    accepted(request.send().await?).await?;
    let rows = expected(&[100, 40]);
    deployment
        .stacks(&deployment.hot, TENANT, OTLP_TYPE, SELECTOR, &rows)
        .await?;
    deployment
        .stored(TENANT, OTLP_TYPE, SELECTOR, &rows)
        .await?;
    Ok(())
}

fn otlp_request() -> pb::otlp_profiles::ExportProfilesServiceRequest {
    serde_json::from_value(json!({
        "dictionary": {
            "stringTable":["","cpu","nanoseconds","main.work","main.hotloop","app.go"],
            "mappingTable":[{"filenameStrindex":5}],
            "functionTable":[
                {"nameStrindex":3,"systemNameStrindex":3,"filenameStrindex":5,"startLine":"1"},
                {"nameStrindex":4,"systemNameStrindex":4,"filenameStrindex":5,"startLine":"2"}],
            "locationTable":[
                {"address":"4096","lines":[{"functionIndex":0,"line":"10"}]},
                {"address":"8192","lines":[{"functionIndex":1,"line":"20"}]}],
            "stackTable":[{"locationIndices":[1,0]},{"locationIndices":[0]}]
        },
        "resourceProfiles":[{
            "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},
            "scopeProfiles":[{"profiles":[{
                "sampleType":{"typeStrindex":1,"unitStrindex":2},
                "periodType":{"typeStrindex":1,"unitStrindex":2},"period":"10000000",
                "timeUnixNano":"1700000000000000000","durationNano":"1000000000",
                "samples":[{"stackIndex":0,"values":["100"]},{"stackIndex":1,"values":["40"]}]
            }]}]
        }]
    }))
    .expect("fixed OTLP fixture")
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn profile_metadata_and_selectors_survive_storage_and_restart() -> TestResult {
    let deployment = Deployment::start().await?;
    deployment
        .push(TENANT, START, "checkout", &[100, 40], false)
        .await?;
    let rows = expected(&[100, 40]);
    deployment
        .stacks(&deployment.hot, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    let cold = deployment
        .stored(TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    for container in [&deployment.hot, &cold] {
        for range in [json!({}), json!({"start":START-1000,"end":START+1000})] {
            let result = deployment
                .rpc(container, TENANT, "ProfileTypes", range)
                .await?;
            assert!(
                result
                    == json!({"profileTypes":[{"ID":PROFILE_TYPE,"name":"process_cpu","sampleType":"cpu","sampleUnit":"nanoseconds","periodType":"cpu","periodUnit":"nanoseconds"}]})
            );
        }
        let result = deployment
            .rpc(
                container,
                TENANT,
                "LabelNames",
                json!({"start":START-1000,"end":START+1000}),
            )
            .await?;
        assert!(
            result["names"]
                == json!([
                    "__name__",
                    "__period_type__",
                    "__period_unit__",
                    "__profile_type__",
                    "__service_name__",
                    "__type__",
                    "__unit__",
                    "env",
                    "service_name"
                ])
        );
        let result = deployment
            .rpc(
                container,
                TENANT,
                "LabelValues",
                json!({"name":"service_name","start":START-1000,"end":START+1000}),
            )
            .await?;
        assert!(result == json!({"names":["checkout"]}));
        for range in [json!({}), json!({"start":START-1000,"end":START+1000})] {
            let mut body = range;
            body["labelNames"] = json!(["__profile_type__", "service_name"]);
            let result = deployment.rpc(container, TENANT, "Series", body).await?;
            assert!(
                result
                    == json!({"labelsSet":[{"labels":[{"name":"__profile_type__","value":PROFILE_TYPE},{"name":"service_name","value":"checkout"}]}]})
            );
        }
        let result = deployment
            .rpc(
                container,
                TENANT,
                "ProfileTypes",
                json!({"start":START+20_000,"end":START+30_000}),
            )
            .await?;
        let result: pb::querier::v1::ProfileTypesResponse = serde_json::from_value(result)?;
        assert!(result == pb::querier::v1::ProfileTypesResponse::default());
        deployment
            .stacks(
                container,
                TENANT,
                PROFILE_TYPE,
                r#"{service_name="missing"}"#,
                &BTreeMap::new(),
            )
            .await?;
        deployment
            .stacks(
                container,
                TENANT,
                PROFILE_TYPE,
                r#"{service_name=~"check.*",env!="other"}"#,
                &rows,
            )
            .await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn tenants_keep_identical_profile_labels_isolated_across_restart() -> TestResult {
    let deployment = Deployment::start().await?;
    deployment
        .push(TENANT, START, "checkout", &[100, 40], false)
        .await?;
    deployment
        .push("tenant-b", START, "checkout", &[7, 3], false)
        .await?;
    let rows = expected(&[100, 40]);
    deployment
        .stacks(&deployment.hot, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    let cold = deployment
        .stored(TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    for container in [&deployment.hot, &cold] {
        deployment
            .stacks(
                container,
                "tenant-b",
                PROFILE_TYPE,
                SELECTOR,
                &expected(&[7, 3]),
            )
            .await?;
        deployment
            .stacks(
                container,
                "tenant-c",
                PROFILE_TYPE,
                SELECTOR,
                &BTreeMap::new(),
            )
            .await?;
        let result = deployment
            .rpc(
                container,
                "tenant-c",
                "LabelValues",
                json!({"name":"service_name","start":START-1000,"end":START+1000}),
            )
            .await?;
        let result: pb::types::v1::LabelValuesResponse = serde_json::from_value(result)?;
        assert!(result == pb::types::v1::LabelValuesResponse::default());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn separate_batches_merge_without_double_counting_hot_and_cold() -> TestResult {
    let deployment = Deployment::start().await?;
    deployment
        .push(TENANT, START, "checkout", &[100, 40], false)
        .await?;
    deployment
        .push(TENANT, START + 2000, "checkout", &[5, 2], true)
        .await?;
    let rows = expected(&[105, 42]);
    deployment
        .stacks(&deployment.hot, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    let cold = deployment
        .stored(TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    // The hot querier also sees the blocks now; overlapping tiers count once.
    deployment
        .stacks(&deployment.hot, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    deployment
        .stacks(&cold, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn malformed_pprof_is_rejected_without_wal_data() -> TestResult {
    let deployment = Deployment::start().await?;
    for corruption in 0..5 {
        let mut bad = profile(START, &[999]);
        match corruption {
            0 => bad.sample_type[0].r#type = 100_500,
            1 => bad.function[0].name = 100_500,
            2 => bad.mapping[0].filename = 100_500,
            3 => bad.sample[0].label.push(proto::Label {
                key: 100_500,
                ..Default::default()
            }),
            _ => bad.string_table[0] = "not empty".into(),
        }
        let request = push_request("rejected", gzip(&bad.encode_to_vec())?);
        let response = deployment
            .request(
                &deployment.distributor,
                TENANT,
                PUSH,
                "application/json",
                serde_json::to_vec(&request)?,
            )
            .await?;
        let status = response.status();
        let error: Value = response.json().await?;
        assert!(
            status == StatusCode::BAD_REQUEST,
            "corruption {corruption}: {error}"
        );
        assert!(error["code"] == "invalid_argument");
    }
    deployment
        .push(TENANT, START, "checkout", &[100, 40], false)
        .await?;
    let rows = expected(&[100, 40]);
    deployment
        .stacks(&deployment.hot, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    let cold = deployment
        .stored(TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    for container in [&deployment.hot, &cold] {
        deployment
            .stacks(
                container,
                TENANT,
                PROFILE_TYPE,
                r#"{service_name="rejected"}"#,
                &BTreeMap::new(),
            )
            .await?;
        let result = deployment
            .rpc(
                container,
                TENANT,
                "LabelValues",
                json!({"name":"service_name","start":START-1000,"end":START+1000}),
            )
            .await?;
        assert!(result == json!({"names":["checkout"]}));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn query_status_codes_match_upstream_cases() -> TestResult {
    let deployment = Deployment::start().await?;
    deployment
        .push(TENANT, START, "checkout", &[100, 40], false)
        .await?;
    deployment
        .stacks(
            &deployment.hot,
            TENANT,
            PROFILE_TYPE,
            SELECTOR,
            &expected(&[100, 40]),
        )
        .await?;
    let query = format!("{QUERY}/SelectMergeProfile");
    let valid = json!({"profileTypeID":PROFILE_TYPE,"labelSelector":SELECTOR,"start":START-1000,"end":START+1000});
    let cases = [
        (
            Method::POST,
            "application/json",
            serde_json::to_vec(&valid)?,
            StatusCode::OK,
        ),
        (
            Method::POST,
            "application/json",
            b"{broken".to_vec(),
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::POST,
            "text/plain",
            serde_json::to_vec(&valid)?,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            Method::GET,
            "application/json",
            vec![],
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            Method::POST,
            "application/json",
            serde_json::to_vec(
                &json!({"profileTypeID":PROFILE_TYPE,"labelSelector":"{broken","start":START-1000,"end":START+1000}),
            )?,
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::POST,
            "application/json",
            serde_json::to_vec(
                &json!({"profileTypeID":"bad","labelSelector":"{}","start":START-1000,"end":START+1000}),
            )?,
            StatusCode::BAD_REQUEST,
        ),
    ];
    for (method, content_type, body, expected) in cases {
        let response = deployment
            .client
            .request(
                method.clone(),
                format!("{}{query}", base_url(&deployment.hot, PORT).await?),
            )
            .header("X-Scope-OrgID", TENANT)
            .header("Content-Type", content_type)
            .body(body)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        assert!(
            status == expected,
            "{method} {content_type}: {status}: {body}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and Bazel-loaded images"]
async fn single_binary_shutdown_drains_profiles_to_storage() -> TestResult {
    let deployment = Deployment::start().await?;
    deployment.distributor.stop().await?;
    deployment.hot.stop().await?;
    let all = deployment.role("all", false).await?;
    let cold = deployment.role("querier", true).await?;
    let request = push_request(
        "checkout",
        gzip(&profile(START, &[100, 40]).encode_to_vec())?,
    );
    accepted(
        deployment
            .request(
                &all,
                TENANT,
                PUSH,
                "application/json",
                serde_json::to_vec(&request)?,
            )
            .await?,
    )
    .await?;
    let rows = expected(&[100, 40]);
    deployment
        .stacks(&all, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await?;
    assert!(
        collapsed(
            &deployment
                .profile(&cold, TENANT, PROFILE_TYPE, SELECTOR)
                .await?
        )
        .is_empty()
    );
    all.stop().await?;
    cold.stop().await?;
    let restarted = deployment.role("querier", true).await?;
    deployment
        .stacks(&restarted, TENANT, PROFILE_TYPE, SELECTOR, &rows)
        .await
}
