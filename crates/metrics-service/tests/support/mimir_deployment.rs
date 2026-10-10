//! Mimir integration scenarios adapted to Krabka's deployed roles.
//!
//! Sources: `grafana/mimir/integration/{compactor,distributor,ooo_ingestion,
//! query_frontend_cache,querier_remote_read,otlp_ingestion}_test.go`.
//! Expected answers come from fixed inputs, not another Krabka query.

use std::time::Duration;

use assert2::assert;
use krabka_metrics::wire::pb;
use prost::Message as _;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, GenericImage, ImageExt as _};

use super::{
    ADMIN_PORT, DATA_PORT, Deployment, QUERY_TIMEOUT, RemoteWrite, START_MS, TestResult, base_url,
    s3_role, start,
};

fn labels(metric: &str, extra: &[(&str, &str)]) -> Vec<pb::v1::Label> {
    let mut labels: Vec<_> = std::iter::once(("__name__", metric))
        .chain(extra.iter().copied())
        .map(|(name, value)| pb::v1::Label {
            name: name.into(),
            value: value.into(),
        })
        .collect();
    labels.sort_by(|a, b| a.name.cmp(&b.name));
    labels
}

fn floats(metric: &str, samples: &[(i64, f64)], extra: &[(&str, &str)]) -> pb::v1::TimeSeries {
    pb::v1::TimeSeries {
        labels: labels(metric, extra),
        samples: samples
            .iter()
            .map(|&(timestamp, value)| pb::v1::Sample { timestamp, value })
            .collect(),
        ..Default::default()
    }
}

// Schema 0: two positive buckets (0.5, 1] and (1, 2], with 2 and 3 observations.
fn histogram(timestamp: i64, float: bool) -> pb::v1::Histogram {
    let mut histogram = pb::v1::Histogram {
        timestamp,
        schema: 0,
        sum: 8.0,
        positive_spans: vec![pb::v1::BucketSpan {
            offset: 0,
            length: 2,
        }],
        reset_hint: pb::v1::histogram::ResetHint::Gauge as i32,
        ..Default::default()
    };
    if float {
        histogram.count = Some(pb::v1::histogram::Count::CountFloat(5.0));
        histogram.zero_count = Some(pb::v1::histogram::ZeroCount::ZeroCountFloat(0.0));
        histogram.positive_counts = vec![2.0, 3.0];
    } else {
        histogram.count = Some(pb::v1::histogram::Count::CountInt(5));
        histogram.zero_count = Some(pb::v1::histogram::ZeroCount::ZeroCountInt(0));
        histogram.positive_deltas = vec![2, 1];
    }
    histogram
}

fn histograms(metric: &str, times: &[i64], float: bool) -> pb::v1::TimeSeries {
    pb::v1::TimeSeries {
        labels: labels(metric, &[]),
        histograms: times.iter().map(|&time| histogram(time, float)).collect(),
        ..Default::default()
    }
}

fn symbol(symbols: &mut Vec<String>, value: &str) -> u32 {
    if let Some(index) = symbols.iter().position(|symbol| symbol == value) {
        return u32::try_from(index).expect("small test symbol table");
    }
    symbols.push(value.to_owned());
    u32::try_from(symbols.len() - 1).expect("small test symbol table")
}

fn write_body(
    version: RemoteWrite,
    timeseries: Vec<pb::v1::TimeSeries>,
    metadata: Vec<pb::v1::MetricMetadata>,
) -> TestResult<Vec<u8>> {
    let body = match version {
        RemoteWrite::V1 => pb::v1::WriteRequest {
            timeseries,
            metadata,
        }
        .encode_to_vec(),
        RemoteWrite::V2 => {
            let mut symbols = vec![String::new()];
            let mut series = Vec::new();
            for ts in timeseries {
                let metric = ts
                    .labels
                    .iter()
                    .find(|label| label.name == "__name__")
                    .expect("named series")
                    .value
                    .clone();
                let labels_refs = ts
                    .labels
                    .iter()
                    .flat_map(|label| {
                        [
                            symbol(&mut symbols, &label.name),
                            symbol(&mut symbols, &label.value),
                        ]
                    })
                    .collect();
                let meta = metadata
                    .iter()
                    .find(|meta| meta.metric_family_name == metric)
                    .map(|meta| pb::v2::Metadata {
                        r#type: meta.r#type,
                        help_ref: symbol(&mut symbols, &meta.help),
                        unit_ref: symbol(&mut symbols, &meta.unit),
                    });
                series.push(pb::v2::TimeSeries {
                    labels_refs,
                    samples: ts
                        .samples
                        .into_iter()
                        .map(|s| pb::v2::Sample {
                            timestamp: s.timestamp,
                            value: s.value,
                            ..Default::default()
                        })
                        .collect(),
                    // Histogram fields 1..16 have the same protobuf numbers in v1 and v2.
                    histograms: ts
                        .histograms
                        .iter()
                        .map(|h| pb::v2::Histogram::decode(h.encode_to_vec().as_slice()))
                        .collect::<Result<_, _>>()?,
                    exemplars: ts
                        .exemplars
                        .into_iter()
                        .map(|e| pb::v2::Exemplar {
                            labels_refs: e
                                .labels
                                .iter()
                                .flat_map(|label| {
                                    [
                                        symbol(&mut symbols, &label.name),
                                        symbol(&mut symbols, &label.value),
                                    ]
                                })
                                .collect(),
                            timestamp: e.timestamp,
                            value: e.value,
                        })
                        .collect(),
                    metadata: meta,
                });
            }
            pb::v2::Request {
                symbols,
                timeseries: series,
            }
            .encode_to_vec()
        }
    };
    Ok(snap::raw::Encoder::new().compress_vec(&body)?)
}

async fn write(
    deployment: &Deployment,
    version: RemoteWrite,
    tenant: &str,
    series: Vec<pb::v1::TimeSeries>,
    metadata: Vec<pb::v1::MetricMetadata>,
) -> TestResult<reqwest::Response> {
    let base = base_url(&deployment.distributor, DATA_PORT).await?;
    let (content_type, version_header) = match version {
        RemoteWrite::V1 => ("application/x-protobuf", "0.1.0"),
        RemoteWrite::V2 => (
            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
            "2.0.0",
        ),
    };
    Ok(deployment
        .client
        .post(format!("{base}/api/v1/push"))
        .header("X-Scope-OrgID", tenant)
        .header("Content-Type", content_type)
        .header("Content-Encoding", "snappy")
        .header("X-Prometheus-Remote-Write-Version", version_header)
        .body(write_body(version, series, metadata)?)
        .send()
        .await?)
}

async fn accepted(response: reqwest::Response) -> TestResult {
    let status = response.status();
    let body = response.text().await?;
    assert!(status.is_success(), "push: HTTP {status}: {body}");
    Ok(())
}

fn seconds(milliseconds: i64) -> String {
    format!("{}.{:03}", milliseconds / 1000, milliseconds % 1000)
}

fn timestamp(milliseconds: i64) -> Value {
    if milliseconds % 1000 == 0 {
        return json!(milliseconds / 1000);
    }
    serde_json::from_str(&seconds(milliseconds)).expect("fixed positive timestamp")
}

fn api_url(base: &str, path: &str, params: &[(&str, String)]) -> TestResult<url::Url> {
    let mut url = url::Url::parse(&format!("{base}{path}"))?;
    url.query_pairs_mut()
        .extend_pairs(params.iter().map(|(name, value)| (*name, value.as_str())));
    Ok(url)
}

fn range_url(base: &str, query: &str, bounds: (i64, i64, i64)) -> TestResult<url::Url> {
    api_url(
        base,
        "/api/v1/query_range",
        &[
            ("query", query.into()),
            ("start", seconds(bounds.0)),
            ("end", seconds(bounds.1)),
            ("step", seconds(bounds.2)),
        ],
    )
}

async fn data(client: &Client, url: &url::Url, tenant: &str) -> TestResult<Value> {
    let response = client
        .get(url.clone())
        .header("X-Scope-OrgID", tenant)
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    assert!(status == StatusCode::OK, "{url}: HTTP {status}: {text}");
    let body: Value = serde_json::from_str(&text)?;
    assert!(body["status"] == "success", "{body}");
    Ok(body["data"].clone())
}

async fn wait_data(client: &Client, url: &url::Url, tenant: &str, expected: &Value) -> TestResult {
    let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
    loop {
        let actual = data(client, url, tenant).await?;
        if actual == *expected {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "{url} tenant {tenant}: expected {expected}, last result {actual}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn matrix(series: Value) -> Value {
    Value::Object(
        [
            ("resultType".into(), json!("matrix")),
            ("result".into(), series),
        ]
        .into_iter()
        .collect(),
    )
}

fn float_matrix(metric: &str, samples: &[(i64, i32)], extra: Value) -> Value {
    let Value::Object(mut metric_labels) = extra else {
        panic!("labels object");
    };
    metric_labels.insert("__name__".into(), json!(metric));
    matrix(
        json!([{"metric": metric_labels, "values": samples.iter().map(|&(time, value)| json!([timestamp(time), value.to_string()])).collect::<Vec<_>>() }]),
    )
}

fn histogram_matrix(metric: &str, times: &[i64]) -> Value {
    matrix(
        json!([{"metric": {"__name__": metric}, "histograms": times.iter().map(|&time| json!([timestamp(time), {"count": "5", "sum": "8", "buckets": [[0, "0.5", "1", "2"], [0, "1", "2", "3"]]}])).collect::<Vec<_>>() }]),
    )
}

async fn wait_metric(
    deployment: &Deployment,
    container: &ContainerAsync<GenericImage>,
    name: &str,
    minimum: f64,
) -> TestResult {
    let base = base_url(container, ADMIN_PORT).await?;
    let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
    loop {
        let text = deployment
            .client
            .get(format!("{base}/metrics"))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let value = text
            .lines()
            .filter_map(|line| line.strip_prefix(&format!("{name} ")))
            .filter_map(|value| value.parse::<f64>().ok())
            .sum::<f64>();
        if value >= minimum {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("{base}: {name} remained {value}, expected >= {minimum}").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn native_histograms_v1_survive_compaction_and_restart() -> TestResult {
    histogram_compaction(RemoteWrite::V1).await
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn native_histograms_v2_survive_compaction_and_restart() -> TestResult {
    histogram_compaction(RemoteWrite::V2).await
}

async fn histogram_compaction(version: RemoteWrite) -> TestResult {
    let deployment = Deployment::start().await?;
    let builder = deployment.builder_with_rows(1).await?;
    let cold = deployment.cold().await?;
    let hot_base = base_url(&deployment.hot, DATA_PORT).await?;
    let cold_base = base_url(&cold, DATA_PORT).await?;
    let times = [START_MS, START_MS + 1000];
    for time in times {
        let response = write(
            &deployment,
            version,
            "tenant-a",
            vec![
                histograms("integer_histogram", &[time], false),
                histograms("float_histogram", &[time], true),
            ],
            vec![],
        )
        .await?;
        if matches!(version, RemoteWrite::V2) {
            assert!(response.headers()["X-Prometheus-Remote-Write-Histograms-Written"] == "2");
            assert!(response.headers()["X-Prometheus-Remote-Write-Samples-Written"] == "0");
        }
        accepted(response).await?;
        // Wait for each write to reach a separate stored block before the next push.
        for metric in ["integer_histogram", "float_histogram"] {
            let expected = histogram_matrix(metric, &times[..=usize::from(time != START_MS)]);
            for base in [&hot_base, &cold_base] {
                wait_data(
                    &deployment.client,
                    &range_url(base, metric, (START_MS, time, 1000))?,
                    "tenant-a",
                    &expected,
                )
                .await?;
            }
        }
    }
    builder.stop_with_timeout(Some(30)).await?;
    let compactor = start(s3_role(&deployment.network, ADMIN_PORT).with_cmd([
        "krabka-metrics",
        "--target=compactor",
        "--object-store-url=s3://metrics",
        "--compactor-interval=100ms",
    ]))
    .await?;
    deployment.ready(&compactor, ADMIN_PORT).await?;
    // A nonzero output counter proves a merge ran, rather than merely a ready process.
    wait_metric(
        &deployment,
        &compactor,
        "krabka_metrics_compaction_blocks_total",
        1.0,
    )
    .await?;
    compactor.stop_with_timeout(Some(30)).await?;
    deployment.hot.stop_with_timeout(Some(30)).await?;
    cold.rm().await?;
    let reopened = deployment.cold().await?;
    let base = base_url(&reopened, DATA_PORT).await?;
    for metric in ["integer_histogram", "float_histogram"] {
        let url = range_url(&base, metric, (START_MS, START_MS + 1000, 1000))?;
        wait_data(
            &deployment.client,
            &url,
            "tenant-a",
            &histogram_matrix(metric, &times),
        )
        .await?;
        assert!(data(&deployment.client, &url, "tenant-b").await? == matrix(json!([])));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn metadata_and_exemplars_v1_survive_storage_and_restart() -> TestResult {
    metadata_and_exemplars(RemoteWrite::V1).await
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn metadata_and_exemplars_v2_survive_storage_and_restart() -> TestResult {
    metadata_and_exemplars(RemoteWrite::V2).await
}

async fn metadata_and_exemplars(version: RemoteWrite) -> TestResult {
    let deployment = Deployment::start().await?;
    let mut series = floats("requests_total", &[(START_MS, 11.0)], &[("job", "api")]);
    series.exemplars = vec![pb::v1::Exemplar {
        labels: vec![pb::v1::Label {
            name: "trace_id".into(),
            value: "0123456789abcdef".into(),
        }],
        timestamp: START_MS,
        value: 3.5,
    }];
    let metadata = vec![pb::v1::MetricMetadata {
        r#type: pb::v1::metric_metadata::MetricType::Counter as i32,
        metric_family_name: "requests_total".into(),
        help: "Requests served".into(),
        unit: "requests".into(),
    }];
    let response = write(&deployment, version, "tenant-a", vec![series], metadata).await?;
    if matches!(version, RemoteWrite::V2) {
        assert!(response.headers()["X-Prometheus-Remote-Write-Samples-Written"] == "1");
        assert!(response.headers()["X-Prometheus-Remote-Write-Exemplars-Written"] == "1");
    }
    accepted(response).await?;
    let expected_metadata = json!({"requests_total": [{"type": "counter", "help": "Requests served", "unit": "requests"}]});
    let expected_exemplars = json!([{"seriesLabels": {"__name__": "requests_total", "job": "api"}, "exemplars": [{"labels": {"trace_id": "0123456789abcdef"}, "value": "3.5", "timestamp": 1_700_000_000}]}]);
    let probes = [
        (
            "/api/v1/metadata",
            vec![("metric", "requests_total".into())],
            expected_metadata,
            json!({}),
        ),
        (
            "/api/v1/query_exemplars",
            vec![
                ("query", "requests_total".into()),
                ("start", seconds(START_MS)),
                ("end", seconds(START_MS)),
            ],
            expected_exemplars,
            json!([]),
        ),
    ];
    let hot = base_url(&deployment.hot, DATA_PORT).await?;
    for (path, params, expected, empty) in &probes {
        let url = api_url(&hot, path, params)?;
        wait_data(&deployment.client, &url, "tenant-a", expected).await?;
        assert!(data(&deployment.client, &url, "tenant-b").await? == *empty);
    }
    let builder = deployment.builder().await?;
    let cold = deployment.cold().await?;
    let base = base_url(&cold, DATA_PORT).await?;
    for (path, params, expected, _) in &probes {
        wait_data(
            &deployment.client,
            &api_url(&base, path, params)?,
            "tenant-a",
            expected,
        )
        .await?;
    }
    builder.stop_with_timeout(Some(30)).await?;
    deployment.hot.stop_with_timeout(Some(30)).await?;
    cold.rm().await?;
    let reopened = deployment.cold().await?;
    let base = base_url(&reopened, DATA_PORT).await?;
    for (path, params, expected, empty) in &probes {
        let url = api_url(&base, path, params)?;
        wait_data(&deployment.client, &url, "tenant-a", expected).await?;
        assert!(data(&deployment.client, &url, "tenant-b").await? == *empty);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn out_of_order_floats_and_histograms_obey_the_tenant_window() -> TestResult {
    let deployment = Deployment::with_overrides(Some(
        "overrides:\n  tenant-a:\n    out_of_order_time_window: 10m\n",
    ))
    .await?;
    let newest = START_MS + 60_000;
    let builder = deployment.builder().await?;
    let cold = deployment.cold().await?;
    for tenant in ["tenant-a", "tenant-b"] {
        for (time, expected_status) in [
            (newest, StatusCode::OK),
            (
                START_MS,
                if tenant == "tenant-a" {
                    StatusCode::OK
                } else {
                    StatusCode::BAD_REQUEST
                },
            ),
            (newest - 600_001, StatusCode::BAD_REQUEST),
        ] {
            for series in [
                floats("ooo_float", &[(time, 5.0)], &[]),
                histograms("ooo_integer_histogram", &[time], false),
                histograms("ooo_float_histogram", &[time], true),
            ] {
                let response =
                    write(&deployment, RemoteWrite::V1, tenant, vec![series], vec![]).await?;
                let status = response.status();
                let body = response.text().await?;
                assert!(
                    status == expected_status,
                    "{tenant} at {time}: {status}: {body}"
                );
            }
        }
    }
    for tenant in ["tenant-a", "tenant-b"] {
        let times: &[i64] = if tenant == "tenant-a" {
            &[START_MS, newest]
        } else {
            &[newest]
        };
        for container in [&deployment.hot, &cold] {
            let base = base_url(container, DATA_PORT).await?;
            for metric in ["ooo_float", "ooo_integer_histogram", "ooo_float_histogram"] {
                let expected = if metric == "ooo_float" {
                    float_matrix(
                        metric,
                        &times.iter().map(|&time| (time, 5)).collect::<Vec<_>>(),
                        json!({}),
                    )
                } else {
                    histogram_matrix(metric, times)
                };
                // Query before the oldest rejected timestamp too: rejected writes must never appear.
                wait_data(
                    &deployment.client,
                    &range_url(&base, metric, (newest - 660_000, newest, 60_000))?,
                    tenant,
                    &expected,
                )
                .await?;
                wait_data(
                    &deployment.client,
                    &range_url(&base, metric, (times[0], newest, 60_000))?,
                    tenant,
                    &expected,
                )
                .await?;
            }
        }
    }
    builder.stop_with_timeout(Some(30)).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn ha_replicas_v1_are_deduplicated_and_tenant_isolated() -> TestResult {
    ha_replicas(RemoteWrite::V1).await
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn ha_replicas_v2_are_deduplicated_and_tenant_isolated() -> TestResult {
    ha_replicas(RemoteWrite::V2).await
}

async fn ha_replicas(version: RemoteWrite) -> TestResult {
    let deployment = Deployment::start().await?;
    for (tenant, replica, value) in [
        ("tenant-a", "a", 11.0),
        ("tenant-a", "b", 99.0),
        ("tenant-b", "b", 23.0),
        ("tenant-b", "a", 88.0),
    ] {
        let response = write(
            &deployment,
            version,
            tenant,
            vec![floats(
                "ha_gauge",
                &[(START_MS, value)],
                &[("cluster", "prometheus"), ("__replica__", replica)],
            )],
            vec![],
        )
        .await?;
        let status = response.status();
        accepted(response).await?;
        if (tenant, replica) == ("tenant-a", "b") || (tenant, replica) == ("tenant-b", "a") {
            assert!(status == StatusCode::ACCEPTED);
        } else {
            assert!(status == StatusCode::OK);
        }
    }
    let builder = deployment.builder().await?;
    let cold = deployment.cold().await?;
    for container in [&deployment.hot, &cold] {
        let base = base_url(container, DATA_PORT).await?;
        for (tenant, value) in [("tenant-a", 11), ("tenant-b", 23)] {
            let url = range_url(&base, "ha_gauge", (START_MS, START_MS, 1000))?;
            wait_data(
                &deployment.client,
                &url,
                tenant,
                &float_matrix(
                    "ha_gauge",
                    &[(START_MS, value)],
                    json!({"cluster": "prometheus"}),
                ),
            )
            .await?;
        }
    }
    builder.stop_with_timeout(Some(30)).await?;
    Ok(())
}

async fn frontend(deployment: &Deployment) -> TestResult<ContainerAsync<GenericImage>> {
    let container = start(super::query_role(&deployment.network).with_cmd([
        "krabka-metrics-service",
        "--target=query-frontend",
        "--listen=0.0.0.0:4041",
        "--object-store-url=s3://metrics",
        "--cold-cache-ttl=100ms",
        "--query-frontend-split=2s",
        "--query-frontend-max-cache-freshness=1ms",
    ]))
    .await?;
    deployment.ready(&container, DATA_PORT).await?;
    Ok(container)
}

// Both tenants' gauge and histogram ranges, as the frontend answers them.
// The ranges span `times` at a one-second step.
async fn assert_frontend_ranges(deployment: &Deployment, base: &str, times: &[i64]) -> TestResult {
    let bounds = (times[0], times[times.len() - 1], 1000);
    for (tenant, value) in [("tenant-a", 7), ("tenant-b", 19)] {
        for (metric, expected) in [
            (
                "frontend_gauge",
                float_matrix(
                    "frontend_gauge",
                    &times.iter().map(|&time| (time, value)).collect::<Vec<_>>(),
                    json!({}),
                ),
            ),
            (
                "frontend_histogram",
                histogram_matrix("frontend_histogram", times),
            ),
        ] {
            assert!(
                data(
                    &deployment.client,
                    &range_url(base, metric, bounds)?,
                    tenant
                )
                .await?
                    == expected
            );
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn frontend_preserves_unaligned_float_and_histogram_queries_across_cache_hits() -> TestResult
{
    let deployment = Deployment::start().await?;
    let builder = deployment.builder().await?;
    let cold = deployment.cold().await?;
    let cold_base = base_url(&cold, DATA_PORT).await?;
    for (tenant, value) in [("tenant-a", 7.0), ("tenant-b", 19.0)] {
        accepted(
            write(
                &deployment,
                RemoteWrite::V1,
                tenant,
                vec![
                    floats("frontend_gauge", &[(START_MS, value)], &[]),
                    histograms("frontend_histogram", &[START_MS], true),
                ],
                vec![],
            )
            .await?,
        )
        .await?;
    }
    let times: Vec<_> = (0..7).map(|step| START_MS + 125 + step * 1000).collect();
    let bounds = (times[0], times[6], 1000);
    // Every evaluation is 125ms off the step grid and crosses four split windows.
    for (tenant, value) in [("tenant-a", 7), ("tenant-b", 19)] {
        for (metric, expected) in [
            (
                "frontend_gauge",
                float_matrix(
                    "frontend_gauge",
                    &times.iter().map(|&time| (time, value)).collect::<Vec<_>>(),
                    json!({}),
                ),
            ),
            (
                "frontend_histogram",
                histogram_matrix("frontend_histogram", &times),
            ),
        ] {
            wait_data(
                &deployment.client,
                &range_url(&cold_base, metric, bounds)?,
                tenant,
                &expected,
            )
            .await?;
        }
    }
    builder.stop_with_timeout(Some(30)).await?;
    deployment.hot.stop_with_timeout(Some(30)).await?;
    let frontend = frontend(&deployment).await?;
    let base = base_url(&frontend, DATA_PORT).await?;
    for _ in 0..2 {
        assert_frontend_ranges(&deployment, &base, &times).await?;
    }
    wait_metric(
        &deployment,
        &frontend,
        "krabka_metrics_query_cache_hits_total",
        16.0,
    )
    .await?;
    frontend.rm().await?;
    // A fresh process must hit the same object-store cache, with tenant keys intact.
    let reopened = self::frontend(&deployment).await?;
    let base = base_url(&reopened, DATA_PORT).await?;
    assert_frontend_ranges(&deployment, &base, &times).await?;
    wait_metric(
        &deployment,
        &reopened,
        "krabka_metrics_query_cache_hits_total",
        16.0,
    )
    .await?;
    Ok(())
}

// Golden chunks emitted by Prometheus v0.314.0's chunkenc appenders for the
// fixed input below. Gauge histograms keep the reset header deterministic.
// These bytes are independent of Krabka's remote-read encoder.
const XOR_GOLDEN: &str = "000380a0abfef962401c000000000000e807d427b703";
const XOR_MIDDLE_GOLDEN: &str = "0001d0afabfef9624020000000000000";
const INTEGER_GOLDENS: [&str; 2] = [
    "0001c0004647f0000c5e7f2b400548040000000000001288",
    "0001c0004647f0000c5e7f2b5f4548040000000000001288",
];
const FLOAT_GOLDENS: [&str; 2] = [
    "0001c0004647f0000c5e7f2b400200a00000000000000000000000000002010000000000000200000000000000020040000000000000",
    "0001c0004647f0000c5e7f2b5f4200a00000000000000000000000000002010000000000000200000000000000020040000000000000",
];

fn hex_bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII golden"), 16)
                .expect("hex golden")
        })
        .collect()
}

fn streamed_response(mut bytes: &[u8]) -> TestResult<pb::v1::ChunkedReadResponse> {
    let mut response = pb::v1::ChunkedReadResponse::default();
    while !bytes.is_empty() {
        let length = usize::try_from(prost::encoding::decode_varint(&mut bytes)?)?;
        let checksum =
            u32::from_be_bytes(bytes.get(..4).ok_or("missing frame checksum")?.try_into()?);
        let payload = bytes
            .get(4..4 + length)
            .ok_or("truncated remote-read frame")?;
        // CRC-32C (Castagnoli), as specified by Prometheus's streamed protocol.
        let crc = !payload.iter().fold(u32::MAX, |crc, byte| {
            (0..8).fold(crc ^ u32::from(*byte), |crc, _| {
                if crc & 1 == 0 {
                    crc >> 1
                } else {
                    (crc >> 1) ^ 0x82f6_3b78
                }
            })
        });
        assert!(checksum == crc);
        let frame = pb::v1::ChunkedReadResponse::decode(payload)?;
        assert!(frame.query_index == 0);
        response.chunked_series.extend(frame.chunked_series);
        bytes = &bytes[4 + length..];
    }
    response
        .chunked_series
        .sort_by(|a, b| a.labels[0].value.cmp(&b.labels[0].value));
    Ok(response)
}

async fn remote_read(
    deployment: &Deployment,
    base: &str,
    tenant: &str,
    request: &pb::v1::ReadRequest,
) -> TestResult<reqwest::Response> {
    Ok(deployment
        .client
        .post(format!("{base}/api/v1/read"))
        .header("X-Scope-OrgID", tenant)
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Prometheus-Remote-Read-Version", "0.1.0")
        .body(snap::raw::Encoder::new().compress_vec(&request.encode_to_vec())?)
        .send()
        .await?
        .error_for_status()?)
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn remote_read_preserves_samples_histograms_hints_and_streamed_frames() -> TestResult {
    let deployment = Deployment::start().await?;
    let input = vec![
        floats(
            "read_float",
            &[
                (START_MS, 7.0),
                (START_MS + 1000, 8.0),
                (START_MS + 2000, 9.0),
            ],
            &[],
        ),
        histograms(
            "read_integer_histogram",
            &[START_MS, START_MS + 1000],
            false,
        ),
        histograms("read_float_histogram", &[START_MS, START_MS + 1000], true),
    ];
    accepted(
        write(
            &deployment,
            RemoteWrite::V1,
            "tenant-a",
            input.clone(),
            vec![],
        )
        .await?,
    )
    .await?;
    let builder = deployment.builder().await?;
    let cold = deployment.cold().await?;
    for container in [&deployment.hot, &cold] {
        let base = base_url(container, DATA_PORT).await?;
        wait_data(
            &deployment.client,
            &range_url(&base, "read_float", (START_MS, START_MS + 2000, 1000))?,
            "tenant-a",
            &float_matrix(
                "read_float",
                &[(START_MS, 7), (START_MS + 1000, 8), (START_MS + 2000, 9)],
                json!({}),
            ),
        )
        .await?;
        for metric in ["read_integer_histogram", "read_float_histogram"] {
            wait_data(
                &deployment.client,
                &range_url(&base, metric, (START_MS, START_MS + 1000, 1000))?,
                "tenant-a",
                &histogram_matrix(metric, &[START_MS, START_MS + 1000]),
            )
            .await?;
        }
        for hint_range in [
            None,
            Some((START_MS, START_MS + 2000)),
            Some((START_MS + 1000, START_MS + 1000)),
        ] {
            let query = pb::v1::Query {
                start_timestamp_ms: START_MS,
                end_timestamp_ms: START_MS + 2000,
                matchers: vec![pb::v1::LabelMatcher {
                    r#type: pb::v1::label_matcher::Type::Re as i32,
                    name: "__name__".into(),
                    value: "read_.*".into(),
                }],
                hints: hint_range.map(|(start_ms, end_ms)| pb::v1::ReadHints {
                    start_ms,
                    end_ms,
                    ..Default::default()
                }),
            };
            let request = pb::v1::ReadRequest {
                queries: vec![query.clone()],
                accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
            };
            let response = remote_read(&deployment, &base, "tenant-a", &request).await?;
            assert!(response.headers()["Content-Type"] == "application/x-protobuf");
            assert!(response.headers()["Content-Encoding"] == "snappy");
            let bytes = snap::raw::Decoder::new().decompress_vec(&response.bytes().await?)?;
            let mut actual = pb::v1::ReadResponse::decode(bytes.as_slice())?;
            let mut expected = input.clone();
            if hint_range == Some((START_MS + 1000, START_MS + 1000)) {
                for series in &mut expected {
                    series
                        .samples
                        .retain(|sample| sample.timestamp == START_MS + 1000);
                    series
                        .histograms
                        .retain(|sample| sample.timestamp == START_MS + 1000);
                }
            }
            expected.sort_by(|a, b| a.labels[0].value.cmp(&b.labels[0].value));
            actual.results[0]
                .timeseries
                .sort_by(|a, b| a.labels[0].value.cmp(&b.labels[0].value));
            assert!(
                actual
                    == pb::v1::ReadResponse {
                        results: vec![pb::v1::QueryResult {
                            timeseries: expected
                        }]
                    }
            );
            let empty = remote_read(&deployment, &base, "tenant-b", &request).await?;
            let empty = snap::raw::Decoder::new().decompress_vec(&empty.bytes().await?)?;
            assert!(
                pb::v1::ReadResponse::decode(empty.as_slice())?
                    == pb::v1::ReadResponse {
                        results: vec![pb::v1::QueryResult { timeseries: vec![] }]
                    }
            );
        }
        for narrowed in [false, true] {
            let request = pb::v1::ReadRequest {
                queries: vec![pb::v1::Query {
                    start_timestamp_ms: START_MS,
                    end_timestamp_ms: START_MS + 2000,
                    matchers: vec![pb::v1::LabelMatcher {
                        r#type: pb::v1::label_matcher::Type::Re as i32,
                        name: "__name__".into(),
                        value: "read_.*".into(),
                    }],
                    hints: narrowed.then_some(pb::v1::ReadHints {
                        start_ms: START_MS + 1000,
                        end_ms: START_MS + 1000,
                        ..Default::default()
                    }),
                }],
                accepted_response_types: vec![
                    pb::v1::ResponseType::StreamedXorChunks as i32,
                    pb::v1::ResponseType::Samples as i32,
                ],
            };
            let response = remote_read(&deployment, &base, "tenant-a", &request).await?;
            assert!(
                response.headers()["Content-Type"]
                    == "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
            );
            let actual = streamed_response(&response.bytes().await?)?;
            let mut expected = vec![pb::v1::ChunkedSeries {
                labels: labels("read_float", &[]),
                chunks: vec![pb::v1::Chunk {
                    min_time_ms: START_MS + if narrowed { 1000 } else { 0 },
                    max_time_ms: START_MS + if narrowed { 1000 } else { 2000 },
                    r#type: pb::v1::chunk::Encoding::Xor as i32,
                    data: hex_bytes(if narrowed {
                        XOR_MIDDLE_GOLDEN
                    } else {
                        XOR_GOLDEN
                    }),
                }],
            }];
            for (metric, encoding, goldens) in [
                (
                    "read_float_histogram",
                    pb::v1::chunk::Encoding::FloatHistogram,
                    FLOAT_GOLDENS,
                ),
                (
                    "read_integer_histogram",
                    pb::v1::chunk::Encoding::Histogram,
                    INTEGER_GOLDENS,
                ),
            ] {
                expected.push(pb::v1::ChunkedSeries {
                    labels: labels(metric, &[]),
                    chunks: goldens
                        .iter()
                        .zip([START_MS, START_MS + 1000])
                        .filter(|(_, time)| !narrowed || *time == START_MS + 1000)
                        .map(|(golden, time)| pb::v1::Chunk {
                            min_time_ms: time,
                            max_time_ms: time,
                            r#type: encoding as i32,
                            data: hex_bytes(golden),
                        })
                        .collect(),
                });
            }
            assert!(
                actual
                    == pb::v1::ChunkedReadResponse {
                        chunked_series: expected,
                        query_index: 0
                    }
            );
        }
    }
    builder.stop_with_timeout(Some(30)).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Bazel Docker deployment"]
async fn otlp_gauges_counters_and_histograms_preserve_translation_and_metadata() -> TestResult {
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
            HistogramDataPoint, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
            ScopeMetrics, Sum, exponential_histogram_data_point, metric, number_data_point,
        },
        resource::v1::Resource,
    };
    let attribute = |key: &str, value: &str| KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
        ..Default::default()
    };
    let attributes = vec![attribute("site", "office")];
    let time_unix_nano = u64::try_from(START_MS)? * 1_000_000;
    let point = |value| NumberDataPoint {
        attributes: attributes.clone(),
        time_unix_nano,
        value: Some(number_data_point::Value::AsDouble(value)),
        ..Default::default()
    };
    let input = MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    attribute("service.name", "api"),
                    attribute("service.namespace", "prod"),
                    attribute("service.instance.id", "pod-1"),
                ],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "receiver".into(),
                    version: "1".into(),
                    ..Default::default()
                }),
                metrics: vec![
                    Metric {
                        name: "room.temperature".into(),
                        description: "Room temperature".into(),
                        unit: "Cel".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![point(7.0)],
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "requests".into(),
                        description: "Requests served".into(),
                        unit: "1".into(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![point(11.0)],
                            aggregation_temporality: 2,
                            is_monotonic: true,
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "request.duration".into(),
                        description: "Request duration".into(),
                        unit: "s".into(),
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: attributes.clone(),
                                time_unix_nano,
                                count: 5,
                                sum: Some(8.0),
                                bucket_counts: vec![2, 3, 0],
                                explicit_bounds: vec![1.0, 2.0],
                                ..Default::default()
                            }],
                            aggregation_temporality: 2,
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "queue.duration".into(),
                        description: "Queue duration".into(),
                        unit: "s".into(),
                        data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                            data_points: vec![ExponentialHistogramDataPoint {
                                attributes,
                                time_unix_nano,
                                count: 5,
                                sum: Some(8.0),
                                scale: 0,
                                positive: Some(exponential_histogram_data_point::Buckets {
                                    offset: -1,
                                    bucket_counts: vec![2, 3],
                                }),
                                ..Default::default()
                            }],
                            aggregation_temporality: 2,
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let deployment = Deployment::start().await?;
    let write_base = base_url(&deployment.distributor, DATA_PORT).await?;
    let response = deployment
        .client
        .post(format!("{write_base}/otlp/v1/metrics"))
        .header("X-Scope-OrgID", "tenant-a")
        .header("Content-Type", "application/x-protobuf")
        .body(input.encode_to_vec())
        .send()
        .await?;
    let status = response.status();
    let response = response.bytes().await?;
    assert!(status == StatusCode::OK, "OTLP: {status}: {response:?}");
    let response =
        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse::decode(
            response,
        )?;
    assert!(response.partial_success.is_none());
    let common = json!({"job": "prod/api", "instance": "pod-1", "site": "office", "otel_scope_name": "receiver", "otel_scope_version": "1"});
    let mut histogram_expected = histogram_matrix("queue_duration_seconds", &[START_MS]);
    histogram_expected["result"][0]["metric"]
        .as_object_mut()
        .expect("metric labels")
        .extend(common.as_object().expect("labels").clone());
    let mut probes = vec![
        (
            "room_temperature_celsius",
            float_matrix("room_temperature_celsius", &[(START_MS, 7)], common.clone()),
        ),
        (
            "requests_total",
            float_matrix("requests_total", &[(START_MS, 11)], common.clone()),
        ),
        (
            "request_duration_seconds_count",
            float_matrix(
                "request_duration_seconds_count",
                &[(START_MS, 5)],
                common.clone(),
            ),
        ),
        (
            "request_duration_seconds_sum",
            float_matrix(
                "request_duration_seconds_sum",
                &[(START_MS, 8)],
                common.clone(),
            ),
        ),
        ("queue_duration_seconds", histogram_expected),
    ];
    for (bound, value) in [("1", 2), ("2", 5), ("+Inf", 5)] {
        let mut extra = common.clone();
        extra["le"] = json!(bound);
        probes.push((
            "request_duration_seconds_bucket",
            float_matrix(
                "request_duration_seconds_bucket",
                &[(START_MS, value)],
                extra,
            ),
        ));
    }
    let hot = base_url(&deployment.hot, DATA_PORT).await?;
    check_otlp(&deployment, &hot, &probes).await?;
    let builder = deployment.builder().await?;
    let cold = deployment.cold().await?;
    let base = base_url(&cold, DATA_PORT).await?;
    check_otlp(&deployment, &base, &probes).await?;
    builder.stop_with_timeout(Some(30)).await?;
    deployment.hot.stop_with_timeout(Some(30)).await?;
    cold.rm().await?;
    let reopened = deployment.cold().await?;
    let base = base_url(&reopened, DATA_PORT).await?;
    check_otlp(&deployment, &base, &probes).await?;
    Ok(())
}

async fn check_otlp(deployment: &Deployment, base: &str, probes: &[(&str, Value)]) -> TestResult {
    for (metric, expected) in probes {
        let query = if *metric == "request_duration_seconds_bucket" {
            format!(
                "{metric}{{le=\"{}\"}}",
                expected["result"][0]["metric"]["le"]
                    .as_str()
                    .expect("bucket bound")
            )
        } else {
            (*metric).to_owned()
        };
        let url = range_url(base, &query, (START_MS, START_MS, 1000))?;
        wait_data(&deployment.client, &url, "tenant-a", expected).await?;
        assert!(data(&deployment.client, &url, "tenant-b").await? == matrix(json!([])));
    }
    for (metric, kind, help, unit) in [
        (
            "room_temperature_celsius",
            "gauge",
            "Room temperature",
            "Cel",
        ),
        ("requests_total", "counter", "Requests served", "1"),
        (
            "request_duration_seconds",
            "histogram",
            "Request duration",
            "s",
        ),
        ("queue_duration_seconds", "histogram", "Queue duration", "s"),
    ] {
        let url = api_url(base, "/api/v1/metadata", &[("metric", metric.into())])?;
        wait_data(
            &deployment.client,
            &url,
            "tenant-a",
            &json!({metric: [{"type": kind, "help": help, "unit": unit}]}),
        )
        .await?;
        assert!(data(&deployment.client, &url, "tenant-b").await? == json!({}));
    }
    Ok(())
}
