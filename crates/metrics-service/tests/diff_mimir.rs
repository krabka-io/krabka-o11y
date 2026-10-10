//! Docker-backed differential probe against real Grafana Mimir.
//!
//! This is the headline equality test for the metrics slice. Mimir is the
//! system that Krabka replaces, so corpus equality over identical
//! `remote_write` input is the strongest single correctness signal.
//!
//! The corpus is the one `diff_prometheus` drives: the vendored upstream
//! `promql/promqltest` suite, turned into `remote_write` bodies and queries by
//! `support/promql_corpus.rs`. Mimir embeds Prometheus's own `PromQL` engine, so
//! it starts from the same known-divergence list and adds only what is Mimir's.
//!
//! Cargo ignores this test by default, because it pulls and runs
//! `mirror.gcr.io/grafana/mimir` under Docker.
//! Run with:
//!
//! `cargo test -p krabka-metrics-service --test diff_mimir -- --ignored --nocapture`

#![recursion_limit = "512"]

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use assert2::assert;
use bytes::Bytes;
use krabka_metrics::wire::pb;
use krabka_promql::{InMemoryMetricStore, MergedMetricStore, WalHead};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    number_data_point,
};
use promql_corpus::{CorpusCase, CorpusSeries, KnownDivergence, PromqlCorpus, QueryKind};
use prost::Message as _;
use reqwest::StatusCode;
use serde_json::Value;
use testcontainers::{
    GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

// `normalize` lives with the hand-written seed dataset that `grafana_integration`
// asserts against; `promql_corpus` reaches it through this module. Nothing else
// in it belongs to this suite.
#[path = "support/corpus_differential.rs"]
mod corpus_differential;
#[path = "support/upstream_http.rs"]
mod upstream_http;

use self::{
    corpus_differential::{CorpusDiff, PromApi, QueryParam, query_url, quoted, seconds_param},
    upstream_http::{KrabkaServer, TestResult, mapped_base_url, wait_for_http_ok},
};

#[allow(dead_code)]
#[path = "../../metrics/tests/support/diff_corpus.rs"]
mod diff_corpus;

#[path = "support/promql_corpus.rs"]
mod promql_corpus;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// Fixed tenant header used on both sides.
///
/// Mimir needs `X-Scope-OrgID` on every push and query, because multitenancy is
/// on and this test pins one tenant. Krabka's querier keys storage by the same
/// header.
const TENANT: &str = "compliance";

/// Mimir's default HTTP server port.
///
/// In monolithic mode, this port serves `/ready`, `/api/v1/push`, and the
/// `/prometheus/api/v1/query*` read endpoints.
const MIMIR_PORT: u16 = 9009;

/// Queries in flight against one engine.
///
/// Mimir's monolithic query frontend applies its outstanding-request limit to
/// the internal work a query fans out into. Serializing this correctness probe
/// avoids load-shedding responses that are unrelated to query semantics.
const QUERY_CONCURRENCY: usize = 1;

/// Where Mimir disagrees with Krabka for reasons that are Mimir's own.
///
/// Mimir 3.2.1 embeds older engine semantics than the pinned Prometheus 3.14
/// oracle. The shared curated corpus mixes a 3.8.1 base with the 3.14 updates
/// recorded in its attribution. These exact cases name observed differences;
/// the list must be rebaselined when either oracle changes.
///
/// The contract runs both ways: a case listed here MUST disagree, so the list
/// cannot quietly become a licence.
const MIMIR_DIVERGENCES: &[KnownDivergence] = &[
    KnownDivergence {
        reason: "Mimir 3.2.1 embeds an older Prometheus engine. These exact cases use older engine semantics than the pinned Prometheus 3.14 oracle; the bidirectional list must be rebaselined if either dependency changes.",
        cases: &[
            "aggregators.test:522",
            "aggregators.test:546",
            "aggregators.test:653",
            "aggregators.test:656",
            "aggregators.test:689",
            "aggregators.test:692",
            "at_modifier.test:93",
            "duration_expression.test:170",
            "duration_expression.test:173",
            "duration_expression.test:176",
            "duration_expression.test:203",
            "duration_expression.test:206",
            "duration_expression.test:209",
            "duration_expression.test:212",
            "duration_expression.test:215",
            "duration_expression.test:218",
            "duration_expression.test:221",
            "duration_expression.test:224",
            "duration_expression.test:227",
            "extended_vectors.test:370",
            "extended_vectors.test:373",
            "extended_vectors.test:376",
            "extended_vectors.test:379",
            "extended_vectors.test:382",
            "extended_vectors.test:385",
            "extended_vectors.test:392",
            "extended_vectors.test:395",
            "name_label_dropping.test:84",
            "native_histograms.test:1200",
            "native_histograms.test:1205",
            "native_histograms.test:1217",
            "native_histograms.test:1406",
            "native_histograms.test:1427",
            "native_histograms.test:1492",
            "native_histograms.test:1497",
            "native_histograms.test:1502",
            "native_histograms.test:1506",
            "native_histograms.test:1510",
            "native_histograms.test:1515",
            "native_histograms.test:1519",
            "native_histograms.test:1524",
            "native_histograms.test:1735",
            "native_histograms.test:1835",
            "type_and_unit.test:122",
            "type_and_unit.test:140",
            "type_and_unit.test:181",
            "type_and_unit.test:195",
            "type_and_unit.test:198",
            "type_and_unit.test:208",
            "type_and_unit.test:222",
            "type_and_unit.test:237",
            "type_and_unit.test:279",
            "type_and_unit.test:39",
            "type_and_unit.test:53",
            "type_and_unit.test:56",
            "type_and_unit.test:63",
            "type_and_unit.test:77",
            "type_and_unit.test:92",
        ],
    },
    KnownDivergence {
        reason: "Mimir 3.2.1 drops metric names before range functions and binary operations. The pinned Prometheus 3.14 oracle enables delayed name removal and retains these names through those operations.",
        cases: &[
            "name_label_dropping.test:59",
            "name_label_dropping.test:64",
            "name_label_dropping.test:69",
            "name_label_dropping.test:74",
            "name_label_dropping.test:115",
            "name_label_dropping.test:119",
            "name_label_dropping.test:123",
        ],
    },
    KnownDivergence {
        reason: "Mimir 3.2.1 emits the older histogram annotations without the originating metric name or the public API monotonicity summary. Prometheus 3.14 retains metric names and enriches monotonicity annotations.",
        cases: &[
            "histograms.test:958",
            "histograms.test:962",
            "histograms.test:966",
            "histograms.test:1079",
            "histograms.test:1084",
            "native_histograms.test:1787",
            "native_histograms.test:1791",
            "native_histograms.test:1794",
            "native_histograms.test:1797",
        ],
    },
    KnownDivergence {
        reason: "Mimir 3.2.1 query sharding rewrites AVG as SUM/COUNT, so two finite maxima overflow before division. Krabka, unsharded Mimir and Prometheus 3.14 retain the finite average; separate response guards check these exact values.",
        cases: &["last_operation_numeric:overflow:avg"],
    },
];

const MIMIR_AGREES_WITH_KRABKA: &[KnownDivergence] = &[];

/// Minimal Mimir monolithic config.
///
/// The config is single-binary, and the CLI passes `-target=all`. Object
/// storage is on the filesystem under `/tmp`. Native histograms are on, so
/// Mimir accepts the histogram cases of the corpus. Multitenancy stays on, and
/// the test pins one `X-Scope-OrgID`.
///
/// The block ranges are the corpus's doing. It lays its segments out over about
/// three weeks of simulated time, and an ingester compacts its head as soon as
/// the head covers more than one and a half block ranges -- which, at the
/// default two hours, is immediately. The compacted blocks then belong to the
/// store-gateway, which has not synced the bucket yet, and every query comes
/// back empty. A block range wider than the corpus keeps the whole seed in the
/// head where the querier can still see it.
const MIMIR_CONFIG: &str = r"
multitenancy_enabled: true

server:
  http_listen_port: 9009
  grpc_listen_port: 9095

common:
  storage:
    backend: filesystem
    filesystem:
      dir: /tmp/mimir/data

blocks_storage:
  backend: filesystem
  filesystem:
    dir: /tmp/mimir/blocks
  bucket_store:
    sync_dir: /tmp/mimir/tsdb-sync
  tsdb:
    dir: /tmp/mimir/tsdb
    block_ranges_period: [ 1440h ]
    retention_period: 2160h
    head_compaction_idle_timeout: 0s

compactor:
  data_dir: /tmp/mimir/compactor

ruler_storage:
  backend: filesystem
  filesystem:
      dir: /tmp/mimir/ruler

alertmanager_storage:
  backend: filesystem
  filesystem:
    dir: /tmp/mimir/alertmanager

alertmanager:
  data_dir: /tmp/mimir/alertmanager-data

ingester:
  ring:
    # Monolithic single-binary has exactly one ingester; the default
    # replication factor of 3 makes the distributor reject every push with
    # 'at least 2 live replicas required, could only find 1'.
    replication_factor: 1

limits:
  native_histograms_ingestion_enabled: true

distributor:
  influx_endpoint_enabled: true

api:
  otlp_translation_headers_enabled: true
";

#[tokio::test]
#[ignore = "requires Docker"]
async fn mimir_compliance_corpus_matches_krabka() -> TestResult {
    let mut corpus = promql_corpus::promql_corpus();
    let client = reqwest::Client::new();

    // Real Mimir in monolithic mode.
    let mimir = start_mimir().await?;
    let mimir_base = mapped_base_url(&mimir, MIMIR_PORT).await?;
    wait_for_http_ok(
        &client,
        &format!("{mimir_base}/ready"),
        Duration::from_mins(1),
    )
    .await?;
    let mimir_alertmanager = start_mimir_alertmanager().await?;
    let mimir_alertmanager_base = mapped_base_url(&mimir_alertmanager, MIMIR_PORT).await?;
    wait_for_http_ok(
        &client,
        &format!("{mimir_alertmanager_base}/ready"),
        Duration::from_mins(1),
    )
    .await?;

    // In-process Krabka write+query path (identical to diff_prometheus.rs).
    let krabka = start_krabka_query_server().await?;
    verify_ruler_and_alertmanager_contracts(
        &client,
        &krabka.base_url,
        &mimir_base,
        &mimir_alertmanager_base,
    )
    .await?;
    mimir_diff(&krabka.base_url, &mimir_base)
        .seed(&client, &corpus)
        .await?;
    let engines = EnginePair {
        client: &client,
        krabka_base: &krabka.base_url,
        mimir_base: &mimir_base,
    };
    verify_ingest_contract(engines).await?;

    let mut mismatches = mimir_diff(&krabka.base_url, &mimir_base)
        .run(&client, &corpus)
        .await?;
    // Seed these after the vendored cases, whose broad selectors must not see
    // additional extreme-valued series.
    let at_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())? - 1_000;
    let numeric = last_operation_numeric_corpus(at_ms);
    mimir_diff(&krabka.base_url, &mimir_base)
        .seed(&client, &numeric)
        .await?;
    verify_numeric_seed(engines, &numeric).await?;
    verify_numeric_overflow(engines, &numeric).await?;
    mismatches.extend(
        mimir_diff(&krabka.base_url, &mimir_base)
            .run(&client, &numeric)
            .await?,
    );
    corpus.series.extend(numeric.series);
    corpus.cases.extend(numeric.cases);
    promql_corpus::write_report(
        "diff_mimir",
        &corpus,
        &mismatches,
        MIMIR_DIVERGENCES,
        &promql_corpus::ReportConfiguration {
            oracle_flags: vec![
                "-query-frontend.enabled-promql-experimental-functions=all".to_string(),
                "-query-frontend.enabled-promql-extended-range-selectors=all".to_string(),
            ],
            candidate_engine_opts: candidate_engine_opts(),
        },
    );
    krabka.shutdown();

    let verdict = promql_corpus::check_divergences(
        &corpus,
        &mismatches,
        MIMIR_DIVERGENCES,
        MIMIR_AGREES_WITH_KRABKA,
    );
    assert!(
        verdict.is_none(),
        "the differential and the known-divergence list disagree:\n{}",
        verdict.unwrap_or_default()
    );
    Ok(())
}

fn last_operation_numeric_corpus(at_ms: i64) -> PromqlCorpus {
    let mut corpus = PromqlCorpus::default();
    for (scenario, values, operations) in [
        ("overflow", vec![f64::MAX, f64::MAX], &["avg"][..]),
        (
            "cancellation",
            vec![f64::MAX, -f64::MAX],
            &["sum", "avg"][..],
        ),
        ("compensation", vec![1e16, 1.0, 1.0], &["sum", "avg"][..]),
    ] {
        for (index, value) in values.into_iter().enumerate() {
            corpus.series.push(CorpusSeries {
                labels: vec![
                    ("__name__".into(), "mimir_last_operation_fold".into()),
                    ("scenario".into(), scenario.into()),
                    ("series".into(), index.to_string()),
                ],
                floats: vec![(at_ms, value)],
                ..Default::default()
            });
        }
        for operation in operations {
            corpus.cases.push(CorpusCase {
                name: format!("last_operation_numeric:{scenario}:{operation}"),
                promql: format!(
                    "{operation}(last_over_time(mimir_last_operation_fold{{scenario={}}}[30m]))",
                    quoted(scenario)
                ),
                kind: QueryKind::Instant { time: at_ms },
                expects_failure: false,
            });
        }
    }
    corpus
}

async fn verify_numeric_seed(engines: EnginePair<'_>, corpus: &PromqlCorpus) -> TestResult {
    let EnginePair {
        client,
        krabka_base,
        mimir_base,
    } = engines;
    // Comparing an aggregate alone could hide missing cancelling samples.
    // Each full selector must return its exact identity, timestamp and value.
    for series in &corpus.series {
        let selector = series
            .labels
            .iter()
            .map(|(name, value)| format!("{name}={}", quoted(value)))
            .collect::<Vec<_>>()
            .join(",");
        let (at_ms, value) = series.floats[0];
        let case = CorpusCase {
            name: format!("last_operation_numeric:seed:{{{selector}}}"),
            promql: format!("{{{selector}}}"),
            kind: QueryKind::Instant { time: at_ms },
            expects_failure: false,
        };
        let timestamp: Value = serde_json::from_str(
            seconds_param(at_ms)
                .trim_end_matches('0')
                .trim_end_matches('.'),
        )?;
        let labels: BTreeMap<_, _> = series.labels.iter().cloned().collect();
        let expected = serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": labels, "value": [timestamp, value.to_string()]}]
            }
        });
        for (base, prefix) in [(krabka_base, ""), (mimir_base, "/prometheus")] {
            PromApi {
                base,
                prefix,
                tenant: Some(TENANT),
            }
            .wait_for_query_ready(client, &case.promql, at_ms)
            .await?;
            let response = PromApi {
                base,
                prefix,
                tenant: Some(TENANT),
            }
            .query_case(client, &case)
            .await?;
            let mismatch = promql_corpus::compare_case(&case, &response, &expected);
            assert!(
                mismatch.is_none(),
                "{base}: {}",
                mismatch.unwrap_or_default()
            );
        }
    }
    println!(
        "diff_mimir: verified all {} numeric seed identities on both engines",
        corpus.series.len()
    );
    Ok(())
}

async fn verify_numeric_overflow(engines: EnginePair<'_>, corpus: &PromqlCorpus) -> TestResult {
    let EnginePair {
        client,
        krabka_base,
        mimir_base,
    } = engines;
    let case = corpus
        .cases
        .iter()
        .find(|case| case.name == "last_operation_numeric:overflow:avg")
        .ok_or("numeric corpus has no overflow average")?;
    let QueryKind::Instant { time } = case.kind else {
        return Err("numeric overflow average is not an instant query".into());
    };
    let timestamp: Value = serde_json::from_str(
        seconds_param(time)
            .trim_end_matches('0')
            .trim_end_matches('.'),
    )?;
    // Keep the default sharded disagreement, but do not let its exemption hide
    // an incorrect finite result or an unrelated upstream error.
    for (base, prefix, shards, value) in [
        (krabka_base, "", None, f64::MAX.to_string()),
        (mimir_base, "/prometheus", Some("0"), f64::MAX.to_string()),
        (mimir_base, "/prometheus", None, "+Inf".into()),
    ] {
        let mut request = client
            .get(query_url(
                base,
                &format!("{prefix}/api/v1/query"),
                &[
                    QueryParam {
                        name: "query",
                        argument: case.promql.clone(),
                    },
                    QueryParam {
                        name: "time",
                        argument: seconds_param(time),
                    },
                ],
            ))
            .header("X-Scope-OrgID", TENANT);
        if let Some(shards) = shards {
            request = request.header("Sharding-Control", shards);
        }
        let response = request.send().await?;
        assert!(
            response.status() == StatusCode::OK,
            "{base}, shards={shards:?}"
        );
        let response: Value = response.json().await?;
        let expected = serde_json::json!({
            "status": "success",
            "data": {"resultType": "vector", "result": [{"metric": {}, "value": [timestamp, value]}]}
        });
        let mismatch = promql_corpus::compare_case(case, &response, &expected);
        assert!(
            mismatch.is_none(),
            "{base}, shards={shards:?}: {}",
            mismatch.unwrap_or_default()
        );
    }
    println!(
        "diff_mimir: verified overflow average: Krabka MAX, unsharded Mimir MAX, default Mimir +Inf"
    );
    Ok(())
}

async fn verify_ruler_and_alertmanager_contracts(
    client: &reqwest::Client,
    krabka: &str,
    mimir: &str,
    mimir_alertmanager: &str,
) -> TestResult {
    let rule = "name: recording\nrules:\n  - record: m11:up\n    expr: vector(1)\n";
    for base in [krabka, mimir] {
        let response = client
            .post(format!("{base}/prometheus/config/v1/rules/team"))
            .header("X-Scope-OrgID", TENANT)
            .body(rule)
            .send()
            .await?;
        assert!(
            response.status() == StatusCode::ACCEPTED,
            "rule POST failed for {base}: {response:?}"
        );
    }
    let krabka_group = client
        .get(format!(
            "{krabka}/prometheus/config/v1/rules/team/recording"
        ))
        .header("X-Scope-OrgID", TENANT)
        .send()
        .await?;
    let mimir_group = client
        .get(format!("{mimir}/prometheus/config/v1/rules/team/recording"))
        .header("X-Scope-OrgID", TENANT)
        .send()
        .await?;
    assert!(krabka_group.status() == mimir_group.status());
    let krabka_group: serde_yaml::Value = serde_yaml::from_slice(&krabka_group.bytes().await?)?;
    let mimir_group: serde_yaml::Value = serde_yaml::from_slice(&mimir_group.bytes().await?)?;
    assert!(krabka_group == mimir_group, "ruler group payload differs");
    for base in [krabka, mimir] {
        let response = client
            .delete(format!("{base}/prometheus/config/v1/rules/team/recording"))
            .header("X-Scope-OrgID", TENANT)
            .send()
            .await?;
        assert!(
            response.status() == StatusCode::ACCEPTED,
            "rule DELETE failed for {base}"
        );
    }

    let config = "template_files: {}\nalertmanager_config: |\n  route:\n    receiver: default\n  receivers:\n    - name: default\n";
    for base in [krabka, mimir_alertmanager] {
        let response = client
            .post(format!("{base}/api/v1/alerts"))
            .header("X-Scope-OrgID", TENANT)
            .body(config)
            .send()
            .await?;
        assert!(
            response.status() == StatusCode::CREATED,
            "Alertmanager config POST failed for {base}: {response:?}"
        );
    }
    wait_for_alertmanager_ready(client, mimir_alertmanager).await?;
    let alert = serde_json::json!([{"labels":{"alertname":"M11Down","instance":"one"},"annotations":{"summary":"down"}}]);
    for base in [krabka, mimir_alertmanager] {
        let response = client
            .post(format!("{base}/alertmanager/api/v2/alerts"))
            .header("X-Scope-OrgID", TENANT)
            .json(&alert)
            .send()
            .await?;
        assert!(
            response.status().is_success(),
            "v2 alert POST failed for {base}: {response:?}"
        );
        let response = client
            .get(format!(
                "{base}/alertmanager/api/v2/alerts?filter=alertname%3DM11Down"
            ))
            .header("X-Scope-OrgID", TENANT)
            .send()
            .await?;
        assert!(
            response.status().is_success(),
            "v2 alert GET failed for {base}: {response:?}"
        );
        let alerts: Value = response.json().await?;
        assert!(alerts[0]["fingerprint"].as_str().is_some());
        assert!(alerts[0]["status"]["state"] == "active");
    }
    let silence = serde_json::json!({"matchers":[{"name":"alertname","value":"M11Down","isRegex":false,"isEqual":true}],"startsAt":"2026-01-01T00:00:00Z","endsAt":"2099-01-01T00:00:00Z","createdBy":"differential","comment":"m11"});
    for base in [krabka, mimir_alertmanager] {
        let response = client
            .post(format!("{base}/alertmanager/api/v2/silences"))
            .header("X-Scope-OrgID", TENANT)
            .json(&silence)
            .send()
            .await?;
        assert!(
            response.status().is_success(),
            "v2 silence POST failed for {base}: {response:?}"
        );
        let body: Value = response.json().await?;
        assert!(body["silenceID"].as_str().is_some());
    }
    Ok(())
}

async fn wait_for_alertmanager_ready(client: &reqwest::Client, base: &str) -> TestResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(format!("{base}/alertmanager/api/v2/status"))
            .header("X-Scope-OrgID", TENANT)
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("Alertmanager API did not initialize: {response:?}").into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[derive(Debug)]
struct IngestResponse {
    status: StatusCode,
    headers: reqwest::header::HeaderMap,
    body: Bytes,
}

struct IngestCase<'a> {
    path: &'a str,
    request_headers: &'a [(&'a str, &'a str)],
    response_headers: &'a [&'a str],
    metric: &'a str,
    expected_value: &'a str,
}

impl IngestResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

async fn verify_ingest_contract(engines: EnginePair<'_>) -> TestResult {
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())? - 1_000;

    let v1 = pb::v1::WriteRequest {
        timeseries: vec![pb::v1::TimeSeries {
            labels: vec![
                pb::v1::Label {
                    name: "__name__".into(),
                    value: "m11_remote_write_v1".into(),
                },
                pb::v1::Label {
                    name: "source".into(),
                    value: "differential".into(),
                },
            ],
            samples: vec![pb::v1::Sample {
                value: 1.0,
                timestamp: now_ms,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    compare_ingest(
        engines,
        IngestCase {
            path: "/api/v1/push",
            request_headers: &[
                ("Content-Type", "application/x-protobuf"),
                ("Content-Encoding", "snappy"),
            ],
            response_headers: &[],
            metric: "m11_remote_write_v1",
            expected_value: "1",
        },
        IngestPayload {
            body: snappy(&v1.encode_to_vec())?,
            at_ms: now_ms,
        },
    )
    .await?;

    let v2 = pb::v2::Request {
        symbols: vec![
            String::new(),
            "__name__".into(),
            "m11_remote_write_v2".into(),
            "source".into(),
            "differential".into(),
        ],
        timeseries: vec![pb::v2::TimeSeries {
            labels_refs: vec![1, 2, 3, 4],
            samples: vec![pb::v2::Sample {
                value: 2.0,
                timestamp: now_ms,
                start_timestamp: 0,
            }],
            ..Default::default()
        }],
    };
    compare_ingest(
        engines,
        IngestCase {
            path: "/api/v1/push",
            request_headers: &[
                (
                    "Content-Type",
                    "application/x-protobuf;proto=io.prometheus.write.v2.Request",
                ),
                ("Content-Encoding", "snappy"),
                ("X-Prometheus-Remote-Write-Version", "2.0.0"),
            ],
            response_headers: &[
                "x-prometheus-remote-write-samples-written",
                "x-prometheus-remote-write-histograms-written",
                "x-prometheus-remote-write-exemplars-written",
            ],
            metric: "m11_remote_write_v2",
            expected_value: "2",
        },
        IngestPayload {
            body: snappy(&v2.encode_to_vec())?,
            at_ms: now_ms,
        },
    )
    .await?;

    let otlp = MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "m11.otlp.gauge".into(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: u64::try_from(now_ms)?.saturating_mul(1_000_000),
                            value: Some(number_data_point::Value::AsDouble(3.0)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            schema_url: String::new(),
        }],
    };
    compare_ingest(
        engines,
        IngestCase {
            path: "/otlp/v1/metrics",
            request_headers: &[("Content-Type", "application/x-protobuf")],
            response_headers: &["content-type", "content-length", "x-content-type-options"],
            metric: "m11_otlp_gauge",
            expected_value: "3",
        },
        IngestPayload {
            body: otlp.encode_to_vec(),
            at_ms: now_ms,
        },
    )
    .await?;

    compare_ingest(
        engines,
        IngestCase {
            path: "/api/v1/push/influx/write?precision=ms",
            request_headers: &[("Content-Type", "text/plain")],
            response_headers: &["content-type", "content-length"],
            metric: "m11_influx",
            expected_value: "4",
        },
        IngestPayload {
            body: format!("m11_influx,source=differential value=4 {now_ms}\n").into_bytes(),
            at_ms: now_ms,
        },
    )
    .await?;
    Ok(())
}

/// The two engines a Mimir differential check compares, and the client it
/// reaches them with.
#[derive(Clone, Copy)]
struct EnginePair<'a> {
    client: &'a reqwest::Client,
    krabka_base: &'a str,
    mimir_base: &'a str,
}

/// One ingest request body, and the time its samples are queried at.
struct IngestPayload {
    body: Vec<u8>,
    at_ms: i64,
}

async fn compare_ingest(
    engines: EnginePair<'_>,
    case: IngestCase<'_>,
    payload: IngestPayload,
) -> TestResult {
    let EnginePair {
        client,
        krabka_base,
        mimir_base,
    } = engines;
    let IngestPayload { body, at_ms } = payload;
    let krabka = post_ingest(client, krabka_base, case.path, case.request_headers, &body).await?;
    let mimir = post_ingest(client, mimir_base, case.path, case.request_headers, &body).await?;
    assert!(
        krabka.status == mimir.status,
        "{} status differs: Krabka {:?}, Mimir {:?}",
        case.path,
        krabka,
        mimir
    );
    assert!(krabka.status.is_success(), "{}: {krabka:?}", case.path);
    for header in case.response_headers {
        assert!(
            krabka.header(header) == mimir.header(header),
            "{} header {header} differs: Krabka {:?}, Mimir {:?}",
            case.path,
            krabka,
            mimir
        );
    }
    assert!(
        krabka.body == mimir.body,
        "{} response body differs: Krabka {:?}, Mimir {:?}",
        case.path,
        krabka,
        mimir
    );

    PromApi {
        base: krabka_base,
        prefix: "",
        tenant: Some(TENANT),
    }
    .wait_for_query_ready(client, case.metric, at_ms)
    .await?;
    PromApi {
        base: mimir_base,
        prefix: "/prometheus",
        tenant: Some(TENANT),
    }
    .wait_for_query_ready(client, case.metric, at_ms)
    .await?;
    let krabka_query = PromApi {
        base: krabka_base,
        prefix: "",
        tenant: Some(TENANT),
    }
    .query_instant(client, case.metric, at_ms)
    .await?;
    let mimir_query = PromApi {
        base: mimir_base,
        prefix: "/prometheus",
        tenant: Some(TENANT),
    }
    .query_instant(client, case.metric, at_ms)
    .await?;
    assert!(
        krabka_query["data"]["result"] == mimir_query["data"]["result"],
        "{} query differs:\nKrabka: {krabka_query}\nMimir: {mimir_query}",
        case.metric
    );
    let timestamp: Value = serde_json::from_str(
        seconds_param(at_ms)
            .trim_end_matches('0')
            .trim_end_matches('.'),
    )?;
    let expected = serde_json::json!({
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [{"metric": {}, "value": [timestamp, case.expected_value]}]
        }
    });
    for query in [
        format!("sum(last_over_time({}[30m]))", case.metric),
        format!("avg(last_over_time({}[30m]))", case.metric),
    ] {
        let krabka_query = PromApi {
            base: krabka_base,
            prefix: "",
            tenant: Some(TENANT),
        }
        .query_instant(client, &query, at_ms)
        .await?;
        let mimir_query = PromApi {
            base: mimir_base,
            prefix: "/prometheus",
            tenant: Some(TENANT),
        }
        .query_instant(client, &query, at_ms)
        .await?;
        assert!(
            krabka_query == mimir_query && mimir_query == expected,
            "{query} response differs:\nKrabka: {krabka_query}\nMimir: {mimir_query}\nExpected: {expected}"
        );
    }
    Ok(())
}

async fn post_ingest(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> TestResult<IngestResponse> {
    let mut request = client
        .post(format!("{base}{path}"))
        .header("X-Scope-OrgID", TENANT)
        .body(body.to_vec());
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = request.send().await?;
    Ok(IngestResponse {
        status: response.status(),
        headers: response.headers().clone(),
        body: response.bytes().await?,
    })
}

fn snappy(body: &[u8]) -> TestResult<Vec<u8>> {
    Ok(snap::raw::Encoder::new().compress_vec(body)?)
}

async fn start_mimir() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagreed
    // testcontainers pulled the image over the network and the suite compared
    // against whatever it got rather than against the pinned bytes.
    let tag = std::env::var("KRABKA_MIMIR_IMAGE_TAG").expect(
        "KRABKA_MIMIR_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under \
         cargo, set it to that image's tag in //bazel/images/images.bzl.",
    );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/mimir".to_string(), tag)
            .with_exposed_port(MIMIR_PORT.tcp())
            // Mimir logs go-kit lines to stderr; the HTTP server announces
            // itself with "server listening on addresses" once the port is up. (It
            // never logs the literal "Starting Mimir" — its banner is "Starting
            // application".) Readiness of the ingester ring is then polled via the
            // /ready endpoint below, which can take ~30s in monolithic mode.
            .with_wait_for(WaitFor::message_on_stderr("server listening on addresses"))
            // Copy the config into the image rather than bind-mounting a host path,
            // so the test has no host-filesystem prerequisites.
            .with_copy_to("/etc/mimir/mimir.yaml", MIMIR_CONFIG.as_bytes().to_vec())
            // host-gateway entry kept for symmetry with the other Docker suites; the
            // differential path here is container->host-agnostic (we dial Mimir's
            // mapped port), but it makes the container reachable both ways on Linux.
            .with_host("host.docker.internal", Host::HostGateway)
            .with_cmd([
                "-target=all",
                "-config.file=/etc/mimir/mimir.yaml",
                // The corpus uses epoch-relative timestamps, far older than
                // Mimir's default 13h ingester-query window — without this the
                // querier never looks in the (head-resident) ingester and every
                // query returns empty. 0 = always query ingesters regardless of
                // sample age. (CLI flag form: the YAML field lives under a
                // different config path than `querier:`.)
                "-querier.query-ingesters-within=0",
                // `info()` and `double_exponential_smoothing` are experimental
                // upstream, and Mimir gates them per tenant rather than by the
                // `--enable-feature` flag Prometheus uses.
                "-query-frontend.enabled-promql-experimental-functions=all",
                "-query-frontend.enabled-promql-extended-range-selectors=all",
            ])
            .start(),
    )
    .await??)
}

async fn start_mimir_alertmanager() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag = std::env::var("KRABKA_MIMIR_IMAGE_TAG")
        .expect("KRABKA_MIMIR_IMAGE_TAG is set by --config=docker");
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/mimir".to_string(), tag)
            .with_exposed_port(MIMIR_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("server listening on addresses"))
            .with_copy_to("/etc/mimir/mimir.yaml", MIMIR_CONFIG.as_bytes().to_vec())
            .with_cmd([
                "-target=alertmanager",
                "-config.file=/etc/mimir/mimir.yaml",
                "-alertmanager.enable-api=true",
            ])
            .start(),
    )
    .await??)
}

fn candidate_engine_opts() -> krabka_promql::EngineOpts {
    krabka_promql::EngineOpts {
        enable_type_and_unit_labels: false,
        ..krabka_promql::EngineOpts::default()
    }
}

async fn start_krabka_query_server() -> TestResult<KrabkaServer> {
    let head = WalHead::new();
    let state = Arc::new(krabka_promql::PrometheusApiState::new(
        Arc::new(MergedMetricStore::new(
            InMemoryMetricStore::new(),
            head.clone(),
        )),
        candidate_engine_opts(),
    ));
    let query_router = krabka_promql::mimir_ruler_prometheus_router(Arc::clone(&state))
        .merge(krabka_promql::mimir_ruler_router(Arc::clone(&state)))
        .merge(krabka_promql::mimir_alertmanager_router(state));
    KrabkaServer::start(query_router, head, "127.0.0.1:0".parse()?).await
}

fn mimir_diff<'a>(krabka_base: &'a str, mimir_base: &'a str) -> CorpusDiff<'a> {
    CorpusDiff {
        suite: "diff_mimir",
        krabka: PromApi {
            base: krabka_base,
            prefix: "",
            tenant: Some(TENANT),
        },
        upstream: PromApi {
            base: mimir_base,
            prefix: "/prometheus",
            tenant: Some(TENANT),
        },
        upstream_write_path: "/api/v1/push",
        query_concurrency: QUERY_CONCURRENCY,
    }
}
