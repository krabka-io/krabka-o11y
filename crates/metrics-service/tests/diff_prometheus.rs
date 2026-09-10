//! Docker-backed differential probe against real Prometheus.
//!
//! The corpus is the vendored upstream `promql/promqltest` suite, replayed
//! through two HTTP APIs rather than through one engine: `support/promql_corpus.rs`
//! turns every `load` block into a `remote_write` body and every `eval` line
//! into a query, both engines are handed the same bytes, and the two answers are
//! compared. What the `.test` file says the answer *should* be is never read --
//! the oracle here is the running Prometheus, not the file.
//!
//! Cargo ignores the differential by default, because it pulls and runs
//! `mirror.gcr.io/prom/prometheus`.
//! Run with:
//!
//! `cargo test -p krabka-metrics-service --test diff_prometheus -- --ignored --nocapture`

use std::{net::SocketAddr, sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use diff_corpus::seed_dataset;
use futures::StreamExt;
use krabka_metrics::{
    WalRecord,
    distributor::{DistributorState, ProduceError, WalSink},
    wire::pb,
};
use krabka_promql::WalHead;
use promql_corpus::{CorpusCase, PromqlCorpus, QueryKind};
use prost::Message;
use reqwest::StatusCode;
use serde_json::Value;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::oneshot;

// `seed_dataset` is the small hand-written dataset that `grafana_integration`
// asserts against; this suite reuses it for the plain remote-write smoke test
// below, and reuses `normalize` through `promql_corpus`. The rest of the module
// belongs to the other suites.
#[allow(dead_code)]
#[path = "../../metrics/tests/support/diff_corpus.rs"]
mod diff_corpus;

// `without_files` belongs to `diff_mimir`, which has an upstream that cannot be
// asked about every file; this suite runs the corpus whole.
#[allow(dead_code)]
#[path = "support/promql_corpus.rs"]
mod promql_corpus;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

const TENANT: &str = "compliance";
const PROMETHEUS_PORT: u16 = 9090;

/// Samples per `remote_write` request.
///
/// The whole corpus is a few hundred thousand samples, which is past what
/// either receiver will decode in one body.
const SAMPLES_PER_BATCH: usize = 20_000;

/// Queries in flight against one engine.
///
/// The corpus is large enough that a serial walk of it dominates the run, and
/// small enough that neither side needs protecting from twelve at once.
const QUERY_CONCURRENCY: usize = 12;

/// The Prometheus feature flags the corpus needs.
///
/// Every one of these gates a *file* of the vendored corpus rather than
/// changing how the rest of `PromQL` behaves: `info()` and the
/// `double_exponential_smoothing` family are experimental functions,
/// `duration_expression.test` needs duration expressions, `extended_vectors.test`
/// needs the extended range selectors, `type_and_unit.test` needs `__type__` and
/// `__unit__` to be understood rather than treated as ordinary labels, and the
/// histogram corpora need native histograms.
const PROMETHEUS_FEATURES: &str = "native-histograms,promql-experimental-functions,\
     promql-duration-expr,promql-extended-range-selectors,type-and-unit-labels";

/// The corpus builds, and every case it declines to run says why.
///
/// This runs without Docker, so a corpus that has drifted away from the
/// vendored `.test` files fails the ordinary `bazel test //...` rather than
/// waiting for the container job.
#[test]
fn the_differential_corpus_is_large_and_every_skip_is_explained() {
    let corpus = promql_corpus::promql_corpus();
    println!(
        "corpus: {} series, {} samples, {} cases, {} skipped",
        corpus.series.len(),
        corpus.sample_count(),
        corpus.cases.len(),
        corpus.skipped.len()
    );

    // The corpus this replaced held nine queries over eleven series. The floor
    // is not the exact count -- vendoring a further upstream file should not
    // fail the build -- but it is far enough above nine that a corpus which
    // quietly stopped loading cannot pass.
    assert!(corpus.cases.len() > 1_000);
    assert!(corpus.series.len() > 100);
    assert!(corpus.skipped.iter().all(|case| !case.reason.is_empty()));

    // A skipped case is still nameable, and no name is claimed twice.
    let mut names: Vec<&str> = corpus
        .cases
        .iter()
        .map(|case| case.name.as_str())
        .chain(corpus.skipped.iter().map(|case| case.name.as_str()))
        .collect();
    let total = names.len();
    names.sort_unstable();
    names.dedup();
    assert!(names.len() == total);
}

#[tokio::test]
async fn krabka_remote_write_endpoint_feeds_query_store() -> TestResult {
    let client = reqwest::Client::new();
    let krabka = start_krabka_query_server().await?;
    let remote_write = remote_write_body();

    post_remote_write(
        &client,
        &krabka.base_url,
        "/api/v1/write",
        Some(TENANT),
        &remote_write,
    )
    .await?;
    wait_for_query_ready(&client, &krabka.base_url, Some(TENANT), "up", 45_000).await?;

    krabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn prometheus_compliance_corpus_matches_krabka() -> TestResult {
    let corpus = promql_corpus::promql_corpus();
    let client = reqwest::Client::new();
    let prometheus = start_prometheus().await?;
    let prometheus_base = mapped_base_url(&prometheus, PROMETHEUS_PORT).await?;
    wait_for_http_ok(&client, &prometheus_base, "/-/ready").await?;

    let krabka = start_krabka_query_server().await?;
    seed_both(&client, &krabka.base_url, &prometheus_base, &corpus).await?;

    let mismatches = run_corpus(&client, &krabka.base_url, &prometheus_base, &corpus).await?;
    promql_corpus::write_report("diff_prometheus", &corpus, &mismatches, &[]);
    krabka.shutdown();

    let verdict = promql_corpus::check_divergences(&corpus, &mismatches, &[], &[]);
    assert!(
        verdict.is_none(),
        "the differential and the known-divergence list disagree:\n{}",
        verdict.unwrap_or_default()
    );
    Ok(())
}

/// Writes the whole corpus to both engines, in batch order.
async fn seed_both(
    client: &reqwest::Client,
    krabka_base: &str,
    prometheus_base: &str,
    corpus: &PromqlCorpus,
) -> TestResult {
    let batches = promql_corpus::remote_write_batches(&corpus.series, SAMPLES_PER_BATCH);
    println!(
        "diff_prometheus: seeding {} series / {} samples in {} batches",
        corpus.series.len(),
        corpus.sample_count(),
        batches.len()
    );
    // The upstream first: it is the stricter receiver of the two, and a corpus
    // shape it refuses is a fault in the seed rather than in Krabka.
    for batch in &batches {
        post_remote_write(client, prometheus_base, "/api/v1/write", None, batch).await?;
        post_remote_write(client, krabka_base, "/api/v1/write", Some(TENANT), batch).await?;
    }

    let (probe, at) = corpus_probe(corpus).ok_or("the corpus seeded no float samples")?;
    wait_for_query_ready(client, krabka_base, Some(TENANT), &probe, at).await?;
    wait_for_query_ready(client, prometheus_base, None, &probe, at).await?;
    Ok(())
}

/// A selector and timestamp that must return something once the seed has
/// landed, taken from the corpus rather than assumed.
fn corpus_probe(corpus: &PromqlCorpus) -> Option<(String, i64)> {
    let series = corpus
        .series
        .iter()
        .find(|series| !series.floats.is_empty())?;
    let selector = series
        .labels
        .iter()
        .map(|(name, value)| format!("{name}={}", quoted(value)))
        .collect::<Vec<_>>()
        .join(",");
    Some((format!("{{{selector}}}"), series.floats.first()?.0))
}

fn quoted(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Runs every case against both engines and returns the disagreements.
async fn run_corpus(
    client: &reqwest::Client,
    krabka_base: &str,
    prometheus_base: &str,
    corpus: &PromqlCorpus,
) -> TestResult<Vec<(String, String)>> {
    let started = std::time::Instant::now();
    let mut mismatches: Vec<(String, String)> = futures::stream::iter(corpus.cases.iter())
        .map(|case| async move {
            let krabka = query_case(client, krabka_base, "", Some(TENANT), case).await;
            let upstream = query_case(client, prometheus_base, "", None, case).await;
            let detail = match (krabka, upstream) {
                (Ok(krabka), Ok(upstream)) => {
                    promql_corpus::compare_case(case, &krabka, &upstream)
                }
                (krabka, upstream) => Some(format!(
                    "{} `{}`: transport failure\n      krabka:   {krabka:?}\n      upstream: {upstream:?}",
                    case.name, case.promql
                )),
            };
            detail.map(|detail| (case.name.clone(), detail))
        })
        .buffer_unordered(QUERY_CONCURRENCY)
        .filter_map(|mismatch| async move { mismatch })
        .collect()
        .await;
    mismatches.sort();
    println!(
        "diff_prometheus: {} cases in {:.1}s, {} skipped, {} disagreed",
        corpus.cases.len(),
        started.elapsed().as_secs_f64(),
        corpus.skipped.len(),
        mismatches.len()
    );
    Ok(mismatches)
}

async fn start_prometheus() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagreed
    // testcontainers pulled the image over the network and the suite compared
    // against whatever it got rather than against the pinned bytes.
    let tag = std::env::var("KRABKA_PROMETHEUS_IMAGE_TAG")
        .expect(
            "KRABKA_PROMETHEUS_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under \
         cargo, set it to that image's tag in //bazel/images/images.bzl.",
        );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/prom/prometheus".to_string(), tag)
            .with_exposed_port(PROMETHEUS_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "Server is ready to receive web requests",
            ))
            .with_cmd([
                "--config.file=/etc/prometheus/prometheus.yml",
                "--storage.tsdb.path=/prometheus",
                "--web.enable-remote-write-receiver",
                &format!("--enable-feature={PROMETHEUS_FEATURES}"),
                // The corpus lays its segments out over roughly three weeks of
                // simulated time, starting at the epoch. The default fifteen
                // days of retention would make the oldest segments eligible for
                // deletion the moment the head is first compacted.
                "--storage.tsdb.retention.time=1000d",
            ])
            .start(),
    )
    .await??)
}

async fn mapped_base_url(
    container: &testcontainers::ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port.tcp()).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

struct KrabkaServer {
    base_url: String,
    shutdown: Option<oneshot::Sender<()>>,
}

impl KrabkaServer {
    fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn start_krabka_query_server() -> TestResult<KrabkaServer> {
    let head = WalHead::new();
    let query_router = krabka_metrics_service::prometheus_router_for_store(head.clone());
    let sink: Arc<dyn WalSink> = Arc::new(WalHeadSink { head });
    let distributor = Arc::new(DistributorState::new(sink));
    let router = query_router.merge(krabka_metrics::distributor::router(distributor));
    let (tx, rx) = oneshot::channel();
    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let bound = krabka_metrics_service::serve_prometheus_router(addr, router, async move {
        let _ = rx.await;
    })
    .await?;

    Ok(KrabkaServer {
        base_url: format!("http://{bound}"),
        shutdown: Some(tx),
    })
}

struct WalHeadSink {
    head: WalHead,
}

#[async_trait::async_trait]
impl WalSink for WalHeadSink {
    async fn append(&self, _key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.head.apply_wal_record(&record);
        Ok(())
    }
}

fn remote_write_body() -> Vec<u8> {
    let req = pb::v1::WriteRequest {
        timeseries: seed_dataset()
            .into_iter()
            .map(|point| pb::v1::TimeSeries {
                labels: remote_write_labels(point.metric, point.labels),
                samples: point
                    .samples
                    .iter()
                    .map(|(timestamp, value)| pb::v1::Sample {
                        value: *value,
                        timestamp: *timestamp,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy remote_write")
}

fn remote_write_labels(metric: &str, labels: &[(&str, &str)]) -> Vec<pb::v1::Label> {
    std::iter::once(pb::v1::Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    })
    .chain(labels.iter().map(|(name, value)| pb::v1::Label {
        name: (*name).to_string(),
        value: (*value).to_string(),
    }))
    .collect()
}

async fn post_remote_write(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    tenant: Option<&str>,
    body: &[u8],
) -> TestResult {
    let mut request = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body.to_vec());
    if let Some(tenant) = tenant {
        request = request.header("X-Scope-OrgID", tenant);
    }
    let response = request.send().await?;
    let status = response.status();
    if !(status == StatusCode::OK || status == StatusCode::NO_CONTENT) {
        let detail = response.text().await.unwrap_or_default();
        return Err(format!("remote_write to {base}{path} returned {status}: {detail}").into());
    }
    Ok(())
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, path: &str) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        if client
            .get(format!("{base}{path}"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(format!("{base}{path} did not become ready").into())
}

async fn wait_for_query_ready(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    query: &str,
    at_ms: i64,
) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        let json = query_instant(client, base, "/api/v1/query", tenant, query, at_ms).await?;
        if json["data"]["result"]
            .as_array()
            .is_some_and(|result| !result.is_empty())
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(format!("query `{query}` did not become non-empty on {base}").into())
}

async fn query_case(
    client: &reqwest::Client,
    base: &str,
    prefix: &str,
    tenant: Option<&str>,
    case: &CorpusCase,
) -> TestResult<Value> {
    match case.kind {
        QueryKind::Instant { time } => {
            query_instant(
                client,
                base,
                &format!("{prefix}/api/v1/query"),
                tenant,
                &case.promql,
                time,
            )
            .await
        }
        QueryKind::Range { start, end, step } => {
            query_range(
                client,
                base,
                &format!("{prefix}/api/v1/query_range"),
                tenant,
                &case.promql,
                (start, end, step),
            )
            .await
        }
    }
}

async fn query_instant(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    tenant: Option<&str>,
    promql: &str,
    time_ms: i64,
) -> TestResult<Value> {
    let mut request = client.get(query_url(
        base,
        path,
        &[
            ("query", promql.to_string()),
            ("time", seconds_param(time_ms)),
        ],
    ));
    if let Some(tenant) = tenant {
        request = request.header("X-Scope-OrgID", tenant);
    }
    json_body(request).await
}

async fn query_range(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    tenant: Option<&str>,
    promql: &str,
    range: (i64, i64, i64),
) -> TestResult<Value> {
    let (start_ms, end_ms, step_ms) = range;
    let mut request = client.get(query_url(
        base,
        path,
        &[
            ("query", promql.to_string()),
            ("start", seconds_param(start_ms)),
            ("end", seconds_param(end_ms)),
            ("step", seconds_param(step_ms)),
        ],
    ));
    if let Some(tenant) = tenant {
        request = request.header("X-Scope-OrgID", tenant);
    }
    json_body(request).await
}

/// The JSON body of a query response, whatever its status.
///
/// A corpus case the upstream file marks `expect fail` is answered with 400 or
/// 422 and a body that says which kind of failure it was, and that body is the
/// thing being compared. `error_for_status` would throw it away.
async fn json_body(request: reqwest::RequestBuilder) -> TestResult<Value> {
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    serde_json::from_str(&text)
        .map_err(|error| format!("{status} response was not JSON: {error}: {text}").into())
}

fn query_url(base: &str, path: &str, params: &[(&str, String)]) -> String {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params.iter().map(|(name, value)| (*name, value.as_str())))
        .finish();
    format!("{base}{path}?{query}")
}

fn seconds_param(ms: i64) -> String {
    let sign = if ms < 0 { "-" } else { "" };
    let abs_ms = i128::from(ms).abs();
    format!("{sign}{}.{:03}", abs_ms / 1000, abs_ms % 1000)
}
