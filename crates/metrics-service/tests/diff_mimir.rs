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

use std::{net::SocketAddr, sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use futures::StreamExt;
use krabka_metrics::{
    WalRecord,
    distributor::{DistributorState, ProduceError, WalSink},
};
use krabka_promql::WalHead;
use promql_corpus::{CorpusCase, KnownDivergence, PromqlCorpus, QueryKind};
use reqwest::StatusCode;
use serde_json::Value;
use testcontainers::{
    GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::oneshot;

// `normalize` lives with the hand-written seed dataset that `grafana_integration`
// asserts against; `promql_corpus` reaches it through this module. Nothing else
// in it belongs to this suite.
#[allow(dead_code)]
#[path = "../../metrics/tests/support/diff_corpus.rs"]
mod diff_corpus;

#[path = "support/promql_corpus.rs"]
mod promql_corpus;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

/// Samples per `remote_write` request.
const SAMPLES_PER_BATCH: usize = 20_000;

/// Queries in flight against one engine.
const QUERY_CONCURRENCY: usize = 12;

/// Corpus files Mimir cannot be asked about, and why.
///
/// These are not Krabka divergences. Mimir 2.16.1 embeds an older Prometheus
/// `PromQL` than the corpus was vendored from, and the syntax these files are
/// about does not parse there at all -- every case in them would come back as a
/// parse error against a Krabka answer, which says nothing about either engine's
/// semantics. `diff_prometheus` runs all three against a Prometheus that does
/// have them.
const MIMIR_UNSUPPORTED_FILES: &[(&str, &str)] = &[
    (
        "duration_expression.test",
        "Mimir 2.16.1 has no `promql-duration-expr`, so a duration written as an expression is a \
         parse error there",
    ),
    (
        "extended_vectors.test",
        "Mimir 2.16.1 has no `promql-extended-range-selectors`, so `[3m] smoothed` and `[3m] \
         anchored` are parse errors there",
    ),
];

/// Where Mimir disagrees with Krabka for reasons that are Mimir's own.
///
/// Mimir 2.16.1 embeds a Prometheus older than the `v3.8.1` the corpus is
/// vendored from, and every entry here is that gap rather than a Krabka bug:
/// `diff_prometheus` runs the same cases against a Prometheus that has the
/// behaviour and Krabka agrees with it there. Anything Krabka gets wrong
/// against Prometheus's engine is in `promql_corpus::UPSTREAM_DIVERGENCES`
/// instead, because Mimir runs that same engine.
///
/// The contract runs both ways: a case listed here MUST disagree, so the list
/// cannot quietly become a licence.
const MIMIR_DIVERGENCES: &[KnownDivergence] = &[
    KnownDivergence {
        reason: "Mimir 2.16.1 has no type-and-unit labels, so `__type__` and `__unit__` are ordinary \
             labels there and survive every operation. Krabka drops them where Prometheus v3.8 \
             drops them, which is what `diff_prometheus` confirms.",
        cases: &[
            "type_and_unit.test:39",
            "type_and_unit.test:53",
            "type_and_unit.test:56",
            "type_and_unit.test:63",
            "type_and_unit.test:122",
            "type_and_unit.test:140",
            "type_and_unit.test:181",
            "type_and_unit.test:195",
            "type_and_unit.test:198",
            "type_and_unit.test:208",
            "type_and_unit.test:279",
        ],
    },
    KnownDivergence {
        reason: "`histogram_fraction` over a classic histogram's `_bucket` series returns an empty \
             vector on Mimir 2.16.1. Prometheus v3.8 and Krabka both compute the fraction.",
        cases: &[
            "histograms.test:119",
            "histograms.test:127",
            "histograms.test:135",
            "histograms.test:145",
            "histograms.test:156",
            "histograms.test:172",
            "histograms.test:191",
            "histograms.test:211",
            "histograms.test:228",
            "histograms.test:245",
            "histograms.test:263",
            "histograms.test:281",
            "histograms.test:300",
            "histograms.test:318",
            "histograms.test:335",
            "histograms.test:350",
            "histograms.test:365",
            "histograms.test:380",
            "histograms.test:399",
            "histograms.test:418",
            "histograms.test:437",
            "histograms.test:455",
            "histograms.test:475",
            "histograms.test:488",
            "histograms.test:501",
            "histograms.test:515",
            "histograms.test:529",
            "histograms.test:1084",
        ],
    },
    KnownDivergence {
        reason: "A native histogram carrying NaN observations is read differently: Mimir 2.16.1 \
             answers a number where Prometheus v3.8 and Krabka answer NaN. Upstream changed how \
             `histogram_quantile` and `histogram_fraction` treat NaN buckets after the Prometheus \
             that Mimir embeds.",
        cases: &[
            "native_histograms.test:1492",
            "native_histograms.test:1497",
            "native_histograms.test:1502",
            "native_histograms.test:1510",
            "native_histograms.test:1524",
        ],
    },
    KnownDivergence {
        reason: "Mimir 2.16.1 omits the NaN-observation info annotations emitted by Prometheus \
             v3.8 and Krabka; the query values are identical.",
        cases: &[
            "native_histograms.test:1506",
            "native_histograms.test:1515",
            "native_histograms.test:1519",
        ],
    },
    KnownDivergence {
        reason: "The function does not exist in Mimir 2.16.1 at all: the query comes back as `parse \
             error: unknown function`. `ts_of_first_over_time`, `ts_of_last_over_time` and \
             `first_over_time` all arrived upstream after it.",
        cases: &[
            "functions.test:1324",
            "functions.test:1327",
            "functions.test:1346",
            "functions.test:1349",
            "functions.test:1618",
            "name_label_dropping.test:47",
        ],
    },
    KnownDivergence {
        reason: "Mimir 2.16.1 renders an empty native histogram with `buckets: []`, while \
             Prometheus v3.8 and Krabka omit the empty field.",
        cases: &[
            "functions.test:1075",
            "functions.test:1078",
            "functions.test:1081",
            "functions.test:1606",
            "native_histograms.test:5",
            "native_histograms.test:989",
            "native_histograms.test:993",
            "native_histograms.test:997",
            "native_histograms.test:1001",
            "native_histograms.test:1013",
            "subquery.test:146",
            "subquery.test:151",
        ],
    },
    KnownDivergence {
        reason: "An aggregation whose parameter is an expression rather than a literal -- \
             `topk(scalar(foo), ...)` -- is evaluated once on Mimir 2.16.1 and per step on \
             Prometheus v3.8 and Krabka.",
        cases: &[
            "aggregators.test:363",
            "aggregators.test:366",
            "aggregators.test:560",
        ],
    },
    KnownDivergence {
        reason: "A parenthesised string literal fails on Mimir 2.16.1 with `unexpected result in \
             StepInvariantExpr evaluation`. Prometheus v3.8 and Krabka answer with the string.",
        cases: &["literals.test:61", "literals.test:70"],
    },
    KnownDivergence {
        reason: "`info()` joins a different `target_info` sample on Mimir 2.16.1: instance `b` comes \
             back `state=\"running\"` where Prometheus v3.8 and Krabka give `state=\"stopped\"`.",
        cases: &["info.test:142", "info.test:147"],
    },
    KnownDivergence {
        reason: "`min_over_time` over a subquery of `topk` makes Mimir 2.16.1 panic -- the query \
             answers `unexpected error: runtime error: index out of range [0] with length 0`. \
             Prometheus v3.8 and Krabka answer it.",
        cases: &["subquery.test:158"],
    },
    KnownDivergence {
        reason: "`irate` over a window whose newest sample is NaN answers 0.016667 on Mimir 2.16.1, \
             where Prometheus v3.8 and Krabka both answer NaN.",
        cases: &["functions.test:241"],
    },
];

/// Shared divergences that Mimir does not show.
///
/// `UPSTREAM_DIVERGENCES` is written against Prometheus v3.8. Where Mimir's
/// older engine still behaves the way Krabka does, the case agrees here and
/// would otherwise be reported as a divergence that had been fixed.
const MIMIR_AGREES_WITH_KRABKA: &[KnownDivergence] = &[KnownDivergence {
    reason: "Mimir 2.16.1 does not return the warning and info annotations added by newer \
             Prometheus versions, so these annotation-only upstream divergences are absent.",
    cases: &[
        "functions.test:122",
        "functions.test:135",
        "functions.test:150",
        "functions.test:161",
        "functions.test:167",
        "functions.test:178",
        "functions.test:1786",
        "functions.test:181",
        "functions.test:1862",
        "functions.test:198",
        "functions.test:207",
        "functions.test:370",
        "histograms.test:1016",
        "histograms.test:1028",
        "histograms.test:545",
        "histograms.test:702",
        "histograms.test:712",
        "histograms.test:722",
        "histograms.test:757",
        "histograms.test:765",
        "histograms.test:773",
        "histograms.test:784",
        "histograms.test:792",
        "histograms.test:802",
        "histograms.test:810",
        "histograms.test:821",
        "histograms.test:831",
        "histograms.test:842",
        "histograms.test:852",
        "histograms.test:865",
        "histograms.test:879",
        "histograms.test:894",
        "histograms.test:908",
        "histograms.test:971",
        "histograms.test:979",
        "histograms.test:983",
        "histograms.test:992",
        "name_label_dropping.test:39",
        "name_label_dropping.test:92",
        "operators.test:117",
        "operators.test:121",
        "operators.test:131",
        "selectors.test:102",
        "selectors.test:106",
        "selectors.test:13",
        "selectors.test:19",
        "selectors.test:23",
        "selectors.test:26",
        "selectors.test:32",
        "selectors.test:38",
        "selectors.test:68",
        "selectors.test:7",
        "selectors.test:72",
        "selectors.test:76",
        "selectors.test:80",
        "selectors.test:84",
        "selectors.test:89",
        "selectors.test:93",
        "selectors.test:97",
        "subquery.test:115",
        "subquery.test:119",
        "subquery.test:123",
        "subquery.test:134",
        "subquery.test:23",
        "subquery.test:34",
        "subquery.test:38",
    ],
}];

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

ingester:
  ring:
    # Monolithic single-binary has exactly one ingester; the default
    # replication factor of 3 makes the distributor reject every push with
    # 'at least 2 live replicas required, could only find 1'.
    replication_factor: 1

limits:
  native_histograms_ingestion_enabled: true
";

#[tokio::test]
#[ignore = "requires Docker"]
async fn mimir_compliance_corpus_matches_krabka() -> TestResult {
    let corpus = promql_corpus::promql_corpus()
        .without_files(MIMIR_UNSUPPORTED_FILES)
        .without_custom_bucket_histograms(
            "Mimir 2.16.1's distributor refuses a native histogram with schema -53 outright -- \
             `err-mimir-invalid-native-histogram-schema` -- so a custom-bucket histogram cannot be \
             seeded into it at all. `diff_prometheus` covers these cases.",
        );
    let client = reqwest::Client::new();

    // Real Mimir in monolithic mode.
    let mimir = start_mimir().await?;
    let mimir_base = mapped_base_url(&mimir, MIMIR_PORT).await?;
    wait_for_http_ok(&client, &mimir_base, "/ready").await?;

    // In-process Krabka write+query path (identical to diff_prometheus.rs).
    let krabka = start_krabka_query_server().await?;
    seed_both(&client, &krabka.base_url, &mimir_base, &corpus).await?;

    let mismatches = run_corpus(&client, &krabka.base_url, &mimir_base, &corpus).await?;
    promql_corpus::write_report("diff_mimir", &corpus, &mismatches, MIMIR_DIVERGENCES);
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

/// Writes the whole corpus to both engines, in batch order.
///
/// Mimir's `remote_write` receiver lives at `/api/v1/push`; Krabka's is the
/// Prometheus one.
async fn seed_both(
    client: &reqwest::Client,
    krabka_base: &str,
    mimir_base: &str,
    corpus: &PromqlCorpus,
) -> TestResult {
    let batches = promql_corpus::remote_write_batches(&corpus.series, SAMPLES_PER_BATCH);
    println!(
        "diff_mimir: seeding {} series / {} samples in {} batches",
        corpus.series.len(),
        corpus.sample_count(),
        batches.len()
    );
    // The upstream first: it is the stricter receiver of the two, and a corpus
    // shape it refuses is a fault in the seed rather than in Krabka.
    for batch in &batches {
        post_remote_write(client, mimir_base, "/api/v1/push", batch).await?;
        post_remote_write(client, krabka_base, "/api/v1/write", batch).await?;
    }

    let (probe, at) = corpus_probe(corpus).ok_or("the corpus seeded no float samples")?;
    wait_for_query_ready(client, krabka_base, "/api/v1/query", &probe, at).await?;
    wait_for_query_ready(client, mimir_base, "/prometheus/api/v1/query", &probe, at).await?;
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
    mimir_base: &str,
    corpus: &PromqlCorpus,
) -> TestResult<Vec<(String, String)>> {
    let started = std::time::Instant::now();
    let mut mismatches: Vec<(String, String)> = futures::stream::iter(corpus.cases.iter())
        .map(|case| async move {
            let krabka = query_case(client, krabka_base, "", case).await;
            let upstream = query_case(client, mimir_base, "/prometheus", case).await;
            let detail = match (krabka, upstream) {
                (Ok(krabka), Ok(upstream)) => promql_corpus::compare_case(case, &krabka, &upstream),
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
        "diff_mimir: {} cases in {:.1}s, {} skipped, {} disagreed",
        corpus.cases.len(),
        started.elapsed().as_secs_f64(),
        corpus.skipped.len(),
        mismatches.len()
    );
    Ok(mismatches)
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
            // Mimir 2.16.x logs go-kit lines to stderr; the HTTP server announces
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
    let bound = krabka_metrics_service::serve_prometheus_router(
        addr,
        router,
        &krabka_observability::server_security::ServerSecurity::default(),
        async move {
            let _ = rx.await;
        },
    )
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

async fn post_remote_write(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    body: &[u8],
) -> TestResult {
    let response = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Scope-OrgID", TENANT)
        .body(body.to_vec())
        .send()
        .await?;
    let status = response.status();
    if !(status == StatusCode::OK || status == StatusCode::NO_CONTENT) {
        let detail = response.text().await.unwrap_or_default();
        return Err(format!("remote_write to {base}{path} returned {status}: {detail}").into());
    }
    Ok(())
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, path: &str) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
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
    query_path: &str,
    query: &str,
    at_ms: i64,
) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        let json = query_instant(client, base, query_path, query, at_ms).await?;
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
    case: &CorpusCase,
) -> TestResult<Value> {
    match case.kind {
        QueryKind::Instant { time } => {
            query_instant(
                client,
                base,
                &format!("{prefix}/api/v1/query"),
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
    promql: &str,
    time_ms: i64,
) -> TestResult<Value> {
    json_body(
        client
            .get(query_url(
                base,
                path,
                &[
                    ("query", promql.to_string()),
                    ("time", seconds_param(time_ms)),
                ],
            ))
            .header("X-Scope-OrgID", TENANT),
    )
    .await
}

async fn query_range(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    promql: &str,
    range: (i64, i64, i64),
) -> TestResult<Value> {
    let (start_ms, end_ms, step_ms) = range;
    json_body(
        client
            .get(query_url(
                base,
                path,
                &[
                    ("query", promql.to_string()),
                    ("start", seconds_param(start_ms)),
                    ("end", seconds_param(end_ms)),
                    ("step", seconds_param(step_ms)),
                ],
            ))
            .header("X-Scope-OrgID", TENANT),
    )
    .await
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
