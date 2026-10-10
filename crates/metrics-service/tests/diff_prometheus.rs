#![recursion_limit = "512"]

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

use std::time::Duration;

use assert2::assert;
use krabka_promql::WalHead;
use promql_corpus::{CorpusCase, KnownDivergence, QueryKind};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

// `seed_dataset` is the small hand-written dataset that `grafana_integration`
// asserts against; this suite reuses it for the plain remote-write smoke test
// below, and reuses `normalize` through `promql_corpus`. The rest of the module
// belongs to the other suites.
#[path = "support/corpus_differential.rs"]
mod corpus_differential;
#[path = "support/seed_remote_write.rs"]
mod seed_remote_write;
#[path = "support/upstream_http.rs"]
mod upstream_http;

use self::{
    corpus_differential::{CorpusDiff, PromApi, SAMPLES_PER_BATCH},
    seed_remote_write::{remote_write_body, remote_write_labels},
    upstream_http::{
        KrabkaServer, RemoteWrite, TestResult, mapped_base_url, post_remote_write, wait_for_http_ok,
    },
};

#[allow(dead_code)]
#[path = "../../metrics/tests/support/diff_corpus.rs"]
mod diff_corpus;

// The two Docker-backed PromQL suites share this corpus support module.
#[allow(dead_code)]
#[path = "support/promql_corpus.rs"]
mod promql_corpus;

#[path = "support/compliance_fixture.rs"]
mod compliance_fixture;

#[path = "support/generated_differential.rs"]
mod generated_differential;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

const TENANT: &str = "compliance";
const PROMETHEUS_PORT: u16 = 9090;

/// Queries in flight against one engine.
///
/// The corpus is large enough that a serial walk of it dominates the run, and
/// small enough that neither side needs protecting from twelve at once.
const QUERY_CONCURRENCY: usize = 12;

/// The Prometheus feature flags the corpus needs.
///
/// Native histograms, experimental functions, duration expressions, extended
/// range selectors and type/unit labels enable the corresponding corpus
/// features. Delayed name removal changes composed-expression semantics and
/// matches the pinned upstream test engine and the candidate's behavior.
const PROMETHEUS_FEATURES: &str = "native-histograms,promql-experimental-functions,\
     promql-duration-expr,promql-extended-range-selectors,type-and-unit-labels,promql-delayed-name-removal";

/// Query changes observed when the client oracle moved from Prometheus 3.8 to
/// 3.14. The list is bidirectional: the suite fails if any case starts agreeing
/// again, so each difference remains an explicit compatibility decision.
const PROMETHEUS_DIVERGENCES: &[KnownDivergence] = &[KnownDivergence {
    reason: "Prometheus 3.14 rejects nested duration expressions that 3.8 and Krabka accept.",
    cases: &[
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
    ],
}];

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
        RemoteWrite {
            base: &krabka.base_url,
            path: "/api/v1/write",
            tenant: Some(TENANT),
            body: &remote_write,
        },
    )
    .await?;
    PromApi {
        base: &krabka.base_url,
        prefix: "",
        tenant: Some(TENANT),
    }
    .wait_for_query_ready(&client, "up", 45_000)
    .await?;

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
    wait_for_http_ok(
        &client,
        &format!("{prometheus_base}/-/ready"),
        Duration::from_secs(15),
    )
    .await?;

    let krabka = start_krabka_query_server().await?;
    prometheus_diff(&krabka.base_url, &prometheus_base)
        .seed(&client, &corpus)
        .await?;

    let mismatches = prometheus_diff(&krabka.base_url, &prometheus_base)
        .run(&client, &corpus)
        .await?;
    promql_corpus::write_report(
        "diff_prometheus",
        &corpus,
        &mismatches,
        PROMETHEUS_DIVERGENCES,
        &promql_corpus::ReportConfiguration {
            oracle_flags: vec![format!("--enable-feature={PROMETHEUS_FEATURES}")],
            candidate_engine_opts: krabka_promql::EngineOpts::default(),
        },
    );
    krabka.shutdown();

    let verdict =
        promql_corpus::check_divergences(&corpus, &mismatches, PROMETHEUS_DIVERGENCES, &[]);
    assert!(
        verdict.is_none(),
        "the differential and the known-divergence list disagree:\n{}",
        verdict.unwrap_or_default()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the pinned Prometheus compliance executable"]
async fn upstream_promql_http_compliance_matches_krabka() -> TestResult {
    let executable = std::env::var_os("KRABKA_PROMQL_COMPLIANCE_BIN")
        .ok_or("KRABKA_PROMQL_COMPLIANCE_BIN is required")?;
    let queries = std::env::var_os("KRABKA_PROMQL_COMPLIANCE_QUERIES")
        .ok_or("KRABKA_PROMQL_COMPLIANCE_QUERIES is required")?;
    // Check inputs before starting any containers. Missing inputs are errors.
    std::fs::metadata(&executable)?;
    std::fs::read(&queries)?;
    let client = reqwest::Client::new();
    let prometheus = start_prometheus().await?;
    let prometheus_base = mapped_base_url(&prometheus, PROMETHEUS_PORT).await?;
    wait_for_http_ok(
        &client,
        &format!("{prometheus_base}/-/ready"),
        Duration::from_secs(15),
    )
    .await?;
    let krabka = start_krabka_query_server().await?;
    for batch in compliance_fixture::batches() {
        post_remote_write(
            &client,
            RemoteWrite {
                base: &prometheus_base,
                path: "/api/v1/write",
                tenant: None,
                body: &batch,
            },
        )
        .await?;
        post_remote_write(
            &client,
            RemoteWrite {
                base: &krabka.base_url,
                path: "/api/v1/write",
                tenant: Some(TENANT),
                body: &batch,
            },
        )
        .await?;
    }
    for (base, tenant) in [(&prometheus_base, None), (&krabka.base_url, Some(TENANT))] {
        PromApi {
            base,
            prefix: "",
            tenant,
        }
        .wait_for_query_ready(
            &client,
            "demo_memory_usage_bytes",
            compliance_fixture::END_MS,
        )
        .await?;
    }
    let directory = tempfile::tempdir()?;
    let config_path = directory.path().join("targets.yml");
    let config = serde_json::json!({
        "reference_target_config": {"query_url": prometheus_base},
        "test_target_config": {"query_url": krabka.base_url, "headers": {"X-Scope-OrgID": TENANT}},
        "query_time_parameters": {
            "end_time": (compliance_fixture::END_MS / 1_000).to_string(), "range_in_seconds": 600, "resolution_in_seconds": 10,
        },
    });
    std::fs::write(&config_path, serde_yaml::to_string(&config)?)?;
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(executable)
            .arg("-config-file")
            .arg(config_path)
            .arg("-config-file")
            .arg(queries)
            .arg("-output-format=json")
            .arg("-output-passing=true")
            .arg("-query-parallelism=8")
            .output()
    })
    .await??;
    let counts = write_compliance_report(&output)?;
    let report_dir = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(
        || std::path::PathBuf::from("../../target"),
        std::path::PathBuf::from,
    );
    generated_differential::run_typed(
        "promql",
        &[
            generated_differential::TypedExpr::prom_metric(
                "demo_memory_usage_bytes",
                &[generated_differential::LabelMatcher::new(
                    "type",
                    generated_differential::MatchOp::Eq,
                    "free",
                )],
            ),
            generated_differential::TypedExpr::prom_metric("demo_cpu_usage_seconds_total", &[]),
            generated_differential::TypedExpr::prom_metric("demo_num_cpus", &[]),
        ],
        &[
            generated_differential::TypedConstructor::PromAbs,
            generated_differential::TypedConstructor::PromSum { by: vec![] },
            generated_differential::TypedConstructor::PromSum {
                by: vec!["instance".into()],
            },
            generated_differential::TypedConstructor::PromAdd(1),
            generated_differential::TypedConstructor::PromClampMin(0),
        ],
        &report_dir,
        |query| {
            let client = &client;
            let krabka_base = &krabka.base_url;
            let prometheus_base = &prometheus_base;
            async move {
                let case = CorpusCase {
                    name: "generated seed-42 composition".to_owned(),
                    promql: query,
                    kind: QueryKind::Range {
                        start: compliance_fixture::END_MS - 600_000,
                        end: compliance_fixture::END_MS,
                        step: 10_000,
                    },
                    expects_failure: false,
                };
                let krabka = PromApi {
                    base: krabka_base,
                    prefix: "",
                    tenant: Some(TENANT),
                }
                .query_case(client, &case)
                .await?;
                let upstream = PromApi {
                    base: prometheus_base,
                    prefix: "",
                    tenant: None,
                }
                .query_case(client, &case)
                .await?;
                if krabka["status"] != "success" || upstream["status"] != "success" {
                    return Ok(Some(format!(
                        "valid composition `{}` was rejected: krabka={krabka}; upstream={upstream}",
                        case.promql
                    )));
                }
                Ok(promql_corpus::compare_case(&case, &krabka, &upstream))
            }
        },
    )
    .await?;
    generated_differential::run(
        "promql-rejections",
        &[
            "abs(demo_num_cpus,0)",
            "scalar(demo_num_cpus,0)",
            "sum_over_time(demo_num_cpus)",
        ],
        &["sum({expr})", "abs({expr})", "sum by(instance)({expr})"],
        &report_dir,
        |query| {
            let client = &client;
            let krabka_base = &krabka.base_url;
            let prometheus_base = &prometheus_base;
            async move {
                let case = CorpusCase {
                    name: "generated seed-42 rejection".to_owned(),
                    promql: query,
                    kind: QueryKind::Instant { time: compliance_fixture::END_MS },
                    expects_failure: true,
                };
                // First establish that this is an invalid query in the pinned
                // oracle. Two successful replies must never qualify a refusal.
                let upstream = PromApi { base: prometheus_base, prefix: "", tenant: None }.query_case(client, &case).await?;
                if upstream["status"] != "error"
                    || !matches!(upstream["errorType"].as_str(), Some("bad_data" | "execution"))
                {
                    return Ok(Some(format!(
                        "invalid composition `{}` was not rejected by the oracle: {upstream}",
                        case.promql
                    )));
                }
                let krabka = PromApi { base: krabka_base, prefix: "", tenant: Some(TENANT) }.query_case(client, &case).await?;
                if krabka["status"] != "error" {
                    return Ok(Some(format!(
                        "invalid composition `{}` was accepted by Krabka: {krabka}; upstream={upstream}",
                        case.promql
                    )));
                }
                Ok(promql_corpus::compare_case(&case, &krabka, &upstream))
            }
        },
    )
    .await?;
    krabka.shutdown();
    assert!(counts.compared > 0, "no response payloads were compared");
    assert!(
        counts.skipped == 0,
        "skipped comparisons are not conformance: {counts:?}"
    );
    assert!(
        counts.mismatched == 0,
        "upstream HTTP conformance mismatches: {counts:?}"
    );
    assert!(
        output.status.success(),
        "compliance tester failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    println!("upstream PromQL HTTP compliance: {counts:?}");
    Ok(())
}

fn write_compliance_report(
    output: &std::process::Output,
) -> TestResult<compliance_fixture::ReportCounts> {
    let report_dir = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(
        || std::path::PathBuf::from("../../target"),
        std::path::PathBuf::from,
    );
    std::fs::create_dir_all(&report_dir)?;
    std::fs::write(
        report_dir.join("promql-http-compliance.json"),
        &output.stdout,
    )?;
    std::fs::write(
        report_dir.join("promql-http-compliance.stderr.txt"),
        &output.stderr,
    )?;
    let counts = compliance_fixture::report_counts(&output.stdout)?;
    std::fs::write(
        report_dir.join("promql-http-compliance-summary.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "compared_payloads": counts.compared,
            "paired_expected_errors": counts.paired_errors,
            "skipped_comparisons": counts.skipped,
            "mismatches": counts.mismatched,
            "end_time_seconds": compliance_fixture::END_MS / 1_000,
        }))?,
    )?;
    Ok(counts)
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
            // Epoch-based corpus samples must not race the image's self-scrape.
            .with_copy_to("/etc/prometheus/prometheus.yml", b"global: {}\n".to_vec())
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

async fn start_krabka_query_server() -> TestResult<KrabkaServer> {
    let head = WalHead::new();
    let query_router = krabka_metrics_service::prometheus_router_for_store(head.clone());
    KrabkaServer::start(query_router, head, "127.0.0.1:0".parse()?).await
}

fn prometheus_diff<'a>(krabka_base: &'a str, prometheus_base: &'a str) -> CorpusDiff<'a> {
    CorpusDiff {
        suite: "diff_prometheus",
        krabka: PromApi {
            base: krabka_base,
            prefix: "",
            tenant: Some(TENANT),
        },
        upstream: PromApi {
            base: prometheus_base,
            prefix: "",
            tenant: None,
        },
        upstream_write_path: "/api/v1/write",
        query_concurrency: QUERY_CONCURRENCY,
    }
}
