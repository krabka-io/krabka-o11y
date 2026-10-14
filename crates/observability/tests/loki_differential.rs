//! Docker-backed differential probe against real Grafana Loki.
//!
//! Metrics, traces and profiles each check themselves against the upstream
//! component in a container: `diff_mimir`, `diff_prometheus`,
//! `tempo_differential` and `pyroscope_differential`. This suite does the same
//! for the logs path. That path is the largest of the four: a Loki push
//! receiver that speaks JSON, snappy-protobuf and OTLP, structured metadata, a
//! `LogQL` engine, the analytics endpoints and the ops endpoints.
//!
//! The structure follows `diff_prometheus.rs` and `diff_mimir.rs`. One corpus
//! is written to both a Loki container and this crate's own routers. Then the
//! same requests go to both, and a comparison removes only the fields that are
//! legitimately volatile.
//!
//! Cargo ignores this test by default, because it runs
//! `mirror.gcr.io/grafana/loki` under Docker. Run with:
//!
//! `cargo test -p krabka-observability --test loki_differential -- --ignored --nocapture`
//!
//! One test, not many. A container start costs tens of seconds, and a
//! differential is most useful when it reports *every* case that disagrees
//! rather than stopping at the first. Each case is compared, the disagreements
//! are collected, and the assertion at the end prints all of them.
//!
//! The cases run in corpus order against one Loki. The reads come first. The
//! pushes that the corpus makes on purpose, and the ops calls that change
//! Loki's state (`/log_level`, the delete API, the ingester controls), come
//! last. A case that must not see another case's data uses a tenant of its own.

mod support;

#[path = "support/loki_experimental_queries.rs"]
mod loki_experimental_queries;

#[path = "support/loki_remote_fixture.rs"]
mod loki_remote_fixture;

#[path = "support/rejection_responses.rs"]
mod rejection_responses;

#[path = "../../metrics-service/tests/support/generated_differential.rs"]
mod generated_differential;

#[path = "../../logql/tests/support/template_functions.rs"]
mod template_functions;

use std::{
    borrow::Cow,
    fmt::Write as _,
    io::Write as _,
    sync::Mutex,
    time::{Duration, Instant},
};

use assert2::assert;
use futures_util::StreamExt as _;
use krabka_blockstore::{LabelIndex, LogBlockIndex as BlockIndex};
use krabka_observability::{
    InMemoryWalSink, Limits, LogLevelControl, OverridesProvider, QuerierIndexSource, QuerierState,
    Role, ServiceConfig, ServiceDependencies, build_service_router,
    distributor_router_with_overrides, json_logging_layer, loki_router,
};
use prost::Message as _;
use rejection_responses::{ImplementationAnswer, record_rejection_response};
use reqwest::Method;
use serde_json::{Value, json};
use support::{
    LokiProtoEntry, LokiProtoLabelPair, LokiProtoPushRequest, LokiProtoStream, LokiProtoTimestamp,
};
use testcontainers::{GenericImage, ImageExt, core::IntoContainerPort, runners::AsyncRunner};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WebSocketError, Message, client::IntoClientRequest as _},
};
use tracing_subscriber::{Registry, layer::SubscriberExt as _, util::SubscriberInitExt as _};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The test's undeclared outputs directory, which is `target` when Bazel does
/// not name one, created if it is missing.
fn test_output_dir() -> TestResult<std::path::PathBuf> {
    let output = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(
        || std::path::PathBuf::from("../../target"),
        std::path::PathBuf::from,
    );
    std::fs::create_dir_all(&output)?;
    Ok(output)
}

/// Writes `report` as pretty JSON to `file_name` in [`test_output_dir`].
fn write_test_output(file_name: &str, report: &impl serde::Serialize) -> TestResult {
    std::fs::write(
        test_output_dir()?.join(file_name),
        serde_json::to_vec_pretty(report)?,
    )?;
    Ok(())
}

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// How long Loki gets to report itself ready.
///
/// A single-binary Loki binds its HTTP port well before the ingester ring is
/// usable, and `/ready` answers 503 with "waiting for 15s after being ready"
/// until it is.
const READY_TIMEOUT: Duration = Duration::from_mins(2);

/// How long a tail case waits for the first frame.
const TAIL_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a live tail case waits between the upgrade and its push.
///
/// Loki registers a tail with its ingesters after it answers the upgrade. An
/// entry pushed before that reaches the tail through the history query
/// instead, and arrives shaped as a replayed entry.
const LIVE_TAIL_SETTLE: Duration = Duration::from_secs(2);

/// The tenant that the seeded corpus lives in.
///
/// `auth_enabled: true` below makes Loki require `X-Scope-OrgID` on every push
/// and query, which is the header Krabka's distributor and querier key on too.
/// Leaving Loki's default of `auth_enabled: false` would have let the two sides
/// disagree about tenancy without the corpus noticing.
const TENANT: &str = "compliance";

/// The tenant that holds the lines the parser-error cases read.
///
/// A tenant of its own, so that lines built to fail a parser do not change the
/// answer of a case that reads the main corpus.
const PARSER_TENANT: &str = "parsers";

/// A tenant nothing is ever pushed to, for the empty-answer cases.
const VACANT_TENANT: &str = "vacant";

/// The tenant the push cases write to.
///
/// Several push cases are accepted, and the entries they write must not reach
/// the answer of a read case.
const PUSH_TENANT: &str = "pushes";

/// The tenant the delete lifecycle runs in.
const DELETE_TENANT: &str = "deletes";

/// Loki's HTTP port in single-binary mode.
const LOKI_PORT: u16 = 3100;

/// The placeholder a delete case puts where the request id goes.
///
/// Each side mints its own id. A case that cancels a request names this
/// placeholder, and the probe replaces it with the id that side last listed.
const REQUEST_ID: &str = "REQUEST_ID";

/// A range end 721 hours after the epoch: one hour past Loki's default
/// `max_query_length` of 721h, measured from a start of zero.
const OVERSIZED_END_NS: &str = "2595601000000000";

/// Cases where Krabka and Loki disagree, each with the reason.
///
/// A listed case still runs, and its disagreement is what is expected of it:
/// the suite fails when a listed case *starts* agreeing, so an entry cannot
/// quietly outlive the divergence it describes. Each reason names the
/// behaviour, not the symptom, so that removing the entry is the same work as
/// reading it.
///
/// Do not loosen a normalizer to make a case pass. Each one drops only what is
/// measured on one side's own storage, clock or build.
const LOKI_KNOWN_DIVERGENCE: &[Divergence] = &[];

/// The request header that asks for the `categorize-labels` encoding.
///
/// Loki has two JSON encodings for a `streams` answer, and the corpus asks for
/// both. The default folds an entry's structured metadata and its parsed labels
/// into the stream's label map and leaves the entry two elements long, so one
/// pushed stream comes back as one stream per distinct metadata value. This
/// header switches to the other: the stream keeps only its own labels, and each
/// entry grows a third element, `{"structuredMetadata": {...}, "parsed":
/// {...}}`, an envelope rather than a bare map. That is also where
/// `detected_level` shows up, since Loki's level discovery writes it as
/// structured metadata.
const CATEGORIZE_LABELS: &str = "categorize-labels";

/// The header that carries [`CATEGORIZE_LABELS`].
const ENCODING_FLAGS_HEADER: &str = "X-Loki-Response-Encoding-Flags";

/// Single-binary Loki, configured on the local filesystem.
///
/// `log_level: info` is Krabka's own default level, so `GET /log_level` asks
/// the same question of both sides.
///
/// The discovery limits (`discover_service_name`, `discover_log_levels`) stay
/// at Loki's defaults on purpose: Krabka implements both, so leaving them on is
/// the stronger test.
///
/// The compactor's delete API and the pattern ingester are switched on. Both
/// are off in Loki's defaults, and Krabka serves both, so a default Loki could
/// only ever answer 404 where Krabka answers.
const LOKI_CONFIG: &str = r"
auth_enabled: true

server:
  http_listen_port: 3100
  grpc_listen_port: 9096
  log_level: info

common:
  instance_addr: 127.0.0.1
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory

schema_config:
  configs:
    - from: 2020-10-24
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h

compactor:
  working_directory: /loki/compactor
  retention_enabled: true
  delete_request_store: filesystem

# Historical fixture records are still in the freshly started ingester.
# Query all of them rather than applying Loki's default three-hour cutoff.
querier:
  query_ingesters_within: 0s

ingester:
  wal:
    dir: /dev/shm/loki-wal

pattern_ingester:
  enabled: true

limits_config:
  # Structured metadata is half of what this suite is here to compare, and Loki
  # refuses a push carrying it unless this is on.
  allow_structured_metadata: true
  volume_enabled: true
  deletion_mode: filter-and-delete

analytics:
  reporting_enabled: false
";

#[derive(Clone, Copy, Debug)]
struct Divergence {
    case: &'static str,
    reason: &'static str,
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn loki_corpus_matches_krabka() -> TestResult {
    install_log_level_control();
    let client = reqwest::Client::new();
    let loki = start_loki().await?;
    let loki_base = mapped_base_url(&loki, LOKI_PORT).await?;
    wait_for_ready(&client, &loki_base).await?;

    let krabka = start_krabka().await?;
    let timeline = Timeline::new()?;

    let json_body = json_streams(&timeline, JSON_STREAMS);
    let gzip_body = json_streams(&timeline, GZIP_STREAMS);
    let proto_body = proto_push_body(&timeline)?;
    let parser_body = json_streams(&timeline, PARSER_STREAMS);
    for base in [loki_base.as_str(), krabka.push_url.as_str()] {
        push_json(&client, base, TENANT, &json_body).await?;
        push_gzip_json(&client, base, &gzip_body).await?;
        push_proto(&client, base, &proto_body).await?;
        push_json(&client, base, PARSER_TENANT, &parser_body).await?;
    }
    let seeded = JSON_STREAMS.len() + GZIP_STREAMS.len() + PROTO_STREAMS.len();
    for base in [loki_base.as_str(), krabka.query_url.as_str()] {
        wait_for_seeded(&client, base, &timeline, TENANT, "app", seeded).await?;
        wait_for_seeded(
            &client,
            base,
            &timeline,
            PARSER_TENANT,
            "format",
            PARSER_STREAMS.len(),
        )
        .await?;
    }

    let mut differences = Vec::new();
    let mut healed = Vec::new();
    let mut loki_side = Side::new(&loki_base, &loki_base, &loki_base);
    let mut krabka_side = Side::new(
        &krabka.query_url,
        &krabka.push_url,
        &krabka.block_builder_url,
    );
    let cases = corpus(&timeline)?;
    let total = cases.len();
    let mut template_cases = Vec::new();
    for case in cases {
        let krabka_response = krabka_side.probe(&client, &case).await?;
        let loki_response = loki_side.probe(&client, &case).await?;
        let mut agrees = krabka_response == loki_response;
        if let Some(template_case) = template_functions::CASES
            .iter()
            .find(|template_case| template_case.name == case.name)
        {
            let expected = template_expected_response(&timeline, case.name, template_case.expected);
            agrees &= krabka_response == expected && loki_response == expected;
            template_cases.push(json!({
                "id": case.name, "name": case.name, "expression": template_case.expression,
                "request": {"method": "GET", "path": case.path, "tenant": case.tenant, "query": supported_template_query(template_case.expression)},
                "raw_oracle": loki_side.last_response, "raw_candidate": krabka_side.last_response,
                "oracle": loki_response, "candidate": krabka_response,
                "independent_expected": expected,
                "status": if agrees { "matched" } else { "mismatch" },
            }));
        }
        match known_divergence(case.name) {
            Some(divergence) if agrees => healed.push(divergence),
            Some(_) => {}
            None if agrees => {}
            None => differences.push(report(&case, &krabka_response, &loki_response)),
        }
    }

    write_test_output(
        "loki-supported-template-functions.json",
        &json!({
            "schema_version": 1, "upstream_source": "7a40404f32b3e6464c9cfc6cc7dd75a40f3931da",
            "timeline_base_ns": timeline.base_ns,
            "planned": template_functions::CASES.len(), "cases": template_cases,
        }),
    )?;
    krabka.shutdown();
    // How much was actually asked. An empty divergence list over four queries
    // and an empty one over sixty read the same in a passing log otherwise.
    println!(
        "loki_differential: {total} case(s); {} agreed with Loki, {} are recorded divergences",
        total - LOKI_KNOWN_DIVERGENCE.len(),
        LOKI_KNOWN_DIVERGENCE.len(),
    );
    assert!(
        differences.is_empty() && healed.is_empty(),
        "{}",
        summary(&differences, &healed)
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The seeded data.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker and the pinned Loki remote correctness executable"]
async fn upstream_loki_remote_correctness_matches_krabka() -> TestResult {
    let binary = std::fs::canonicalize(
        std::env::var_os("KRABKA_LOKI_CORRECTNESS_BIN")
            .ok_or("KRABKA_LOKI_CORRECTNESS_BIN is required")?,
    )?;
    let source = std::fs::canonicalize(
        std::env::var_os("KRABKA_LOKI_CORRECTNESS_SOURCE")
            .ok_or("KRABKA_LOKI_CORRECTNESS_SOURCE is required")?,
    )?;
    let working_dir = source.join("pkg/logql/bench");
    std::fs::metadata(working_dir.join("queries"))?;
    std::fs::metadata(working_dir.join("remote_test.go"))?;
    let seconds = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?;
    let end_seconds = (seconds - 60) / 10 * 10;
    let fixture = loki_remote_fixture::fixture(end_seconds)?;
    let metadata_dir = tempfile::tempdir()?;
    std::fs::write(
        metadata_dir.path().join("dataset_metadata.json"),
        serde_json::to_vec_pretty(&fixture.metadata)?,
    )?;
    let client = reqwest::Client::new();
    let loki = start_loki().await?;
    let loki_base = mapped_base_url(&loki, LOKI_PORT).await?;
    wait_for_ready(&client, &loki_base).await?;
    let krabka = start_krabka().await?;
    for batch in &fixture.batches {
        for base in [&loki_base, &krabka.push_url] {
            push_json(&client, base, TENANT, batch).await?;
        }
    }
    wait_for_remote_fixture(&client, &[&loki_base, &krabka.query_url], &fixture).await?;
    let report_dir = test_output_dir()?;
    std::fs::write(
        report_dir.join("loki-remote-dataset-metadata.json"),
        serde_json::to_vec_pretty(&fixture.metadata)?,
    )?;
    let mut failures = Vec::new();
    for mode in ["range", "instant"] {
        if let Some(failure) = collect_loki_remote_report(
            &binary,
            &working_dir,
            metadata_dir.path(),
            &loki_base,
            &krabka.query_url,
            mode,
            &report_dir,
        )
        .await?
        {
            failures.push(failure);
        }
    }
    if let Err(error) = run_generated_logql(
        &client,
        &loki_base,
        &krabka.query_url,
        fixture.end_ns,
        &report_dir,
    )
    .await
    {
        failures.push(format!("generated LogQL: {error}"));
    }
    krabka.shutdown();
    if !failures.is_empty() {
        return Err(format!("Loki remote correctness failed: {}", failures.join("; ")).into());
    }
    Ok(())
}

async fn run_generated_logql(
    client: &reqwest::Client,
    reference: &str,
    candidate: &str,
    end_ns: i64,
    report_dir: &std::path::Path,
) -> TestResult {
    let end = (end_ns / 1_000_000_000).to_string();
    generated_differential::run_typed(
        "logql",
        &[
            generated_differential::TypedExpr::log_count_over_time(
                &[generated_differential::LabelMatcher::new(
                    "service_name",
                    generated_differential::MatchOp::Eq,
                    "fixture-json",
                )],
                60,
            ),
            generated_differential::TypedExpr::log_count_over_time(
                &[generated_differential::LabelMatcher::new(
                    "service_name",
                    generated_differential::MatchOp::Eq,
                    "fixture-logfmt",
                )],
                60,
            ),
        ],
        &[
            generated_differential::TypedConstructor::LogSum,
            generated_differential::TypedConstructor::LogMax,
            generated_differential::TypedConstructor::LogAvg,
            generated_differential::TypedConstructor::LogAdd(1),
        ],
        report_dir,
        |expression| {
            let end = &end;
            async move {
                let mut responses = Vec::with_capacity(2);
                let mut valid = true;
                for base in [candidate, reference] {
                    let response = client
                        .get(format!(
                            "{base}/loki/api/v1/query?{}",
                            url::form_urlencoded::Serializer::new(String::new())
                                .append_pair("query", &expression)
                                .append_pair("time", end)
                                .finish()
                        ))
                        .header("X-Scope-OrgID", TENANT)
                        .send()
                        .await?;
                    let status = response.status().as_u16();
                    let text = response.text().await?;
                    let body = json_or_text(&text);
                    valid &= status == 200
                        && body["status"] == "success"
                        && body["data"]["resultType"] == "vector"
                        && body["data"]["result"].as_array().is_some_and(|result| {
                            !result.is_empty()
                                && result.iter().all(|series| {
                                    series["metric"].is_object()
                                        && series["value"].as_array().is_some_and(|sample| {
                                            sample.len() == 2
                                                && sample[0].as_f64().is_some()
                                                && sample[1].as_str().is_some_and(|value| {
                                                    value.parse::<f64>().is_ok_and(f64::is_finite)
                                                })
                                        })
                                })
                        });
                    responses.push(reduce(Shape::Api, status, "application/json", &text));
                }
                Ok((!valid || responses[0] != responses[1]).then(|| format!(
                    "composition `{expression}` disagreed or was rejected: krabka={}; upstream={}",
                    responses[0], responses[1]
                )))
            }
        },
    )
    .await?;

    let observations = Mutex::new(Vec::new());
    generated_differential::run(
        "logql-rejections",
        &[
            "sum()",
            r#"count_over_time({service_name="fixture-json"}[1m], 1)"#,
            r#"rate({service_name="fixture-json"}[)"#,
        ],
        &["sum({expr})", "max({expr})", "({expr})+1"],
        report_dir,
        |expression| {
            let end = &end;
            let observations = &observations;
            async move {
                let mut responses = Vec::with_capacity(2);
                let mut rejected = true;
                for side in [
                    DifferentialImplementation {
                        name: "upstream",
                        base: reference,
                    },
                    DifferentialImplementation {
                        name: "krabka",
                        base: candidate,
                    },
                ] {
                    let encoded = url::form_urlencoded::Serializer::new(String::new())
                        .append_pair("query", &expression)
                        .append_pair("time", end)
                        .finish();
                    let response = client
                        .get(format!("{}/loki/api/v1/query?{encoded}", side.base))
                        .header("X-Scope-OrgID", TENANT)
                        .send()
                        .await?;
                    rejected &= record_rejection_response(
                        &mut responses,
                        ImplementationAnswer {
                            implementation: side.name,
                            response,
                        },
                        logql_query_rejection_kind,
                    )
                    .await?;
                }
                let mut observations = observations
                    .lock()
                    .map_err(|_| "rejection ledger poisoned")?;
                observations.push(
                    json!({"expression": expression, "expected_outcome": "query-rejection",
                    "matched": rejected, "responses": responses}),
                );
                std::fs::write(
                    report_dir.join("logql-rejection-responses.json"),
                    serde_json::to_vec_pretty(&*observations)?,
                )?;
                Ok((!rejected).then(|| {
                    format!("expected parser/type rejection for {expression}: {responses:?}")
                }))
            }
        },
    )
    .await
}

fn logql_query_rejection_kind(status: u16, text: &str) -> Option<&'static str> {
    if status != 400 {
        return None;
    }
    let body = json_or_text(text);
    if body.get("data").is_some() || body["status"] == "success" {
        return None;
    }
    let message = body["error"].as_str().unwrap_or(text).to_ascii_lowercase();
    if message.contains("parse error") || message.contains("syntax error") {
        Some("parser-error")
    } else if message.contains("type mismatch") || message.contains("invalid type") {
        Some("type-error")
    } else {
        None
    }
}

#[test]
fn logql_rejection_comparator_rejects_success_and_unrelated_failures() {
    assert!(logql_query_rejection_kind(400, "parse error: unexpected )") == Some("parser-error"));
    assert!(
        logql_query_rejection_kind(
            400,
            r#"{"status":"error","error":"parse error: unexpected )"}"#
        ) == Some("parser-error")
    );
    assert!(logql_query_rejection_kind(200, "parse error: unexpected )").is_none());
    assert!(logql_query_rejection_kind(500, "parse error: unexpected )").is_none());
    assert!(logql_query_rejection_kind(400, "missing tenant").is_none());
    assert!(
        logql_query_rejection_kind(
            400,
            r#"{"status":"success","data":{},"error":"parse error"}"#
        )
        .is_none()
    );
}

async fn wait_for_remote_fixture(
    client: &reqwest::Client,
    bases: &[&str],
    fixture: &loki_remote_fixture::Fixture,
) -> TestResult {
    let selectors = fixture.metadata["all_selectors"]
        .as_array()
        .ok_or("missing fixture selectors")?
        .iter()
        .map(|selector| selector.as_str().ok_or("invalid fixture selector"))
        .collect::<Result<Vec<_>, _>>()?;
    if bases.is_empty() || selectors.is_empty() {
        return Err("remote fixture readiness requires endpoints and selectors".into());
    }
    let end = (fixture.end_ns / 1_000_000_000).to_string();
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut missing = Vec::new();
    while Instant::now() < deadline {
        missing.clear();
        for base in bases {
            for selector in &selectors {
                let query = format!("sum(count_over_time({selector}[24h1s]))");
                let body = client
                    .get(format!(
                        "{base}/loki/api/v1/query?{}",
                        url::form_urlencoded::Serializer::new(String::new())
                            .append_pair("query", &query)
                            .append_pair("time", &end)
                            .finish()
                    ))
                    .header("X-Scope-OrgID", TENANT)
                    .timeout(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_secs(10)),
                    )
                    .send()
                    .await?
                    .json::<Value>()
                    .await;
                let complete = body.as_ref().is_ok_and(|body| {
                    body["status"] == "success"
                        && body["data"]["resultType"] == "vector"
                        && body["data"]["result"].as_array().is_some_and(|result| {
                            result.len() == 1
                                && result[0]["value"][1]
                                    .as_str()
                                    .and_then(|value| value.parse::<u64>().ok())
                                    == Some(8_641)
                        })
                });
                if !complete {
                    missing.push(format!("{base}: {query}: {body:?}"));
                }
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!(
        "remote fixture never returned exactly 8641 rows per selector: {}",
        missing.join("; ")
    )
    .into())
}

async fn collect_loki_remote_report(
    binary: &std::path::Path,
    source: &std::path::Path,
    metadata: &std::path::Path,
    reference: &str,
    candidate: &str,
    mode: &str,
    report_dir: &std::path::Path,
) -> TestResult<Option<String>> {
    let output = run_loki_remote_binary(binary, source, metadata, reference, candidate, mode).await;
    let output = match output {
        Ok(output) => RemoteCommandOutput {
            stdout: output.stdout,
            stderr: output.stderr,
            exited_successfully: output.status.success(),
        },
        Err(error) => RemoteCommandOutput {
            stdout: Vec::new(),
            stderr: error.to_string().into_bytes(),
            exited_successfully: false,
        },
    };
    let RemoteCommandOutput {
        stdout,
        stderr,
        exited_successfully,
    } = output;
    std::fs::write(
        report_dir.join(format!("loki-remote-{mode}.stdout.txt")),
        &stdout,
    )?;
    std::fs::write(
        report_dir.join(format!("loki-remote-{mode}.stderr.txt")),
        &stderr,
    )?;
    let parsed = std::str::from_utf8(&stdout)
        .map_err(Box::<dyn std::error::Error + Send + Sync>::from)
        .and_then(|output| loki_remote_fixture::execution_report(output, mode));
    let report = match parsed {
        Ok(report) => report,
        Err(error) => json!({"mode": mode, "success": false, "harness_error": error.to_string()}),
    };
    std::fs::write(
        report_dir.join(format!("loki-remote-{mode}.json")),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("Loki upstream remote correctness: {report}");
    Ok(
        (!exited_successfully || report["success"] != true).then(|| {
            format!(
                "{mode}: report={report}; stderr={}",
                String::from_utf8_lossy(&stderr)
            )
        }),
    )
}

async fn run_loki_remote_binary(
    binary: &std::path::Path,
    source: &std::path::Path,
    metadata: &std::path::Path,
    reference: &str,
    candidate: &str,
    mode: &str,
) -> TestResult<std::process::Output> {
    let mut command = std::process::Command::new(binary);
    command
        .current_dir(source)
        .arg("-test.run=^TestRemoteStorageEquality$")
        .arg("-test.v=true")
        .arg("-test.timeout=20m")
        .arg(format!("-addr-1={reference}"))
        .arg(format!("-addr-2={candidate}"))
        .arg(format!("-org-id={TENANT}"))
        .arg("-metadata-dir")
        .arg(metadata)
        .arg("-seed=42")
        .arg("-remote-include-skipped=true")
        .arg(format!("-remote-range-type={mode}"));
    Ok(tokio::task::spawn_blocking(move || command.output()).await??)
}

/// A seeded stream: its labels, and the entries pushed into it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StaticLabelPair {
    name: &'static str,
    value: &'static str,
}

struct SeedStream {
    labels: &'static [StaticLabelPair],
    entries: &'static [SeedEntry],
}

/// One seeded entry, at `offset_secs` after the corpus base time.
struct SeedEntry {
    offset_secs: i64,
    line: &'static str,
    metadata: &'static [StaticLabelPair],
}

/// Streams pushed as `application/json`, the encoding Promtail and Alloy use.
///
/// `api` carries structured metadata on every entry and JSON lines, so it feeds
/// both the `| json` cases and the metadata cases. `worker` carries neither, so
/// a case that fails on it is not failing on metadata.
const JSON_STREAMS: &[SeedStream] = &[
    SeedStream {
        labels: &[
            StaticLabelPair {
                name: "app",
                value: "api",
            },
            StaticLabelPair {
                name: "env",
                value: "prod",
            },
        ],
        entries: &[
            SeedEntry {
                offset_secs: 0,
                line: r#"{"level":"info","status":200,"msg":"api ok","duration":0.25}"#,
                metadata: &[
                    StaticLabelPair {
                        name: "pod",
                        value: "api-1",
                    },
                    StaticLabelPair {
                        name: "trace_id",
                        value: "abc",
                    },
                ],
            },
            SeedEntry {
                offset_secs: 10,
                line: r#"{"level":"error","status":500,"msg":"api error","duration":1.5}"#,
                metadata: &[
                    StaticLabelPair {
                        name: "pod",
                        value: "api-2",
                    },
                    StaticLabelPair {
                        name: "trace_id",
                        value: "def",
                    },
                ],
            },
            SeedEntry {
                offset_secs: 20,
                line: r#"{"level":"warn","status":404,"msg":"api missing","duration":0.5}"#,
                metadata: &[
                    StaticLabelPair {
                        name: "pod",
                        value: "api-1",
                    },
                    StaticLabelPair {
                        name: "trace_id",
                        value: "ghi",
                    },
                ],
            },
        ],
    },
    SeedStream {
        labels: &[
            StaticLabelPair {
                name: "app",
                value: "worker",
            },
            StaticLabelPair {
                name: "env",
                value: "prod",
            },
        ],
        entries: &[
            SeedEntry {
                offset_secs: 5,
                line: "worker started job=1",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 15,
                line: "worker error job=2 failed",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 25,
                line: "request path=/users/1 status=200",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 26,
                line: "request path=/users/2 status=200",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 27,
                line: "request path=/users/3 status=500",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 28,
                line: "request path=/users/4 status=200",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 29,
                line: "request path=/users/5 status=500",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 30,
                line: "request path=/users/6 status=200",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 31,
                line: "request path=/users/7 status=200",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 32,
                line: "request path=/users/8 status=500",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 33,
                line: "request path=/users/9 status=200",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 34,
                line: "request path=/users/10 status=500",
                metadata: &[],
            },
        ],
    },
];

/// The stream pushed as gzipped JSON, covering the `Content-Encoding` path.
const GZIP_STREAMS: &[SeedStream] = &[SeedStream {
    labels: &[
        StaticLabelPair {
            name: "app",
            value: "gz",
        },
        StaticLabelPair {
            name: "env",
            value: "prod",
        },
    ],
    entries: &[
        SeedEntry {
            offset_secs: 3,
            line: "gz first line",
            metadata: &[],
        },
        SeedEntry {
            offset_secs: 13,
            line: "gz second line",
            metadata: &[],
        },
    ],
}];

/// The streams pushed as snappy-compressed protobuf, the encoding Loki's own
/// clients default to.
///
/// `fmt` is logfmt and carries no structured metadata, so the parser,
/// `label_format` and `unwrap` cases below compare a pipeline rather than a
/// wire shape. `meta` is the protobuf path's metadata carrier, kept in a stream
/// of its own for the same reason: one divergence should show up in the cases
/// that ask about it and nowhere else.
const PROTO_STREAMS: &[SeedStream] = &[
    SeedStream {
        labels: &[
            StaticLabelPair {
                name: "app",
                value: "fmt",
            },
            StaticLabelPair {
                name: "env",
                value: "stage",
            },
        ],
        entries: &[
            SeedEntry {
                offset_secs: 2,
                line: "service=checkout latency=12.5 duration=1.25 took=250ms status=ok",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 12,
                line: "service=cart latency=30 duration=2.5 took=1s status=fail",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 22,
                line: "service=checkout latency=7.5 duration=0.5 took=500ms status=ok",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 32,
                line: "service=search latency=18 duration=1 took=750ms status=ok",
                metadata: &[],
            },
        ],
    },
    SeedStream {
        labels: &[
            StaticLabelPair {
                name: "app",
                value: "meta",
            },
            StaticLabelPair {
                name: "env",
                value: "stage",
            },
        ],
        entries: &[
            SeedEntry {
                offset_secs: 4,
                line: "meta first",
                metadata: &[StaticLabelPair {
                    name: "shard",
                    value: "a",
                }],
            },
            SeedEntry {
                offset_secs: 14,
                line: "meta second",
                metadata: &[StaticLabelPair {
                    name: "shard",
                    value: "b",
                }],
            },
        ],
    },
];

/// Lines built to fail a parser, pushed to [`PARSER_TENANT`].
///
/// `json` holds a line that is not JSON beside one that is. `logfmt` holds an
/// unterminated quote, a clean line, and a standalone key with no `=`.
const PARSER_STREAMS: &[SeedStream] = &[
    SeedStream {
        labels: &[
            StaticLabelPair {
                name: "app",
                value: "api",
            },
            StaticLabelPair {
                name: "env",
                value: "prod",
            },
            StaticLabelPair {
                name: "format",
                value: "json",
            },
        ],
        entries: &[
            SeedEntry {
                offset_secs: 0,
                line: "not json",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 1,
                line: r#"{"status":500}"#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 5,
                line: r#"{"request":{"method":"GET"},"response":{"status":200}}"#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 6,
                line: r#"{"request":{"method":"GET"},"response":{"status":500}}"#,
                metadata: &[],
            },
        ],
    },
    SeedStream {
        labels: &[
            StaticLabelPair {
                name: "app",
                value: "api",
            },
            StaticLabelPair {
                name: "env",
                value: "prod",
            },
            StaticLabelPair {
                name: "format",
                value: "logfmt",
            },
        ],
        entries: &[
            SeedEntry {
                offset_secs: 2,
                line: r#"status=500 msg="unterminated"#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 3,
                line: r#"status=200 msg="ok""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 4,
                line: r#"status=204 empty msg="keep empty""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 7,
                line: r#"status=200 msg="api parser ok""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 8,
                line: r#"status=500 msg="api parser error""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 9,
                line: r#"duration=10ms bytes_consumed=21MB msg="api typed parser too fast""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 10,
                line: r#"duration=25ms bytes_consumed=19MB msg="api typed parser too small""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 11,
                line: r#"duration=25ms bytes_consumed=21MB msg="api typed parser ok""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 12,
                line: "\u{1b}[31mstatus=503 msg=\"colored parser error\"\u{1b}[0m",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 13,
                line: r#"raw=" /checkout/ " path=/api/items msg="template helper""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 14,
                line: r#"short=hi long=hello-world mark=x msg="spacing helper""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 15,
                line: r#"client=10.2.3.4 msg="api ip filter ok""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 16,
                line: r#"client=192.168.2.3 msg="api ip filter miss""#,
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 20,
                line: "cost=1.5 requests=1 size=1KiB latency=100ms api unwrap metric one",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 21,
                line: "cost=2.5 requests=3 size=2KiB latency=200ms api unwrap metric two",
                metadata: &[],
            },
            SeedEntry {
                offset_secs: 22,
                line: "cost=0.5 requests=2 size=512B latency=1s api unwrap metric reset",
                metadata: &[],
            },
        ],
    },
    SeedStream {
        labels: &[
            StaticLabelPair {
                name: "app",
                value: "api",
            },
            StaticLabelPair {
                name: "env",
                value: "prod",
            },
            StaticLabelPair {
                name: "format",
                value: "metadata",
            },
        ],
        entries: &[
            SeedEntry {
                offset_secs: 17,
                line: "api metadata ok",
                metadata: &[
                    StaticLabelPair {
                        name: "trace_id",
                        value: "abc",
                    },
                    StaticLabelPair {
                        name: "status",
                        value: "200",
                    },
                ],
            },
            SeedEntry {
                offset_secs: 18,
                line: "api metadata miss",
                metadata: &[
                    StaticLabelPair {
                        name: "trace_id",
                        value: "def",
                    },
                    StaticLabelPair {
                        name: "status",
                        value: "500",
                    },
                ],
            },
        ],
    },
    SeedStream {
        labels: &[
            StaticLabelPair {
                name: "app",
                value: "api",
            },
            StaticLabelPair {
                name: "env",
                value: "prod",
            },
            StaticLabelPair {
                name: "format",
                value: "packed",
            },
        ],
        entries: &[SeedEntry {
            offset_secs: 19,
            line: r#"{"container":"myapp","pod":"pod-3223f","_entry":"original log message"}"#,
            metadata: &[],
        }],
    },
];

// ---------------------------------------------------------------------------
// Cases.
// ---------------------------------------------------------------------------

/// Which Krabka role answers a case. Loki answers every case on one port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Querier,
    Distributor,
    BlockBuilder,
}

/// How a response is reduced before the two sides are compared.
///
/// Each shape keeps the status code and the body, and removes only what one
/// side measures on its own storage, clock or build.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// A `LogQL` API answer: a JSON body put through [`normalize`], or the
    /// error text of a refusal.
    Api,
    /// An instant answer to a request with no `time`, which each side
    /// evaluates at its own receive time. A sample timestamp within a
    /// minute of the wall clock reads as `<now>`.
    ApiAtNow,
    /// `/loki/api/v1/status/buildinfo`: every string value is a build fact.
    BuildInfo,
    /// A plain-text ops page: `/ready`, `/log_level`, `/services`.
    StatusText,
    /// `/config`: only the `auth_enabled` line and the presence of `target`.
    Config,
    /// `/metrics`: the presence of the metric families a Loki dashboard reads.
    MetricFamilies,
    /// Status, content type and body, with nothing removed.
    Typed,
    /// The delete API: request ids and creation times are minted per side.
    Delete,
    /// A ring status page: its headings and the state words it shows.
    RingPage,
    /// `/index/stats`: the counters are measured on each side's storage.
    IndexStats,
    /// `/index/volume` and `/index/volume_range`: byte counts are measured.
    IndexVolume,
    /// `/detected_fields` and `/detected_field/{name}/values`.
    DetectedFields,
    /// `/detected_labels`.
    DetectedLabels,
    /// `/patterns`: sample timestamps are bucketed on each side's clock.
    Patterns,
    /// A websocket upgrade, compared by the handshake answer alone.
    Handshake,
    /// A websocket tail, compared by the first frame it sends.
    TailFrame,
    /// A websocket tail on a tenant with no history, compared by the first
    /// frame it sends after the case's body is pushed to the distributor.
    LiveTailFrame,
}

/// One comparison: a request made identically to both sides.
struct LogQueryCase<Query> {
    name: &'static str,
    query: Query,
}

struct NamedQueryExpression {
    name: &'static str,
    query: &'static str,
}

struct DifferentialImplementation<'a> {
    name: &'static str,
    base: &'a str,
}

struct RemoteCommandOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    exited_successfully: bool,
}

struct NamedEndpointCase {
    name: &'static str,
    path: &'static str,
}

struct EmptyTenantCase {
    name: &'static str,
    path: &'static str,
    query: &'static str,
}

struct NamedRawQueryCase {
    name: &'static str,
    path: &'static str,
    query: &'static str,
}

struct NamedStepCase {
    name: &'static str,
    endpoint: &'static str,
    step: &'static str,
}

struct DetectedEndpointCase {
    suffix: &'static str,
    endpoint: &'static str,
    query: Option<&'static str>,
}

struct NamedTargetCase {
    name: &'static str,
    path: &'static str,
    target: Target,
}

struct LogLevelCase {
    name: &'static str,
    query: Option<&'static str>,
    form: Option<&'static str>,
}

struct LifecycleCase {
    name: &'static str,
    method: Method,
    path: &'static str,
    query: &'static str,
}

struct ProtobufPushCase {
    name: &'static str,
    streams: Vec<LokiProtoStream>,
}

struct KeyReplacement {
    key: &'static str,
    placeholder: &'static str,
}

struct ExpectedTemplateLevel {
    level: &'static str,
    offsets: &'static [i64],
}

struct DetectedCaseName {
    kind: &'static str,
    endpoint: &'static str,
    name: &'static str,
}

#[derive(Clone, serde::Serialize)]
struct QueryParameter<'a> {
    name: &'a str,
    value: Cow<'a, str>,
}

impl<'a> QueryParameter<'a> {
    fn new(name: &'a str, value: impl Into<Cow<'a, str>>) -> Self {
        Self {
            name,
            value: value.into(),
        }
    }
}

struct Case {
    name: &'static str,
    method: Method,
    path: String,
    /// The query string, already encoded, without the `?`.
    query: String,
    headers: Vec<RequestHeader>,
    body: Vec<u8>,
    tenant: &'static str,
    target: Target,
    shape: Shape,
}

struct RequestHeader {
    name: &'static str,
    value: String,
}

struct LiveTailPush<'a> {
    client: &'a reqwest::Client,
    url: String,
}

struct ServedRouter {
    url: String,
    shutdown: oneshot::Sender<()>,
}

impl Case {
    fn new(name: &'static str, method: Method, path: impl Into<String>) -> Self {
        Self {
            name,
            method,
            path: path.into(),
            query: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
            tenant: TENANT,
            target: Target::Querier,
            shape: Shape::Api,
        }
    }

    fn get(name: &'static str, path: impl Into<String>) -> Self {
        Self::new(name, Method::GET, path)
    }

    fn post(name: &'static str, path: impl Into<String>) -> Self {
        Self::new(name, Method::POST, path)
    }

    /// Appends form-encoded parameters to the query string.
    fn params<'a>(mut self, params: impl IntoIterator<Item = QueryParameter<'a>>) -> Self {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for parameter in params {
            serializer.append_pair(parameter.name, parameter.value.as_ref());
        }
        let encoded = serializer.finish();
        if !self.query.is_empty() && !encoded.is_empty() {
            self.query.push('&');
        }
        self.query.push_str(&encoded);
        self
    }

    /// Sets the query string exactly as given, for the cases that repeat a
    /// parameter or spell one in a way `params` would re-encode.
    fn raw_query(mut self, query: &str) -> Self {
        self.query = query.to_string();
        self
    }

    fn header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push(RequestHeader {
            name,
            value: value.into(),
        });
        self
    }

    fn body(mut self, content_type: &str, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self.header("Content-Type", content_type)
    }

    fn form(self, body: &str) -> Self {
        self.body("application/x-www-form-urlencoded", body)
    }

    fn categorized(self) -> Self {
        self.header(ENCODING_FLAGS_HEADER, CATEGORIZE_LABELS)
    }

    const fn tenant(mut self, tenant: &'static str) -> Self {
        self.tenant = tenant;
        self
    }

    const fn target(mut self, target: Target) -> Self {
        self.target = target;
        self
    }

    const fn shape(mut self, shape: Shape) -> Self {
        self.shape = shape;
        self
    }
}

/// Every case the suite compares, in the order it runs them.
fn corpus(timeline: &Timeline) -> TestResult<Vec<Case>> {
    let mut cases = Vec::new();
    cases.extend(stream_cases(timeline));
    cases.extend(categorized_stream_cases(timeline));
    cases.extend(metric_cases(timeline));
    cases.extend(instant_cases(timeline));
    cases.extend(vector_function_cases(timeline));
    cases.extend(parser_error_cases(timeline));
    cases.extend(parser_cases(timeline));
    cases.extend(template_functions::CASES.iter().map(|template_case| {
        let query = supported_template_query(template_case.expression);
        Case::get(template_case.name, "/loki/api/v1/query_range")
            .params(range_params(timeline, &query, 1))
            .tenant(PARSER_TENANT)
    }));
    cases.extend(parser_metric_cases(timeline));
    cases.extend(parser_instant_cases(timeline));
    cases.extend(metadata_cases(timeline));
    cases.extend(metadata_alias_cases(timeline));
    cases.extend(empty_tenant_cases());
    cases.extend(analytics_cases(timeline));
    cases.extend(format_query_cases());
    cases.extend(format_query_expression_cases());
    cases.extend(format_query_error_cases());
    cases.extend(query_error_cases());
    cases.extend(query_parameter_cases(timeline));
    cases.extend(metadata_error_cases());
    cases.extend(index_error_cases(timeline));
    cases.extend(detected_error_cases());
    cases.extend(tail_cases(timeline));
    cases.extend(json_push_cases(timeline));
    cases.extend(push_encoding_cases(timeline)?);
    cases.extend(protobuf_push_cases(timeline)?);
    cases.extend(status_cases());
    cases.extend(log_level_cases());
    cases.extend(delete_lifecycle_cases());
    cases.extend(ingester_control_cases());
    Ok(cases)
}

/// `/query_range` over log selectors, line filters, parsers and formatters.
fn supported_template_query(expression: &str) -> String {
    let template = serde_json::to_string(&format!("{{{{ {expression} }}}}"))
        .expect("template string serializes");
    format!("{{app=\"api\",format=\"logfmt\"}} | line_format {template}")
}

fn stream_cases(timeline: &Timeline) -> Vec<Case> {
    [
        LogQueryCase {
            name: "selector_api_stream",
            query: r#"{app="api"}"#,
        },
        LogQueryCase {
            name: "selector_meta_stream",
            query: r#"{app="meta"}"#,
        },
        LogQueryCase {
            name: "selector_every_stream",
            query: r#"{env=~".+"}"#,
        },
        LogQueryCase {
            name: "selector_regex_match",
            query: r#"{app=~"worker|gz"}"#,
        },
        LogQueryCase {
            name: "selector_not_equal",
            query: r#"{env="prod", app!="api"}"#,
        },
        LogQueryCase {
            name: "selector_regex_not_match",
            query: r#"{env="prod", app!~"api|gz"}"#,
        },
        LogQueryCase {
            name: "line_filter_contains",
            query: r#"{app="worker"} |= "error""#,
        },
        LogQueryCase {
            name: "line_filter_not_contains",
            query: r#"{app="worker"} != "error""#,
        },
        LogQueryCase {
            name: "line_filter_regex",
            query: r#"{app="worker"} |~ "job=[0-9]""#,
        },
        LogQueryCase {
            name: "line_filter_not_regex",
            query: r#"{app="worker"} !~ "^worker started""#,
        },
        LogQueryCase {
            name: "line_filter_chained",
            query: r#"{app="gz"} |= "gz" != "second""#,
        },
        LogQueryCase {
            name: "json_parser",
            query: r#"{app="api"} | json"#,
        },
        LogQueryCase {
            name: "json_parser_label_filter",
            query: r#"{app="api"} | json | status = "500""#,
        },
        LogQueryCase {
            name: "json_parser_named_expression",
            query: r#"{app="api"} | json severity="level""#,
        },
        LogQueryCase {
            name: "logfmt_parser",
            query: r#"{app="fmt"} | logfmt"#,
        },
        LogQueryCase {
            name: "logfmt_label_filter",
            query: r#"{app="fmt"} | logfmt | status = "ok""#,
        },
        LogQueryCase {
            name: "logfmt_numeric_label_filter",
            query: r#"{app="fmt"} | logfmt | latency > 10"#,
        },
        LogQueryCase {
            name: "label_format_rename",
            query: r#"{app="fmt"} | logfmt | label_format svc=service"#,
        },
        LogQueryCase {
            name: "label_format_template",
            query: r#"{app="fmt"} | logfmt | label_format svc=`{{.service}}-{{.status}}`"#,
        },
        LogQueryCase {
            name: "label_format_overwrites_a_stream_label",
            query: r#"{app="fmt"} | logfmt | label_format app="renamed""#,
        },
        LogQueryCase {
            name: "line_format_template",
            query: r#"{app="fmt"} | logfmt | line_format `{{.service}}/{{.status}}`"#,
        },
        LogQueryCase {
            name: "structured_metadata_filter",
            query: r#"{app="meta"} | shard = "b""#,
        },
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, "/loki/api/v1/query_range")
            .params(range_params(timeline, case.query, 15))
    })
    .collect()
}

/// The same reads, asked for under the `categorize-labels` encoding.
///
/// Every query here reaches a stream that carries something to categorise:
/// structured metadata pushed as JSON (`api`) or as protobuf (`meta`), labels a
/// parser stage produced (`| json`, `| logfmt`), a label `label_format` wrote,
/// and, in `worker`, a stream whose only categorised label is the
/// `detected_level` that Loki's own discovery added.
///
/// The last five ask what happens when a pipeline stage writes a name that is
/// already taken, which is where the two encodings could most easily disagree
/// about which stream an entry belongs to. A `label_format` destination is
/// parsed whatever it overwrote (a series label, a series label rewritten to
/// the string it already held, or a piece of structured metadata) and the name
/// leaves the stream. A parser stage instead never overwrites: it writes
/// `<name>_extracted` and the original stays where it was.
fn categorized_stream_cases(timeline: &Timeline) -> Vec<Case> {
    [
        LogQueryCase {
            name: "categorized_selector_api_stream",
            query: r#"{app="api"}"#,
        },
        LogQueryCase {
            name: "categorized_selector_meta_stream",
            query: r#"{app="meta"}"#,
        },
        LogQueryCase {
            name: "categorized_selector_worker_stream",
            query: r#"{app="worker"}"#,
        },
        LogQueryCase {
            name: "categorized_json_parser",
            query: r#"{app="api"} | json"#,
        },
        LogQueryCase {
            name: "categorized_logfmt_parser",
            query: r#"{app="fmt"} | logfmt"#,
        },
        LogQueryCase {
            name: "categorized_label_format_rename",
            query: r#"{app="fmt"} | logfmt | label_format svc=service"#,
        },
        LogQueryCase {
            name: "categorized_label_format_overwrites_a_stream_label",
            query: r#"{app="fmt"} | logfmt | label_format app="renamed""#,
        },
        LogQueryCase {
            name: "categorized_label_format_overwrites_a_stream_label_from_a_parsed_one",
            query: r#"{app="fmt"} | logfmt | label_format app=service"#,
        },
        LogQueryCase {
            name: "categorized_label_format_rewrites_a_stream_label_unchanged",
            query: r#"{app="fmt"} | logfmt | label_format env="stage""#,
        },
        LogQueryCase {
            name: "categorized_label_format_overwrites_structured_metadata",
            query: r#"{app="meta"} | label_format shard="z""#,
        },
        LogQueryCase {
            name: "categorized_parser_field_collides_with_a_stream_label",
            query: r#"{app="fmt"} | logfmt app="service""#,
        },
        LogQueryCase {
            name: "categorized_structured_metadata_filter",
            query: r#"{app="meta"} | shard = "b""#,
        },
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, "/loki/api/v1/query_range")
            .params(range_params(timeline, case.query, 15))
            .categorized()
    })
    .collect()
}

/// `/query_range` over metric queries: range aggregations, vector
/// aggregations, unwrap, and one deliberately step-unaligned window.
fn metric_cases(timeline: &Timeline) -> Vec<Case> {
    let mut cases: Vec<Case> = [
        LogQueryCase { name: "count_over_time", query: r#"count_over_time({app="api"}[1m])"# },
        LogQueryCase { name: "count_over_time_line_filter", query: r#"count_over_time({app="worker"} |= "error" [1m])"# },
        LogQueryCase { name: "rate", query: r#"rate({app=~"api|worker"}[1m])"# },
        LogQueryCase { name: "bytes_over_time", query: r#"bytes_over_time({app="worker"}[1m])"# },
        LogQueryCase { name: "bytes_rate", query: r#"bytes_rate({app="worker"}[1m])"# },
        LogQueryCase { name: "sum_by_count_over_time", query: r#"sum by (app) (count_over_time({app=~".+"}[1m]))"# },
        LogQueryCase { name: "sum_without_count_over_time", query: r#"sum without (env, detected_level, service_name) (count_over_time({app=~"worker|gz"}[1m]))"# },
        LogQueryCase { name: "max_by_rate", query: r#"max by (env) (rate({app=~".+"}[1m]))"# },
        LogQueryCase { name: "avg_by_count_over_time", query: r#"avg by (app) (count_over_time({app=~".+"}[1m]))"# },
        LogQueryCase { name: "topk_over_range_aggregation", query: r#"topk(1, count_over_time({app=~"worker|gz"}[1m]))"# },
        LogQueryCase { name: "topk_over_vector_aggregation", query: r#"topk(1, sum by (app) (bytes_over_time({app=~".+"}[1m])))"# },
        LogQueryCase { name: "scalar_multiply", query: r#"sum(count_over_time({app=~".+"}[1m])) * 2"# },
        LogQueryCase { name: "binary_vector_division", query: r#"sum by (app) (count_over_time({app=~".+"}[1m])) / sum by (app) (count_over_time({app=~".+"}[2m]))"# },
        LogQueryCase { name: "unwrap_sum_over_time", query: r#"sum_over_time({app="fmt"} | logfmt | unwrap latency [1m])"# },
        LogQueryCase { name: "unwrap_avg_over_time_by_service", query: r#"avg_over_time({app="fmt"} | logfmt | unwrap latency [1m]) by (service)"# },
        LogQueryCase { name: "unwrap_max_over_time", query: r#"max_over_time({app="fmt"} | logfmt | unwrap latency [1m])"# },
        LogQueryCase { name: "unwrap_quantile_over_time", query: r#"quantile_over_time(0.9, {app="fmt"} | logfmt | unwrap latency [1m]) by (service)"# },
        LogQueryCase { name: "unwrap_rate", query: r#"rate({app="fmt"} | logfmt | unwrap latency [1m])"# },
        LogQueryCase { name: "unwrap_duration_conversion", query: r#"sum_over_time({app="fmt"} | logfmt | unwrap duration(took) [1m])"# },
        LogQueryCase { name: "unwrap_label_named_after_a_conversion", query: r#"sum_over_time({app="fmt"} | logfmt | unwrap duration [1m])"# }
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, "/loki/api/v1/query_range").params(range_params(timeline, case.query, 15))
    })
    .collect();

    // A window whose bounds are not multiples of the step. Loki rounds both
    // bounds up to the next multiple; nothing in this repo said what Krabka
    // does, so the corpus asks.
    cases.push(
        Case::get(
            "count_over_time_unaligned_window",
            "/loki/api/v1/query_range",
        )
        .params([
            QueryParameter::new("query", r#"count_over_time({app="api"}[1m])"#.to_string()),
            QueryParameter::new("start", timeline.at(-7).to_string()),
            QueryParameter::new("end", timeline.at(53).to_string()),
            QueryParameter::new("step", "15".to_string()),
            QueryParameter::new("direction", "forward".to_string()),
        ]),
    );
    cases
}

/// `/query`, which answers at a single instant rather than over a window.
fn instant_cases(timeline: &Timeline) -> Vec<Case> {
    [
        LogQueryCase {
            name: "instant_selector_api_stream",
            query: r#"{app="api"}"#,
        },
        LogQueryCase {
            name: "instant_selector_worker_stream",
            query: r#"{app="worker"}"#,
        },
        LogQueryCase {
            name: "instant_sum_count_over_time",
            query: r#"sum(count_over_time({app=~".+"}[5m]))"#,
        },
        LogQueryCase {
            name: "instant_sum_by_count_over_time",
            query: r#"sum by (app) (count_over_time({app=~".+"}[5m]))"#,
        },
        LogQueryCase {
            name: "instant_topk_count_over_time",
            query: r#"topk(2, sum by (app) (count_over_time({app=~".+"}[5m])))"#,
        },
        LogQueryCase {
            name: "instant_unwrap_sum_over_time",
            query: r#"sum_over_time({app="fmt"} | logfmt | unwrap latency [5m])"#,
        },
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, "/loki/api/v1/query").params([
            QueryParameter::new("query", case.query),
            QueryParameter::new("time", timeline.at(60).to_string()),
            QueryParameter::new("direction", "forward".to_string()),
        ])
    })
    .collect()
}

/// `vector()`, `label_replace` and plain arithmetic, which need no stored data.
///
/// The range cases sit near the epoch on purpose: nothing is stored there, so
/// the answer can only come from the expression itself. The instant cases ask
/// at a corpus time in nanoseconds.
fn vector_function_cases(timeline: &Timeline) -> Vec<Case> {
    let instant = timeline.at(60).to_string();
    let label_replace = r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)")"#;
    let mut cases: Vec<Case> = [
        LogQueryCase {
            name: "scalar_arithmetic_range",
            query: "1+2".to_string(),
        },
        LogQueryCase {
            name: "label_replace_vector_range",
            query: label_replace.to_string(),
        },
        LogQueryCase {
            name: "label_replace_vector_or_range",
            query: format!("{label_replace} or vector(2)"),
        },
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, "/loki/api/v1/query_range").params([
            QueryParameter::new("query", case.query),
            QueryParameter::new("start", "0.000000000"),
            QueryParameter::new("end", "20000000000"),
            QueryParameter::new("step", "10s"),
        ])
    })
    .collect();
    cases.extend(
        [
            LogQueryCase {
                name: "label_replace_vector_instant",
                query: label_replace.to_string(),
            },
            LogQueryCase {
                name: "label_replace_vector_arithmetic_instant",
                query: format!("{label_replace} + on() vector(2)"),
            },
            LogQueryCase {
                name: "label_replace_vector_or_instant",
                query: format!("{label_replace} or vector(2)"),
            },
            LogQueryCase {
                name: "label_replace_vector_sort_instant",
                query: format!("sort({label_replace})"),
            },
            LogQueryCase {
                name: "label_replace_vector_sort_desc_instant",
                query: format!("sort_desc({label_replace})"),
            },
        ]
        .into_iter()
        .map(|case| {
            Case::get(case.name, "/loki/api/v1/query").params([
                QueryParameter::new("query", case.query),
                QueryParameter::new("time", instant.as_str()),
            ])
        }),
    );
    cases
}

/// The `__error__` label a failing parser writes, and the logfmt parser's
/// `--keep-empty` and `--strict` flags, over [`PARSER_STREAMS`].
fn parser_error_cases(timeline: &Timeline) -> Vec<Case> {
    [
        LogQueryCase { name: "parser_error_json", query: r#"{app="api",format="json"} | json"# },
        LogQueryCase { name: "parser_error_json_filtered_out", query: r#"{app="api",format="json"} | json | __error__ = """# },
        LogQueryCase { name: "parser_error_logfmt_malformed_field", query: r#"{app="api",format="logfmt"} | logfmt"# },
        LogQueryCase { name: "parser_error_logfmt_keep_empty", query: r#"{app="api",format="logfmt"} | logfmt --keep-empty | empty = """# },
        LogQueryCase { name: "parser_error_logfmt_strict", query: r#"{app="api",format="logfmt"} | logfmt --strict | __error__ = "LogfmtParserErr""# },
        LogQueryCase { name: "parser_error_logfmt_strict_clean", query: r#"{app="api",format="logfmt"} | logfmt --strict | __error__ = """# }
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, "/loki/api/v1/query_range")
            .params(range_params(timeline, case.query, 15))
            .tenant(PARSER_TENANT)
    })
    .collect()
}

/// Parser and formatter coverage carried by the original differential suite.
fn parser_cases(timeline: &Timeline) -> Vec<Case> {
    [
        LogQueryCase { name: "parser_nested_json", query: r#"{app="api",format="json"} | json | request_method = "GET" | response_status >= 500"# },
        LogQueryCase { name: "parser_selected_json", query: r#"{app="api",format="json"} | json method="request.method", status_code="response.status" | status_code >= 500"# },
        LogQueryCase { name: "parser_logfmt_numeric", query: r#"{app="api",format="logfmt"} | logfmt | status >= 500"# },
        LogQueryCase { name: "parser_logfmt_selected", query: r#"{app="api",format="logfmt"} | logfmt status, message="msg" | status >= 500"# },
        LogQueryCase { name: "parser_logfmt_or", query: r#"{app="api",format="logfmt"} | logfmt | status >= 500 or msg = "api parser ok""# },
        LogQueryCase { name: "parser_logfmt_comma_and", query: r#"{app="api",format="logfmt"} | logfmt | status >= 500, msg = "api parser error""# },
        LogQueryCase { name: "parser_logfmt_adjacent_and", query: r#"{app="api",format="logfmt"} | logfmt | status >= 500 msg = "api parser error""# },
        LogQueryCase { name: "parser_backtick_filter", query: r#"{app="api",format="logfmt"} | logfmt | msg = `api parser error`"# },
        LogQueryCase { name: "parser_line_format", query: r#"{app="api",format="logfmt"} | logfmt | line_format `{{.msg}} {{.status}}` |= "api parser error 500""# },
        LogQueryCase { name: "parser_line_format_pipeline", query: r#"{app="api",format="logfmt"} | logfmt | line_format `{{ .msg | replace " " "_" | upper }} {{.status}}` |= "API_PARSER_ERROR 500""# },
        LogQueryCase { name: "parser_line_format_with", query: r#"{app="api",format="logfmt"} | logfmt | line_format `{{ with .raw }}raw={{ . }}{{ else }}missing{{ end }}` |= "raw= /checkout/ ""# },
        LogQueryCase { name: "parser_line_format_trim", query: r#"{app="api",format="logfmt"} | logfmt | line_format `left {{- .msg -}} right` |= "leftapi parser errorright""# },
        LogQueryCase { name: "parser_line_format_comment", query: r#"{app="api",format="logfmt"} | logfmt | line_format `before{{/* hidden */}}after {{ .msg }}` |= "beforeafter api parser error""# },
        LogQueryCase { name: "parser_label_format", query: r#"{app="api",format="logfmt"} | logfmt | label_format namespace=env, summary="{{.msg}} {{.status}}" | namespace = "prod" | summary = "api parser error 500""# },
        LogQueryCase { name: "parser_label_format_pipeline", query: r#"{app="api",format="logfmt"} | logfmt | label_format summary=`{{ .msg | replace " " "_" | upper }}` | summary = "API_PARSER_ERROR""# },
        LogQueryCase { name: "parser_line_format_strings", query: r#"{app="api",format="logfmt"} | logfmt | line_format `{{ .raw | trim | trimPrefix "/" | trimSuffix "/" | title }} {{ .raw | trimAll " /" }} {{ .path | substr 1 10 }} {{ .path | substr 5 -1 }} {{ .path | substr -1 4 }}` |= "Checkout checkout api/items items /api""# },
        LogQueryCase { name: "parser_line_format_logic", query: r#"{app="api",format="logfmt"} | logfmt | line_format `{{ contains "helper" .msg }} {{ .path | hasPrefix "/api" }} {{ .path | hasSuffix "items" }} {{ .msg | eq "template helper" }}` |= "true true true true""# },
        LogQueryCase { name: "parser_template_string_truthiness", query: r#"{app="api",format="logfmt"} | line_format `{{ if "false" }}yes{{ else }}bad{{ end }}|{{ if "0" }}yes{{ else }}bad{{ end }}|{{ if false }}bad{{ else }}no{{ end }}`"# },
        LogQueryCase { name: "parser_template_operands", query: r#"{app="api",format="logfmt"} | line_format `{{ and "left" "right" }}|{{ or "" "fallback" }}|{{ "last" | and "first" }}|{{ "last" | or "first" }}`"# },
        LogQueryCase { name: "parser_template_typed_values", query: r#"{app="api",format="logfmt"} | line_format `{{ eq "b" "a" "b" }}|{{ lt "3" "10" }}|{{ lt 3 10 }}|{{ default "fallback" false }}|{{ default "fallback" 0 }}|{{ default "fallback" "0" }}`"# },
        LogQueryCase { name: "parser_template_collection_values", query: r#"{app="api",format="logfmt"} | line_format `{{ len (fromJson "[1,2]") }}|{{ or (fromJson "[]") (fromJson "[1,2]") }}|{{ with or "" "chosen" }}{{ . | upper }}{{ else }}bad{{ end }}`"# },
        LogQueryCase { name: "parser_template_numeric_conditions", query: r#"{app="api",format="logfmt"} | line_format `{{ if sub 2 2 }}bad{{ else }}zero{{ end }}|{{ if addf 1 -1 }}bad{{ else }}zero{{ end }}`"# },
        LogQueryCase { name: "parser_template_short_circuit", query: r#"{app="api",format="logfmt"} | line_format `{{ or "chosen" (div 1 0) }}|{{ and "" (div 1 0) }}` | __error__ = """# },
        LogQueryCase { name: "parser_template_error_filter", query: r#"{app="api",format="logfmt"} | line_format `prefix{{ count "[" __line__ }}` | __error__ = """# },
        LogQueryCase { name: "parser_template_definitions", query: r#"{app="api",format="logfmt"} | line_format `{{ template "suffix" "chosen" }}{{ define "suffix" }}{{ . | upper }}={{ $ }}{{ end }}`"# },
        LogQueryCase { name: "parser_template_block", query: r#"{app="api",format="logfmt"} | line_format `{{ block "body" "fallback" }}{{ . }}{{ end }}`"# },
        LogQueryCase { name: "parser_template_outer_assignment", query: r#"{app="api",format="logfmt"} | line_format `{{ $sum := 0 }}{{ range $n := fromJson "[1,2,3]" }}{{ $sum = add $sum $n }}{{ . }}{{ end }}|{{ $sum }}`"# },
        LogQueryCase { name: "parser_template_range_flow", query: r#"{app="api",format="logfmt"} | line_format `{{ range 4 }}{{ if eq . 1 }}{{ continue }}{{ end }}{{ if eq . 3 }}{{ break }}{{ end }}{{ . }}{{ end }}done`"# },
        LogQueryCase { name: "parser_template_range_assignment", query: r#"{app="api",format="logfmt"} | line_format `{{$i := 9}}{{$v := "before"}}{{range $i, $v = fromJson "[10,20]"}}{{$i}}={{$v}};{{end}}|{{$i}}:{{$v}}`"# },
        LogQueryCase { name: "parser_template_empty_range_binding", query: r#"{app="api",format="logfmt"} | line_format `{{range $v := fromJson "[]"}}bad{{else}}{{len $v}}{{end}}|{{range (fromJson "null")}}bad{{else}}nil{{end}}`"# },
        LogQueryCase { name: "parser_template_invalid_range_filter", query: r#"{app="api",format="logfmt"} | line_format `{{range "abc"}}bad{{else}}bad{{end}}` | __error__="""# },
        LogQueryCase { name: "parser_template_recursive_execution", query: r#"{app="api",format="logfmt"} | line_format `{{define "down"}}{{if gt . 0}}{{range 1}}{{template "down" (sub $ 1)}}{{end}}{{else}}done{{end}}{{end}}{{template "down" 2000}}`"# },
        LogQueryCase { name: "parser_template_label_composition", query: r#"{app="api",format="logfmt"} | label_format decision=`{{ or "" "chosen" }}` | decision="chosen" | line_format `{{ .decision | upper }}`"# },
        LogQueryCase { name: "parser_line_format_ne", query: r#"{app="api",format="logfmt"} | logfmt | line_format `{{ ne .msg "api parser error" }} {{ .path | ne "/health" }}` |= "true true""# },
        LogQueryCase { name: "parser_line_format_len", query: r#"{app="api",format="logfmt"} | logfmt | line_format `len={{ len .msg }}` |= "len=15""# },
        LogQueryCase { name: "parser_line_format_spacing", query: r#"{app="api",format="logfmt"} | logfmt | line_format `{{ alignLeft 5 .short }}|{{ alignLeft 5 .long }}|{{ alignRight 5 .short }}|{{ alignRight 5 .long }}|{{ repeat 3 .mark }}` |= "hi   |hello|   hi|world|xxx""# },
        LogQueryCase { name: "parser_line_format_regex", query: r#"{app="api",format="logfmt"} | logfmt | line_format `{{ count "e" .msg }}|{{ regexReplaceAll "(template) (helper)" .msg "${2}-${1}" }}|{{ .msg | regexReplaceAllLiteral "(template) (helper)" "${2}-${1}" }}` |= "4|helper-template|${2}-${1}""# },
        LogQueryCase { name: "parser_drop_keep", query: r#"{app="api",format="logfmt"} | logfmt | drop env, msg="api parser error" | keep app, format, status="500" | status = "500""# },
        LogQueryCase { name: "parser_decolorize", query: r#"{app="api",format="logfmt"} | decolorize | logfmt | msg = "colored parser error""# },
        LogQueryCase { name: "parser_pattern_filter", query: r#"{app="api",format="logfmt"} |> `status=500 msg="api parser error"`"# },
        LogQueryCase { name: "parser_pattern", query: r#"{app="api",format="logfmt"} | pattern `status=<status> msg="<msg>"` | status >= 500"# },
        LogQueryCase { name: "parser_regexp", query: r#"{app="api",format="logfmt"} | regexp `status=(?P<status>\d+) msg="(?P<msg>.*)"` | status >= 500"# },
        LogQueryCase { name: "parser_unpack", query: r#"{app="api",format="packed"} | unpack != "container" | pod = "pod-3223f""# },
        LogQueryCase { name: "parser_typed_filter", query: r#"{app="api",format="logfmt"} | logfmt | duration >= 20ms | bytes_consumed > 20MB"# },
        LogQueryCase { name: "parser_ip_cidr", query: r#"{app="api",format="logfmt"} |= ip("10.0.0.0/8")"# },
        LogQueryCase { name: "parser_ip_single", query: r#"{app="api",format="logfmt"} |= ip("10.2.3.4")"# },
        LogQueryCase { name: "parser_ip_range", query: r#"{app="api",format="logfmt"} |= ip("10.2.3.0-10.2.3.10")"# },
        LogQueryCase { name: "parser_not_ip", query: r#"{app="api",format="logfmt"} != ip("192.168.0.0/16")"# },
        LogQueryCase { name: "parser_metadata", query: r#"{app="api",format="metadata"} | trace_id = "abc""# }
    ]
    .into_iter()
    .map(|case| Case::get(case.name, "/loki/api/v1/query_range").params(range_params(timeline, case.query, 1)).tenant(PARSER_TENANT))
    .collect()
}

fn parser_metric_cases(timeline: &Timeline) -> Vec<Case> {
    let names = [
        "parser_metric_count",
        "parser_metric_sum",
        "parser_metric_avg",
        "parser_metric_stdvar",
        "parser_metric_stddev",
        "parser_metric_quantile",
        "parser_metric_min",
        "parser_metric_max",
        "parser_metric_first",
        "parser_metric_last",
        "parser_metric_rate_counter",
        "parser_metric_bytes",
        "parser_metric_duration",
        "parser_metric_duration_seconds",
    ];
    let queries = [
        r#"count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])"#,
        r#"sum_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"avg_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"stdvar_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"stddev_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"quantile_over_time(0.75, {app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"min_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"max_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"first_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"last_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"rate_counter({app="api",format="logfmt"} | logfmt | unwrap requests | __error__ = "" [3s])"#,
        r#"sum_over_time({app="api",format="logfmt"} | logfmt | unwrap bytes(size) | __error__ = "" [3s])"#,
        r#"sum_over_time({app="api",format="logfmt"} | logfmt | unwrap duration(latency) | __error__ = "" [3s])"#,
        r#"sum_over_time({app="api",format="logfmt"} | logfmt | unwrap duration_seconds(latency) | __error__ = "" [3s])"#,
    ];
    (0..names.len())
        .map(|index| {
            Case::get(names[index], "/loki/api/v1/query_range")
                .params(range_params(timeline, queries[index], 1))
                .tenant(PARSER_TENANT)
        })
        .collect()
}

fn parser_instant_cases(timeline: &Timeline) -> Vec<Case> {
    let metric =
        r#"count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])"#;
    [
        LogQueryCase {
            name: "parser_instant_count",
            query: metric.to_string(),
        },
        LogQueryCase {
            name: "parser_instant_parenthesized",
            query: format!("({metric} * 2)"),
        },
        LogQueryCase {
            name: "parser_instant_operand",
            query: format!("({metric}) * 2"),
        },
        LogQueryCase {
            name: "parser_instant_add",
            query: format!("{metric} + on() vector(1)"),
        },
        LogQueryCase {
            name: "parser_instant_group_right",
            query: format!("vector(1) + on() group_right(app, env) {metric}"),
        },
        LogQueryCase {
            name: "parser_instant_comparison",
            query: format!("{metric} > bool on() vector(0)"),
        },
        LogQueryCase {
            name: "parser_instant_and",
            query: format!("{metric} and on() vector(1)"),
        },
        LogQueryCase {
            name: "parser_instant_or",
            query: format!("vector(1) or on() {metric}"),
        },
        LogQueryCase {
            name: "parser_instant_vector_and",
            query: format!("vector(1) and on() {metric}"),
        },
        LogQueryCase {
            name: "parser_instant_unless",
            query: format!("vector(1) unless on(app) {metric}"),
        },
        LogQueryCase {
            name: "parser_instant_group_right_comparison",
            query: format!("vector(2) > bool on() group_right(app, env) {metric}"),
        },
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, "/loki/api/v1/query")
            .params([
                QueryParameter::new("query", case.query),
                QueryParameter::new("time", timeline.at(10).to_string()),
            ])
            .tenant(PARSER_TENANT)
    })
    .collect()
}

/// The label and series endpoints Grafana's log browser drives.
fn metadata_cases(timeline: &Timeline) -> Vec<Case> {
    let mut cases = vec![
        Case::get("labels_endpoint", "/loki/api/v1/labels").params(window(timeline)),
        Case::get("series_endpoint", "/loki/api/v1/series")
            .params(window(timeline))
            .params([QueryParameter::new("match[]", r#"{app=~".+"}"#)]),
    ];
    for case in [
        NamedEndpointCase {
            name: "label_values_app",
            path: "app",
        },
        NamedEndpointCase {
            name: "label_values_env",
            path: "env",
        },
        NamedEndpointCase {
            name: "label_values_service_name",
            path: "service_name",
        },
    ] {
        cases.push(
            Case::get(
                case.name,
                format!("/loki/api/v1/label/{}/values", case.path),
            )
            .params(window(timeline)),
        );
    }
    cases
}

/// The deprecated `/api/prom` aliases, the `POST` forms, and `detected_labels`.
fn metadata_alias_cases(timeline: &Timeline) -> Vec<Case> {
    let api_matcher = [QueryParameter::new("match[]", r#"{app="api"}"#)];
    let worker_matcher = [QueryParameter::new("match[]", r#"{app="worker"}"#)];
    vec![
        Case::get("label_names_singular_path", "/loki/api/v1/label").params(window(timeline)),
        Case::get("api_prom_label_names", "/api/prom/label").params(window(timeline)),
        Case::get("api_prom_label_values", "/api/prom/label/app/values").params(window(timeline)),
        Case::get("series_single_matcher", "/loki/api/v1/series")
            .params(window(timeline))
            .params(api_matcher.clone()),
        Case::get("api_prom_series", "/api/prom/series")
            .params(window(timeline))
            .params(api_matcher.clone()),
        Case::post("series_post_query_matcher", "/loki/api/v1/series")
            .params(window(timeline))
            .params(worker_matcher),
        Case::post("series_post_form_matcher", "/loki/api/v1/series")
            .params(window(timeline))
            .form("match%5B%5D=%7Bapp%3D%22worker%22%7D"),
        Case::get("detected_labels_for_query", "/loki/api/v1/detected_labels")
            .params(window(timeline))
            .params([
                QueryParameter::new("query", r#"{app="api"}"#),
                QueryParameter::new("limit", "10"),
            ])
            .shape(Shape::DetectedLabels),
        Case::get("detected_labels_all", "/loki/api/v1/detected_labels")
            .params(window(timeline))
            .params([QueryParameter::new("limit", "10")])
            .shape(Shape::DetectedLabels),
        Case::get(
            "detected_labels_ignore_bad_step_and_limit",
            "/loki/api/v1/detected_labels",
        )
        .params(window(timeline))
        .params([
            QueryParameter::new("query", r#"{app="api"}"#),
            QueryParameter::new("step", "not-a-number"),
            QueryParameter::new("limit", "not-a-limit"),
        ])
        .shape(Shape::DetectedLabels),
    ]
}

/// The metadata and analytics endpoints over a tenant that holds nothing.
fn empty_tenant_cases() -> Vec<Case> {
    [
        EmptyTenantCase {
            name: "empty_labels",
            path: "/loki/api/v1/labels",
            query: "",
        },
        EmptyTenantCase {
            name: "empty_label_names_singular_path",
            path: "/loki/api/v1/label",
            query: "",
        },
        EmptyTenantCase {
            name: "empty_label_values",
            path: "/loki/api/v1/label/app/values",
            query: "",
        },
        EmptyTenantCase {
            name: "empty_api_prom_label_names",
            path: "/api/prom/label",
            query: "",
        },
        EmptyTenantCase {
            name: "empty_api_prom_label_values",
            path: "/api/prom/label/app/values",
            query: "",
        },
        EmptyTenantCase {
            name: "empty_detected_labels",
            path: "/loki/api/v1/detected_labels",
            query: "limit=10",
        },
        EmptyTenantCase {
            name: "empty_detected_fields",
            path: "/loki/api/v1/detected_fields",
            query: "query=%7Bapp%3D%22api%22%7D&limit=10",
        },
        EmptyTenantCase {
            name: "empty_detected_field_values",
            path: "/loki/api/v1/detected_field/status/values",
            query: "query=%7Bapp%3D%22api%22%7D&limit=10",
        },
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, case.path)
            .raw_query(case.query)
            .tenant(VACANT_TENANT)
    })
    .collect()
}

/// `detected_fields`, `patterns`, `index/volume`, `index/volume_range` and
/// `index/stats` over the seeded corpus.
fn analytics_cases(timeline: &Timeline) -> Vec<Case> {
    let api = r#"{app="api"}"#;
    let prod = r#"{env="prod"}"#;
    vec![
        Case::get("detected_fields", "/loki/api/v1/detected_fields")
            .params(window(timeline))
            .params([
                QueryParameter::new("query", api),
                QueryParameter::new("limit", "10"),
            ])
            .shape(Shape::DetectedFields),
        Case::get(
            "detected_field_values",
            "/loki/api/v1/detected_field/status/values",
        )
        .params(window(timeline))
        .params([
            QueryParameter::new("query", api),
            QueryParameter::new("limit", "10"),
        ])
        .shape(Shape::DetectedFields),
        // Repeated request-shaped worker lines force both pattern engines to
        // return content rather than letting an empty envelope pass vacuously.
        Case::get("patterns", "/loki/api/v1/patterns")
            .params(window(timeline))
            .params([
                QueryParameter::new("query", r#"{app="worker"}"#),
                QueryParameter::new("step", "1s"),
            ])
            .shape(Shape::Patterns),
        Case::get("index_volume", "/loki/api/v1/index/volume")
            .params(window(timeline))
            .params([
                QueryParameter::new("query", prod),
                QueryParameter::new("targetLabels", "app,env"),
            ])
            .shape(Shape::IndexVolume),
        Case::get("index_volume_range", "/loki/api/v1/index/volume_range")
            .params(window(timeline))
            .params([
                QueryParameter::new("query", prod),
                QueryParameter::new("targetLabels", "app,env"),
                QueryParameter::new("step", "1s"),
            ])
            .shape(Shape::IndexVolume),
        Case::get("index_stats", "/loki/api/v1/index/stats")
            .params(window(timeline))
            .params([QueryParameter::new("query", prod)])
            .shape(Shape::IndexStats),
    ]
}

/// `/format_query`, which is pure: no data, no clock, just the pretty-printer.
fn format_query_cases() -> Vec<Case> {
    [
        NamedQueryExpression { name: "format_query_selector", query: r#"{app="api",env="prod"}"# },
        NamedQueryExpression { name: "format_query_pipeline", query: r#"{app="api"}|="error"|json|status="500"|line_format "{{.msg}}""# },
        NamedQueryExpression { name: "format_query_metric", query: r#"sum by(app)(rate({app=~"api|worker"}|="error"[5m]))"# },
        NamedQueryExpression { name: "format_query_unwrap", query: r#"quantile_over_time(0.99,{app="fmt"}|logfmt|unwrap duration[5m])by(service)"# },
        NamedQueryExpression { name: "format_query_binary", query: r#"sum(count_over_time({app="api"}[1m]))/sum(count_over_time({app="worker"}[1m]))"# },
    ]
    .into_iter()
    .map(|case| Case::get(case.name, "/loki/api/v1/format_query").params([QueryParameter::new("query", case.query)]))
    .collect()
}

/// More `/format_query` expressions: binary operators with their modifiers,
/// `label_replace`, `sort`, offsets and number literals.
fn format_query_expression_cases() -> Vec<Case> {
    let mut cases: Vec<Case> = FORMAT_QUERY_EXPRESSIONS
        .iter()
        .map(|example| {
            Case::get(example.name, "/loki/api/v1/format_query")
                .params([QueryParameter::new("query", example.query)])
        })
        .collect();
    // The query string and the form body both name `query`. The two sides must
    // pick the same one.
    cases.push(
        Case::post("format_query_post_precedence", "/loki/api/v1/format_query")
            .params([QueryParameter::new("query", r#"{app="api"}"#)])
            .form("query=%7Bapp%3D%22worker%22%7D"),
    );
    cases
}

const FORMAT_QUERY_EXPRESSIONS: &[NamedQueryExpression] = &[
    NamedQueryExpression {
        name: "format_query_logfmt_regex_filters",
        query: r#"{app="api"} | logfmt | method=~"GET|POST" | path!~"/health.*""#,
    },
    NamedQueryExpression {
        name: "format_query_parenthesized_scalar",
        query: "(1+2)*3",
    },
    NamedQueryExpression {
        name: "format_query_vector_or",
        query: "vector(1) or vector(2)",
    },
    NamedQueryExpression {
        name: "format_query_vector_add",
        query: "vector(1)+vector(2)",
    },
    NamedQueryExpression {
        name: "format_query_metric_add_vector",
        query: r#"count_over_time({app="api"}[30s])+vector(1)"#,
    },
    NamedQueryExpression {
        name: "format_query_metric_add_exponent",
        query: r#"count_over_time({app="api"}[30s])+1.25e-1"#,
    },
    NamedQueryExpression {
        name: "format_query_offset",
        query: r#"count_over_time({app="api"}[10s] offset 1500ms)"#,
    },
    NamedQueryExpression {
        name: "format_query_scalar_bool_metric",
        query: r#"1>bool count_over_time({app="api"}[30s])"#,
    },
    NamedQueryExpression {
        name: "format_query_unwrap_add_vector",
        query: r#"quantile_over_time(0.75,{app="api"} | logfmt | unwrap cost [30s])+vector(1)"#,
    },
    NamedQueryExpression {
        name: "format_query_metric_or_on",
        query: r#"count_over_time({app="api"}[30s]) or on(app) vector(1)"#,
    },
    NamedQueryExpression {
        name: "format_query_vector_add_metric",
        query: r#"vector(1)+count_over_time({app="api"}[30s])"#,
    },
    NamedQueryExpression {
        name: "format_query_metric_add_on",
        query: r#"count_over_time({app="api"}[30s])+on(app)vector(1)"#,
    },
    NamedQueryExpression {
        name: "format_query_vector_group_left_metric",
        query: r#"vector(1)+on(app)group_left count_over_time({app="api"}[30s])"#,
    },
    NamedQueryExpression {
        name: "format_query_on_two_labels",
        query: "vector(1)+on(app,env)vector(2)",
    },
    NamedQueryExpression {
        name: "format_query_group_left_labels",
        query: "vector(1)+on(app)group_left(env)vector(2)",
    },
    NamedQueryExpression {
        name: "format_query_group_left_bare",
        query: "vector(1)+on(app)group_left vector(2)",
    },
    NamedQueryExpression {
        name: "format_query_metric_bool_vector",
        query: r#"count_over_time({app="api"}[30s])>bool vector(1)"#,
    },
    NamedQueryExpression {
        name: "format_query_vector_comparison_group_left",
        query: r#"vector(1)>on(app)group_left count_over_time({app="api"}[30s])"#,
    },
    NamedQueryExpression {
        name: "format_query_vector_bool_vector",
        query: "vector(1)>bool vector(2)",
    },
    NamedQueryExpression {
        name: "format_query_vector_exponent",
        query: "vector(2.5e-1)",
    },
    NamedQueryExpression {
        name: "format_query_trailing_grouping",
        query: r#"sum(rate({app="api"}|="error"[5m])) by (env,status)"#,
    },
    NamedQueryExpression {
        name: "format_query_metric_division",
        query: r#"count_over_time({app="api"}[30s]) / count_over_time({app="api"} |= "error" [30s])"#,
    },
    NamedQueryExpression {
        name: "format_query_metric_bool_metric",
        query: r#"count_over_time({app="api"}[30s]) > bool count_over_time({app="worker"}[30s])"#,
    },
    NamedQueryExpression {
        name: "format_query_metric_or_metric",
        query: r#"count_over_time({app="api"}[30s]) or count_over_time({app="worker"}[30s])"#,
    },
    NamedQueryExpression {
        name: "format_query_ignoring",
        query: r#"count_over_time({app="api"}[30s]) / ignoring(app) count_over_time({app="worker"}[30s])"#,
    },
    NamedQueryExpression {
        name: "format_query_aggregation_group_left",
        query: r#"sum by(app, env)(count_over_time({env="prod"}[30s])) / on(env) group_left sum by(env)(count_over_time({env="prod"}[30s]))"#,
    },
    NamedQueryExpression {
        name: "format_query_sort_metric_add",
        query: r#"sort(count_over_time({app="api"}[30s]) + vector(1))"#,
    },
    NamedQueryExpression {
        name: "format_query_sort_label_replace",
        query: r#"sort(label_replace(vector(1), "service", "api-$1", "missing", "(.*)"))"#,
    },
    NamedQueryExpression {
        name: "format_query_sort_desc_label_replace_metric",
        query: r#"sort_desc(label_replace(count_over_time({app="api"}[30s]) + vector(1), "service", "$1-api", "app", "(.*)"))"#,
    },
    NamedQueryExpression {
        name: "format_query_quantile_leading_dot",
        query: r#"quantile_over_time(.75,{app="api"}|logfmt|unwrap cost[30s]) by(app)"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_filtered_metric",
        query: r#"label_replace(count_over_time({app="api"}|="error"[30s]), "service", "$1-api", "app", "(.*)")"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_vector",
        query: r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)")"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_vector_sum",
        query: r#"label_replace(vector(1)+vector(2), "service", "api-$1", "missing", "(.*)")"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_vector_add",
        query: r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)") + vector(2)"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_vector_or",
        query: r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)") or vector(2)"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_metric_sum",
        query: r#"label_replace(count_over_time({app="api"}[30s]) + vector(1), "service", "$1-api", "app", "(.*)")"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_metric_add",
        query: r#"label_replace(count_over_time({app="api"}[30s]), "service", "$1-api", "app", "(.*)") + vector(2)"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_metric_sum_add",
        query: r#"label_replace(count_over_time({app="api"}[30s]) + vector(1), "service", "$1-api", "app", "(.*)") + vector(2)"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_metric_exponent_add",
        query: r#"label_replace(count_over_time({app="api"}[30s]) + 1.25e-1, "service", "$1-api", "app", "(.*)") + vector(2)"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_metric_or",
        query: r#"label_replace(count_over_time({app="api"}[30s]), "service", "$1-api", "app", "(.*)") or vector(2)"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_metric_sum_or",
        query: r#"label_replace(count_over_time({app="api"}[30s]) + vector(1), "service", "$1-api", "app", "(.*)") or vector(2)"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_metric_bool",
        query: r#"label_replace(count_over_time({app="api"}[30s]), "service", "$1-api", "app", "(.*)") > bool vector(2)"#,
    },
    NamedQueryExpression {
        name: "format_query_label_replace_metric_exponent",
        query: r#"label_replace(count_over_time({app="api"}[30s]) + 1.25e-1, "service", "$1-api", "app", "(.*)")"#,
    },
];

/// `/format_query` refusals.
fn format_query_error_cases() -> Vec<Case> {
    let mut cases: Vec<Case> = [
        NamedQueryExpression {
            name: "format_query_error_unterminated_selector",
            query: "{foo=",
        },
        NamedQueryExpression {
            name: "format_query_error_negative_exponent",
            query: "vector(-2.5e-1)",
        },
        NamedQueryExpression {
            name: "format_query_error_unspaced_or",
            query: "vector(1)orvector(2)",
        },
        NamedQueryExpression {
            name: "format_query_error_label_join_metric",
            query: r#"label_join(count_over_time({app="api"}[30s]), "joined", "/", "app")"#,
        },
        NamedQueryExpression {
            name: "format_query_error_label_join_vector",
            query: r#"label_join(vector(1), "joined", "/", "app")"#,
        },
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, "/loki/api/v1/format_query")
            .params([QueryParameter::new("query", case.query)])
    })
    .collect();
    cases.push(Case::get(
        "format_query_error_missing_query",
        "/loki/api/v1/format_query",
    ));
    cases
}

/// `/query` refusals for expressions the parser rejects.
fn query_error_cases() -> Vec<Case> {
    [
        NamedQueryExpression {
            name: "query_error_unterminated_selector",
            query: "{app=",
        },
        NamedQueryExpression {
            name: "query_error_negative_exponent",
            query: "vector(-2.5e-1)",
        },
        NamedQueryExpression {
            name: "query_error_unspaced_or",
            query: "vector(1)orvector(2)",
        },
        NamedQueryExpression {
            name: "query_error_abs",
            query: "abs(vector(-1.2))",
        },
        NamedQueryExpression {
            name: "query_error_count_values_leading_grouping",
            query: r#"count_values by (env) ("hits", count_over_time({app="api"}[1s]))"#,
        },
        NamedQueryExpression {
            name: "query_error_count_values_trailing_grouping",
            query: r#"count_values("hits", count_over_time({app="api"}[1s])) by (env)"#,
        },
        NamedQueryExpression {
            name: "query_error_approx_topk",
            query: r#"approx_topk(1, count_over_time({app="api"}[1s]))"#,
        },
    ]
    .into_iter()
    .map(|case| {
        Case::get(case.name, "/loki/api/v1/query")
            .params([QueryParameter::new("query", case.query)])
    })
    .collect()
}

/// Parameter parsing on `/query` and `/query_range`: bad values, repeated
/// names, and which of the query string and the form body wins.
fn query_parameter_cases(timeline: &Timeline) -> Vec<Case> {
    let api = r#"{app="api"}"#;
    let window_30s = r#"count_over_time({app="api"}[30s])"#;
    let near_now = [
        QueryParameter::new("start", timeline.at(299).to_string()),
        QueryParameter::new("end", timeline.at(300).to_string()),
    ];
    let mut cases = vec![
        Case::get("query_duplicate_query_param", "/loki/api/v1/query")
            .raw_query("query=vector%281%29&query=vector%282%29")
            .shape(Shape::ApiAtNow),
        // Nineteen digits, which both sides read as nanoseconds. The source
        // suite used `time=0.000000001&time=2`, and `query_range_bounds_in_seconds`
        // says why a short timestamp does not ask about precedence alone.
        Case::get("query_duplicate_time_param", "/loki/api/v1/query")
            .raw_query("query=vector%281%29&time=1000000000000000000&time=2000000000000000000"),
        Case::get("query_single_digit_time", "/loki/api/v1/query")
            .raw_query("query=vector%281%29&time=1"),
        Case::get("query_zero_padded_single_digit_time", "/loki/api/v1/query")
            .raw_query("query=vector%281%29&time=01"),
        Case::get("query_forwarded_subsecond_time", "/loki/api/v1/query")
            .raw_query("query=vector%281%29&time=0.5"),
        Case::get("query_forwarded_time_overflow", "/loki/api/v1/query")
            .raw_query("query=vector%281%29&time=9.223372037"),
        Case::get("query_vector_at_corpus_time", "/loki/api/v1/query").params([
            QueryParameter::new("query", "vector(1)".to_string()),
            QueryParameter::new("time", timeline.at(60).to_string()),
        ]),
        Case::get("query_range_bounds_in_seconds", "/loki/api/v1/query_range").params([
            QueryParameter::new("query", api.to_string()),
            QueryParameter::new("start", (timeline.at(0) / 1_000_000_000).to_string()),
            QueryParameter::new("end", (timeline.at(60) / 1_000_000_000).to_string()),
            QueryParameter::new("direction", "forward".to_string()),
        ]),
        Case::get("query_range_invalid_direction", "/loki/api/v1/query_range").params([
            QueryParameter::new("query", api),
            QueryParameter::new("start", "0.000000000"),
            QueryParameter::new("end", "1.000000000"),
            QueryParameter::new("direction", "sideways"),
        ]),
        Case::get("query_range_zero_step", "/loki/api/v1/query_range").params([
            QueryParameter::new("query", window_30s),
            QueryParameter::new("start", "0.000000000"),
            QueryParameter::new("end", "1.000000000"),
            QueryParameter::new("step", "0"),
        ]),
        Case::get("query_range_unparseable_step", "/loki/api/v1/query_range").params([
            QueryParameter::new("query", window_30s),
            QueryParameter::new("start", "0.000000000"),
            QueryParameter::new("end", "1.000000000"),
            QueryParameter::new("step", "not-a-number"),
        ]),
        Case::get(
            "query_range_excessive_resolution",
            "/loki/api/v1/query_range",
        )
        .params([
            QueryParameter::new("query", "vector(1)"),
            QueryParameter::new("start", "0.000000000"),
            QueryParameter::new("end", "11001000000000"),
            QueryParameter::new("step", "1s"),
        ]),
        Case::get("query_range_oversized_range", "/loki/api/v1/query_range").params([
            QueryParameter::new("query", "vector(1)"),
            QueryParameter::new("start", "0.000000000"),
            QueryParameter::new("end", OVERSIZED_END_NS),
            QueryParameter::new("step", "1h"),
        ]),
        Case::get("query_range_unparseable_start", "/loki/api/v1/query_range").params([
            QueryParameter::new("query", api),
            QueryParameter::new("start", "not-a-number"),
            QueryParameter::new("end", "1.000000000"),
        ]),
        Case::get("query_range_negative_since", "/loki/api/v1/query_range").params([
            QueryParameter::new("query", api),
            QueryParameter::new("end", "1.000000000"),
            QueryParameter::new("since", "-1"),
        ]),
        Case::get("query_range_zero_interval", "/loki/api/v1/query_range")
            .params([QueryParameter::new("query", api)])
            .params(near_now.clone())
            .params([QueryParameter::new("interval", "0")]),
        Case::get("query_range_negative_interval", "/loki/api/v1/query_range")
            .params([QueryParameter::new("query", api)])
            .params(near_now)
            .params([QueryParameter::new("interval", "-1")]),
        Case::get("query_unparseable_limit", "/loki/api/v1/query").params([
            QueryParameter::new("query", api),
            QueryParameter::new("limit", "not-a-number"),
        ]),
        Case::get("query_negative_limit", "/loki/api/v1/query").params([
            QueryParameter::new("query", api),
            QueryParameter::new("limit", "-1"),
        ]),
    ];
    // The form body carries an unterminated selector, the query string a valid
    // one. Loki reads the body first.
    for case in [
        NamedRawQueryCase {
            name: "query_post_body_precedence",
            path: "/loki/api/v1/query",
            query: "query=%7Bapp%3D%22api%22%7D&time=1.000000000",
        },
        NamedRawQueryCase {
            name: "query_range_post_body_precedence",
            path: "/loki/api/v1/query_range",
            query: "query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=1.000000000",
        },
        NamedRawQueryCase {
            name: "api_prom_query_post_body_precedence",
            path: "/api/prom/query",
            query: "query=%7Bapp%3D%22api%22%7D&time=1.000000000",
        },
    ] {
        cases.push(
            Case::post(case.name, case.path)
                .raw_query(case.query)
                .form("query=%7Bapp%3D"),
        );
    }
    let missing_query_window = [
        QueryParameter::new("start", timeline.at(240).to_string()),
        QueryParameter::new("end", timeline.at(241).to_string()),
    ];
    cases.push(Case::get("missing_query_instant", "/loki/api/v1/query"));
    for case in [
        NamedEndpointCase {
            name: "missing_query_range",
            path: "/loki/api/v1/query_range",
        },
        NamedEndpointCase {
            name: "missing_query_index_stats",
            path: "/loki/api/v1/index/stats",
        },
        NamedEndpointCase {
            name: "missing_query_index_volume",
            path: "/loki/api/v1/index/volume",
        },
        NamedEndpointCase {
            name: "missing_query_detected_fields",
            path: "/loki/api/v1/detected_fields",
        },
        NamedEndpointCase {
            name: "missing_query_detected_field_values",
            path: "/loki/api/v1/detected_field/status/values",
        },
    ] {
        cases.push(Case::get(case.name, case.path).params(missing_query_window.clone()));
    }
    cases.push(
        Case::get(
            "missing_query_index_volume_range",
            "/loki/api/v1/index/volume_range",
        )
        .params(missing_query_window)
        .params([QueryParameter::new("step", "1000000000")]),
    );
    cases
}

/// `/labels`, `/label/{name}/values` and `/series` refusals, and the form-body
/// precedence those endpoints share.
fn metadata_error_cases() -> Vec<Case> {
    let mut cases: Vec<Case> = [
        NamedRawQueryCase {
            name: "labels_invalid_query",
            path: "labels",
            query: "query=%7Bapp%3D",
        },
        NamedRawQueryCase {
            name: "label_values_invalid_query",
            path: "label/app/values",
            query: "query=%7Bapp%3D",
        },
        NamedRawQueryCase {
            name: "series_invalid_matcher",
            path: "series",
            query: "match[]=%7Bapp%3D",
        },
        NamedRawQueryCase {
            name: "series_unparseable_start",
            path: "series",
            query: "start=not-a-number",
        },
        NamedRawQueryCase {
            name: "labels_duplicate_start_param",
            path: "labels",
            query: "start=0.000000000&start=not-a-number",
        },
        NamedRawQueryCase {
            name: "labels_oversized_range",
            path: "labels",
            query: "start=0.000000000&end=2595601000000000",
        },
        NamedRawQueryCase {
            name: "label_values_oversized_range",
            path: "label/app/values",
            query: "start=0.000000000&end=2595601000000000",
        },
        NamedRawQueryCase {
            name: "series_oversized_range",
            path: "series",
            query: "match[]=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2595601000000000",
        },
    ]
    .into_iter()
    .map(|case| Case::get(case.name, format!("/loki/api/v1/{}", case.path)).raw_query(case.query))
    .collect();
    for case in [
        NamedEndpointCase {
            name: "series_missing_matcher",
            path: "/loki/api/v1/series",
        },
        NamedEndpointCase {
            name: "api_prom_series_missing_matcher",
            path: "/api/prom/series",
        },
    ] {
        cases.push(Case::get(case.name, case.path));
    }
    for case in [
        NamedEndpointCase {
            name: "series_post_empty",
            path: "/loki/api/v1/series",
        },
        NamedEndpointCase {
            name: "api_prom_series_post_empty",
            path: "/api/prom/series",
        },
    ] {
        cases.push(Case::post(case.name, case.path));
    }
    // The query string carries a start that does not parse, the form body a
    // valid one and a range too long to answer. The refusal says which won.
    for case in [
        NamedEndpointCase {
            name: "labels_post_body_precedence",
            path: "/loki/api/v1/labels",
        },
        NamedEndpointCase {
            name: "label_values_post_body_precedence",
            path: "/loki/api/v1/label/app/values",
        },
        NamedEndpointCase {
            name: "series_post_body_precedence",
            path: "/loki/api/v1/series",
        },
        NamedEndpointCase {
            name: "api_prom_label_post_body_precedence",
            path: "/api/prom/label",
        },
        NamedEndpointCase {
            name: "api_prom_label_values_post_body_precedence",
            path: "/api/prom/label/app/values",
        },
        NamedEndpointCase {
            name: "api_prom_series_post_body_precedence",
            path: "/api/prom/series",
        },
    ] {
        cases.push(
            Case::post(case.name, case.path)
                .raw_query("start=not-a-number")
                .form("start=0.000000000&end=2595601000000000&match%5B%5D=%7Bapp%3D%22api%22%7D"),
        );
    }
    cases
}

/// `/index/stats`, `/index/volume` and `/index/volume_range` refusals.
#[allow(clippy::items_after_statements)]
fn index_error_cases(timeline: &Timeline) -> Vec<Case> {
    let api = r#"{app="api"}"#;
    let mut cases = Vec::new();
    for case in [
        NamedStepCase {
            name: "index_volume_range_zero_step",
            endpoint: "index/volume_range",
            step: "0",
        },
        NamedStepCase {
            name: "index_volume_range_unparseable_step",
            endpoint: "index/volume_range",
            step: "not-a-number",
        },
    ] {
        cases.push(
            Case::get(case.name, format!("/loki/api/v1/{}", case.endpoint)).params([
                QueryParameter::new("query", api),
                QueryParameter::new("start", "0.000000000"),
                QueryParameter::new("end", "1.000000000"),
                QueryParameter::new("step", case.step),
            ]),
        );
    }
    for case in [
        NamedEndpointCase {
            name: "index_volume_unknown_aggregate_by",
            path: "index/volume",
        },
        NamedEndpointCase {
            name: "index_volume_range_unknown_aggregate_by",
            path: "index/volume_range",
        },
    ] {
        cases.push(
            Case::get(case.name, format!("/loki/api/v1/{}", case.path)).params([
                QueryParameter::new("query", api),
                QueryParameter::new("start", "0.000000000"),
                QueryParameter::new("end", "1.000000000"),
                QueryParameter::new("aggregateBy", "bogus"),
            ]),
        );
    }
    struct MissingIndexParameterCase {
        name: &'static str,
        endpoint: &'static str,
        params: Vec<QueryParameter<'static>>,
    }
    for case in [
        MissingIndexParameterCase {
            name: "index_volume_missing_start",
            endpoint: "index/volume",
            params: vec![QueryParameter::new("end", "1.000000000")],
        },
        MissingIndexParameterCase {
            name: "index_volume_missing_end",
            endpoint: "index/volume",
            params: vec![QueryParameter::new("start", "0.000000000")],
        },
        MissingIndexParameterCase {
            name: "index_volume_range_missing_start",
            endpoint: "index/volume_range",
            params: vec![
                QueryParameter::new("end", "1.000000000"),
                QueryParameter::new("step", "1000000000"),
            ],
        },
        MissingIndexParameterCase {
            name: "index_volume_range_missing_end",
            endpoint: "index/volume_range",
            params: vec![
                QueryParameter::new("start", "0.000000000"),
                QueryParameter::new("step", "1000000000"),
            ],
        },
    ] {
        cases.push(
            Case::get(case.name, format!("/loki/api/v1/{}", case.endpoint))
                .params([QueryParameter::new("query", api)])
                .params(case.params)
                .shape(Shape::IndexVolume),
        );
    }
    let recent = [
        QueryParameter::new("start", timeline.at(240).to_string()),
        QueryParameter::new("end", timeline.at(241).to_string()),
    ];
    for case in [
        NamedEndpointCase {
            name: "index_stats_invalid_query",
            path: "index/stats",
        },
        NamedEndpointCase {
            name: "index_volume_invalid_query",
            path: "index/volume",
        },
        NamedEndpointCase {
            name: "index_volume_range_invalid_query",
            path: "index/volume_range",
        },
    ] {
        cases.push(
            Case::get(case.name, format!("/loki/api/v1/{}", case.path))
                .params([QueryParameter::new("query", "{app=")])
                .params(recent.clone()),
        );
    }
    cases.push(
        Case::get(
            "index_volume_duplicate_query_param",
            "/loki/api/v1/index/volume",
        )
        .params([
            QueryParameter::new("query", api),
            QueryParameter::new("query", "{app="),
            QueryParameter::new("start", "0.000000000"),
            QueryParameter::new("end", "0.000000001"),
        ])
        .shape(Shape::IndexVolume),
    );
    cases.push(
        Case::get("index_stats_oversized_range", "/loki/api/v1/index/stats").params([
            QueryParameter::new("query", api),
            QueryParameter::new("start", "0.000000000"),
            QueryParameter::new("end", OVERSIZED_END_NS),
        ]),
    );
    cases.push(
        Case::post(
            "index_stats_post_body_precedence",
            "/loki/api/v1/index/stats",
        )
        .raw_query("start=not-a-number")
        .form("query=%7Bapp%3D%22api%22%7D&start=0.000000000&end=2595601000000000"),
    );
    cases
}

/// `/detected_labels`, `/detected_fields` and `/detected_field/{name}/values`
/// refusals.
fn detected_error_cases() -> Vec<Case> {
    let api = r#"{app="api"}"#;
    let mut cases = Vec::new();
    for case in [
        NamedStepCase {
            name: "detected_fields_zero_step",
            endpoint: "detected_fields",
            step: "0",
        },
        NamedStepCase {
            name: "detected_fields_unparseable_step",
            endpoint: "detected_fields",
            step: "not-a-number",
        },
        NamedStepCase {
            name: "detected_field_values_zero_step",
            endpoint: "detected_field/status/values",
            step: "0",
        },
        NamedStepCase {
            name: "detected_field_values_unparseable_step",
            endpoint: "detected_field/status/values",
            step: "not-a-number",
        },
    ] {
        cases.push(
            Case::get(case.name, format!("/loki/api/v1/{}", case.endpoint)).params([
                QueryParameter::new("query", api),
                QueryParameter::new("start", "0.000000000"),
                QueryParameter::new("end", "1.000000000"),
                QueryParameter::new("step", case.step),
            ]),
        );
    }
    for case in [
        NamedEndpointCase {
            name: "detected_fields_invalid_query",
            path: "detected_fields",
        },
        NamedEndpointCase {
            name: "detected_field_values_invalid_query",
            path: "detected_field/status/values",
        },
    ] {
        cases.push(
            Case::get(case.name, format!("/loki/api/v1/{}", case.path)).params([
                QueryParameter::new("query", "{app="),
                QueryParameter::new("start", "0.000000000"),
                QueryParameter::new("end", "1.000000000"),
            ]),
        );
    }
    for case in [
        DetectedEndpointCase {
            suffix: "labels",
            endpoint: "detected_labels",
            query: None,
        },
        DetectedEndpointCase {
            suffix: "fields",
            endpoint: "detected_fields",
            query: Some(api),
        },
        DetectedEndpointCase {
            suffix: "field_values",
            endpoint: "detected_field/status/values",
            query: Some(api),
        },
    ] {
        let query: Vec<QueryParameter<'_>> = case
            .query
            .map(|query| QueryParameter::new("query", query))
            .into_iter()
            .collect();
        let path = format!("/loki/api/v1/{}", case.endpoint);
        cases.push(
            Case::get(detected_name("oversized_range", case.suffix), path.clone())
                .params([
                    QueryParameter::new("start", "0.000000000"),
                    QueryParameter::new("end", OVERSIZED_END_NS),
                ])
                .params(query.clone()),
        );
        cases.push(
            Case::get(
                detected_name("duplicate_start_param", case.suffix),
                path.clone(),
            )
            .params([
                QueryParameter::new("start", "0.000000000"),
                QueryParameter::new("start", "not-a-number"),
                QueryParameter::new("end", OVERSIZED_END_NS),
            ])
            .params(query),
        );
        cases.push(
            Case::post(detected_name("post_body_precedence", case.suffix), path)
                .raw_query("start=not-a-number")
                .form("start=0.000000000&end=2595601000000000&query=%7Bapp%3D%22api%22%7D"),
        );
    }
    cases
}

/// A case name for one of the three detected endpoints.
///
/// Case names are `&'static str`, and this is the one family that composes
/// them, so the composed names live in a table rather than being leaked.
fn detected_name(kind: &str, endpoint: &str) -> &'static str {
    DETECTED_CASE_NAMES
        .iter()
        .find(|case| case.kind == kind && case.endpoint == endpoint)
        .map_or("detected_unnamed", |case| case.name)
}

const DETECTED_CASE_NAMES: &[DetectedCaseName] = &[
    DetectedCaseName {
        kind: "oversized_range",
        endpoint: "labels",
        name: "detected_labels_oversized_range",
    },
    DetectedCaseName {
        kind: "oversized_range",
        endpoint: "fields",
        name: "detected_fields_oversized_range",
    },
    DetectedCaseName {
        kind: "oversized_range",
        endpoint: "field_values",
        name: "detected_field_values_oversized_range",
    },
    DetectedCaseName {
        kind: "duplicate_start_param",
        endpoint: "labels",
        name: "detected_labels_duplicate_start_param",
    },
    DetectedCaseName {
        kind: "duplicate_start_param",
        endpoint: "fields",
        name: "detected_fields_duplicate_start_param",
    },
    DetectedCaseName {
        kind: "duplicate_start_param",
        endpoint: "field_values",
        name: "detected_field_values_duplicate_start_param",
    },
    DetectedCaseName {
        kind: "post_body_precedence",
        endpoint: "labels",
        name: "detected_labels_post_body_precedence",
    },
    DetectedCaseName {
        kind: "post_body_precedence",
        endpoint: "fields",
        name: "detected_fields_post_body_precedence",
    },
    DetectedCaseName {
        kind: "post_body_precedence",
        endpoint: "field_values",
        name: "detected_field_values_post_body_precedence",
    },
];

/// The websocket tail: refused upgrades, the first frame of a tail that
/// replays the seeded `api` stream, and the first frame of a tail that sees a
/// push arrive after it opened.
///
/// The live cases come first, each on a tenant of its own, so that nothing is
/// replayed ahead of the pushed entry.
fn tail_cases(timeline: &Timeline) -> Vec<Case> {
    let start = timeline.at(-1).to_string();
    let live_entry = json!({"streams": [{
        "stream": {"app": "live", "env": "prod"},
        "values": [[timeline.at(300).to_string(), "level=error msg=boom", {"trace_id": "abc"}]]
    }]})
    .to_string();
    let live = |name, tenant, query| {
        let mut case = Case::get(name, "/loki/api/v1/tail")
            .params([QueryParameter::new("query", query)])
            .tenant(tenant)
            .shape(Shape::LiveTailFrame);
        case.body = live_entry.clone().into_bytes();
        case
    };
    vec![
        live("tail_live_frame", "tail-live", r#"{app="live"}"#),
        live(
            "tail_live_parser_frame",
            "tail-live-parser",
            r#"{app="live"} | logfmt | line_format `{{.msg}}`"#,
        ),
        live(
            "tail_live_frame_categorized",
            "tail-live-categorized",
            r#"{app="live"}"#,
        )
        .categorized(),
        Case::get("tail_invalid_query", "/loki/api/v1/tail")
            .params([QueryParameter::new("query", "{app=")])
            .shape(Shape::Handshake),
        Case::get("tail_missing_query", "/loki/api/v1/tail").shape(Shape::Handshake),
        Case::get("tail_excessive_delay_for", "/loki/api/v1/tail")
            .raw_query("query=%7Bapp%3D%22api%22%7D&delay_for=6")
            .shape(Shape::Handshake),
        Case::get("tail_first_frame", "/loki/api/v1/tail")
            .params([
                QueryParameter::new("query", r#"{app="api"}"#),
                QueryParameter::new("start", start.as_str()),
            ])
            .shape(Shape::TailFrame),
        Case::get("tail_first_frame_categorized", "/loki/api/v1/tail")
            .params([
                QueryParameter::new("query", r#"{app="api"}"#),
                QueryParameter::new("start", start.as_str()),
            ])
            .categorized()
            .shape(Shape::TailFrame),
    ]
}

struct PushBody {
    name: &'static str,
    body: String,
}

macro_rules! push_body {
    ($name:expr, $body:expr $(,)?) => {
        PushBody {
            name: $name,
            body: $body,
        }
    };
}

/// `/loki/api/v1/push` with JSON bodies that Loki refuses, or accepts in a way
/// worth pinning down.
fn json_push_cases(timeline: &Timeline) -> Vec<Case> {
    let now = timeline.at(300).to_string();
    let future = timeline.at(300 + 20 * 60).to_string();
    let entry = |timestamp: &str, line: &str| json!([timestamp, line]);
    let one_stream = |labels: Value, values: Value| {
        json!({"streams": [{"stream": labels, "values": values}]}).to_string()
    };
    let app = || json!({"app": "api"});
    let bodies: Vec<PushBody> = vec![
        push_body!(
            "push_invalid_label_name",
            one_stream(
                json!({"bad-label": "api"}),
                json!([["1000000000", "invalid push label"]]),
            ),
        ),
        push_body!(
            "push_stale_timestamp",
            one_stream(app(), json!([["1000000000", "stale push timestamp"]])),
        ),
        push_body!(
            "push_unparseable_timestamp",
            one_stream(
                app(),
                json!([["not-a-timestamp", "invalid push timestamp"]]),
            ),
        ),
        push_body!(
            "push_numeric_timestamp",
            one_stream(app(), json!([[1_000_000_000, "non-string push timestamp"]])),
        ),
        push_body!(
            "push_object_timestamp",
            one_stream(
                app(),
                json!([[{"ts": "1000000000"}, "object push timestamp"]]),
            ),
        ),
        push_body!(
            "push_array_timestamp",
            one_stream(app(), json!([[["1000000000"], "array push timestamp"]])),
        ),
        push_body!(
            "push_numeric_line",
            one_stream(app(), json!([["1000000000", 500]])),
        ),
        push_body!("push_incomplete_value", one_stream(app(), json!([[now]]))),
        push_body!("push_empty_value", one_stream(app(), json!([[]]))),
        push_body!(
            "push_null_metadata",
            one_stream(app(), json!([[now, "invalid metadata shape", null]])),
        ),
        push_body!(
            "push_extra_value_field",
            one_stream(
                app(),
                json!([[now, "extra push value field", {"trace_id": "abc"}, "extra"]]),
            ),
        ),
        push_body!(
            "push_non_array_value",
            one_stream(
                app(),
                json!([entry(&now, "valid line"), "not-a-push-value"]),
            ),
        ),
        push_body!(
            "push_non_object_stream",
            json!({"streams": ["not-a-stream"]}).to_string(),
        ),
        push_body!(
            "push_non_array_streams",
            json!({"streams": "not-streams"}).to_string(),
        ),
        push_body!("push_array_payload", r#"[{"streams": []}]"#.to_string()),
        push_body!("push_null_payload", "null".to_string()),
        push_body!("push_missing_streams", "{}".to_string()),
        push_body!("push_empty_streams", json!({"streams": []}).to_string()),
        push_body!(
            "push_missing_values",
            json!({"streams": [{"stream": {"app": "api"}}]}).to_string(),
        ),
        push_body!(
            "push_non_array_values",
            one_stream(app(), json!("not-values")),
        ),
        push_body!("push_null_values", one_stream(app(), Value::Null)),
        push_body!(
            "push_non_object_labels",
            one_stream(
                json!("not-labels"),
                json!([entry(&now, "labels field is not an object")]),
            ),
        ),
        push_body!(
            "push_missing_labels",
            json!({"streams": [{"values": [[now, "missing labels field"]]}]}).to_string(),
        ),
        push_body!(
            "push_null_labels",
            one_stream(Value::Null, json!([entry(&now, "null labels field")])),
        ),
        push_body!(
            "push_future_timestamp",
            one_stream(app(), json!([entry(&future, "future push timestamp")])),
        ),
        push_body!(
            "push_empty_labels",
            one_stream(json!({}), json!([entry(&now, "empty push label")])),
        ),
        push_body!(
            "push_numeric_structured_metadata",
            one_stream(
                app(),
                json!([["1000000000", "invalid metadata value", {"status": 500}]]),
            ),
        ),
        push_body!(
            "push_invalid_structured_metadata_name",
            one_stream(
                app(),
                json!([[now, "invalid metadata name", {"9bad": "metadata"}]]),
            ),
        ),
        push_body!(
            "push_duplicate_structured_metadata_name",
            format!(
                r#"{{"streams":[{{"stream":{{"app":"api"}},"values":[["{now}","duplicate structured metadata",{{"trace_id":"abc","trace_id":"def"}}]]}}]}}"#
            ),
        ),
        push_body!(
            "push_duplicate_label_name",
            format!(
                r#"{{"streams":[{{"stream":{{"app":"api","app":"worker"}},"values":[["{now}","duplicate push label"]]}}]}}"#
            ),
        ),
    ];
    let mut cases: Vec<Case> = bodies
        .into_iter()
        .map(|body| push_case(body.name).body("application/json", body.body))
        .collect();
    cases.push(
        Case::post("otlp_future_timestamp", "/otlp/v1/logs")
            .tenant(PUSH_TENANT)
            .target(Target::Distributor)
            .body(
                "application/json",
                json!({
                    "resourceLogs": [{
                        "resource": {"attributes": [
                            {"key": "service.name", "value": {"stringValue": "checkout"}}
                        ]},
                        "scopeLogs": [{"logRecords": [{
                            "timeUnixNano": future,
                            "body": {"stringValue": "future otlp timestamp"}
                        }]}]
                    }]
                })
                .to_string(),
            ),
    );
    cases
}

/// A push to [`PUSH_TENANT`] through the distributor.
fn push_case(name: &'static str) -> Case {
    Case::post(name, "/loki/api/v1/push")
        .tenant(PUSH_TENANT)
        .target(Target::Distributor)
}

/// Pushes that differ by `Content-Encoding`.
fn push_encoding_cases(timeline: &Timeline) -> TestResult<Vec<Case>> {
    let payload = |line: &str| {
        json!({"streams": [{"stream": {"app": "api"}, "values": [[timeline.at(300).to_string(), line]]}]})
            .to_string()
    };
    let mut deflate =
        flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    deflate.write_all(payload("deflated json push").as_bytes())?;
    Ok(vec![
        push_case("push_deflate_json")
            .body("application/json", deflate.finish()?)
            .header("Content-Encoding", "deflate"),
        push_case("push_malformed_gzip")
            .body("application/json", b"not gzip".to_vec())
            .header("Content-Encoding", "gzip"),
        push_case("push_malformed_deflate")
            .body("application/json", b"not deflate".to_vec())
            .header("Content-Encoding", "deflate"),
        push_case("push_unsupported_content_encoding")
            .body("application/json", payload("unsupported encoding"))
            .header("Content-Encoding", "br"),
    ])
}

/// Pushes as snappy-compressed protobuf, and one query that reads back what
/// such a push wrote.
fn protobuf_push_cases(timeline: &Timeline) -> TestResult<Vec<Case>> {
    let now = Some(timeline.at(300) / 1_000_000_000);
    let entry = |seconds: Option<i64>, line: &str| LokiProtoEntry {
        timestamp: seconds.map(|seconds| LokiProtoTimestamp { seconds, nanos: 0 }),
        line: line.to_string(),
        structured_metadata: Vec::new(),
        parsed: Vec::new(),
    };
    let pair = |name: &str, value: &str| LokiProtoLabelPair {
        name: name.to_string(),
        value: value.to_string(),
    };
    let stream = |labels: &str, entry: LokiProtoEntry| LokiProtoStream {
        labels: labels.to_string(),
        entries: vec![entry],
        hash: 0,
    };
    let parsed_at = timeline.at(240);
    let requests = vec![
        ProtobufPushCase {
            name: "push_protobuf_empty",
            streams: Vec::new(),
        },
        ProtobufPushCase {
            name: "push_protobuf_invalid_label_name",
            streams: vec![stream(
                r#"{bad-label="api"}"#,
                entry(now, "invalid protobuf label"),
            )],
        },
        ProtobufPushCase {
            name: "push_protobuf_duplicate_label_name",
            streams: vec![stream(
                r#"{app="api", app="worker"}"#,
                entry(now, "duplicate protobuf label"),
            )],
        },
        ProtobufPushCase {
            name: "push_protobuf_empty_label_set",
            streams: vec![stream("{}", entry(now, "empty protobuf stream labels"))],
        },
        ProtobufPushCase {
            name: "push_protobuf_empty_label_string",
            streams: vec![stream(
                "",
                entry(now, "empty string protobuf stream labels"),
            )],
        },
        ProtobufPushCase {
            name: "push_protobuf_missing_timestamp",
            streams: vec![stream(
                r#"{app="api"}"#,
                entry(None, "missing protobuf timestamp"),
            )],
        },
        ProtobufPushCase {
            name: "push_protobuf_negative_timestamp",
            streams: vec![stream(
                r#"{app="api"}"#,
                entry(Some(-1), "negative protobuf timestamp"),
            )],
        },
        ProtobufPushCase {
            name: "push_protobuf_duplicate_structured_metadata_name",
            streams: vec![stream(
                r#"{app="api"}"#,
                LokiProtoEntry {
                    structured_metadata: vec![pair("trace_id", "abc"), pair("trace_id", "def")],
                    ..entry(now, "duplicate protobuf metadata")
                },
            )],
        },
        ProtobufPushCase {
            name: "push_protobuf_invalid_structured_metadata_name",
            streams: vec![stream(
                r#"{app="api"}"#,
                LokiProtoEntry {
                    structured_metadata: vec![pair("9bad", "metadata")],
                    ..entry(now, "invalid protobuf metadata name")
                },
            )],
        },
        ProtobufPushCase {
            name: "push_protobuf_empty_structured_metadata_name",
            streams: vec![stream(
                r#"{app="api"}"#,
                LokiProtoEntry {
                    structured_metadata: vec![pair("", "metadata")],
                    ..entry(now, "empty protobuf metadata name")
                },
            )],
        },
        ProtobufPushCase {
            name: "push_protobuf_parsed_label",
            streams: vec![stream(
                r#"{app="parsed"}"#,
                LokiProtoEntry {
                    parsed: vec![pair("parsed_status", "200")],
                    ..entry(Some(parsed_at / 1_000_000_000), "protobuf parsed label")
                },
            )],
        },
    ];
    let mut cases = Vec::new();
    for request in requests {
        let body = snap::raw::Encoder::new().compress_vec(
            &LokiProtoPushRequest {
                streams: request.streams,
            }
            .encode_to_vec(),
        )?;
        cases.push(push_case(request.name).body("application/x-protobuf", body));
    }
    cases.push(
        Case::post("push_protobuf_unframed_bytes", "/loki/api/v1/push")
            .tenant(PUSH_TENANT)
            .target(Target::Distributor)
            .body("application/x-protobuf", vec![0xff, 0xff, 0xff]),
    );
    cases.push(
        Case::post("push_protobuf_truncated_message", "/loki/api/v1/push")
            .tenant(PUSH_TENANT)
            .target(Target::Distributor)
            .body("application/x-protobuf", vec![0x03, 0x08, 0xff, 0xff, 0xff]),
    );
    cases.push(
        Case::get("query_protobuf_parsed_label", "/loki/api/v1/query_range")
            .params([
                QueryParameter::new(
                    "query",
                    r#"{app="parsed"} | parsed_status = "200""#.to_string(),
                ),
                QueryParameter::new("start", parsed_at.to_string()),
                QueryParameter::new("end", (parsed_at + 1_000_000_000).to_string()),
            ])
            .tenant(PUSH_TENANT),
    );
    Ok(cases)
}

/// The ops endpoints that only read: build info, readiness, services, config,
/// metrics, the ruler inventory and the ring pages.
fn status_cases() -> Vec<Case> {
    let mut cases = vec![
        Case::get("status_buildinfo", "/loki/api/v1/status/buildinfo").shape(Shape::BuildInfo),
        Case::get("status_ready", "/ready").shape(Shape::StatusText),
        Case::get("status_log_level", "/log_level").shape(Shape::StatusText),
        Case::get("status_memberlist", "/memberlist").shape(Shape::StatusText),
        Case::get("status_services", "/services").shape(Shape::StatusText),
        Case::get("status_config", "/config").shape(Shape::Config),
        Case::get("status_config_diff", "/config")
            .raw_query("mode=diff")
            .shape(Shape::Config),
        Case::get("status_config_defaults", "/config")
            .raw_query("mode=defaults")
            .shape(Shape::Config),
        Case::get("status_metrics", "/metrics").shape(Shape::MetricFamilies),
    ];
    for case in [
        NamedEndpointCase {
            name: "ruler_rules",
            path: "/loki/api/v1/rules",
        },
        NamedEndpointCase {
            name: "ruler_rules_namespace",
            path: "/loki/api/v1/rules/default",
        },
        NamedEndpointCase {
            name: "ruler_rules_group",
            path: "/loki/api/v1/rules/default/api-errors",
        },
        NamedEndpointCase {
            name: "ruler_prometheus_rules",
            path: "/prometheus/api/v1/rules",
        },
        NamedEndpointCase {
            name: "ruler_prometheus_alerts",
            path: "/prometheus/api/v1/alerts",
        },
        NamedEndpointCase {
            name: "ruler_api_prom_rules",
            path: "/api/prom/rules",
        },
        NamedEndpointCase {
            name: "ruler_api_prom_alerts",
            path: "/api/prom/alerts",
        },
        NamedEndpointCase {
            name: "ruler_api_prom_rules_namespace",
            path: "/api/prom/rules/default",
        },
        NamedEndpointCase {
            name: "ruler_api_prom_rules_group",
            path: "/api/prom/rules/default/api-errors",
        },
    ] {
        cases.push(
            Case::get(case.name, case.path)
                .tenant(VACANT_TENANT)
                .shape(Shape::Typed),
        );
    }
    for case in [
        NamedTargetCase {
            name: "ring_querier",
            path: "/ring",
            target: Target::Querier,
        },
        NamedTargetCase {
            name: "ring_distributor",
            path: "/distributor/ring",
            target: Target::Distributor,
        },
        NamedTargetCase {
            name: "ring_compactor",
            path: "/compactor/ring",
            target: Target::BlockBuilder,
        },
        NamedTargetCase {
            name: "ring_scheduler",
            path: "/scheduler/ring",
            target: Target::Querier,
        },
        NamedTargetCase {
            name: "ring_ruler",
            path: "/ruler/ring",
            target: Target::Querier,
        },
    ] {
        cases.push(
            Case::get(case.name, case.path)
                .target(case.target)
                .shape(Shape::RingPage),
        );
    }
    cases
}

/// `POST /log_level`, which changes Loki's log level.
fn log_level_cases() -> Vec<Case> {
    [
        LogLevelCase {
            name: "log_level_post_query",
            query: Some("log_level=debug"),
            form: None,
        },
        LogLevelCase {
            name: "log_level_post_form",
            query: None,
            form: Some("log_level=warn"),
        },
        LogLevelCase {
            name: "log_level_post_query_and_form",
            query: Some("log_level=debug"),
            form: Some("log_level=warn"),
        },
        LogLevelCase {
            name: "log_level_post_unknown_level",
            query: Some("log_level=trace"),
            form: None,
        },
        LogLevelCase {
            name: "log_level_post_empty_level",
            query: Some("log_level="),
            form: None,
        },
        LogLevelCase {
            name: "log_level_post_empty_form",
            query: None,
            form: Some(""),
        },
    ]
    .into_iter()
    .map(|input| {
        let mut request = Case::post(input.name, "/log_level")
            .raw_query(input.query.unwrap_or_default())
            .shape(Shape::StatusText);
        if let Some(form) = input.form {
            request = request.form(form);
        }
        request
    })
    .collect()
}

/// The compactor's delete API: create a request, list it, cancel it, list
/// again.
fn delete_lifecycle_cases() -> Vec<Case> {
    let delete = |name, method| {
        Case::new(name, method, "/loki/api/v1/delete")
            .tenant(DELETE_TENANT)
            .target(Target::BlockBuilder)
            .shape(Shape::Delete)
    };
    vec![
        delete("delete_create", Method::POST).params([
            QueryParameter::new("query", r#"{app="api"} |= "secret""#),
            QueryParameter::new("start", "1591616227"),
            QueryParameter::new("end", "1591619692"),
        ]),
        delete("delete_list_after_create", Method::GET),
        delete("delete_cancel", Method::DELETE).raw_query(&format!("request_id={REQUEST_ID}")),
        delete("delete_list_after_cancel", Method::GET),
    ]
}

/// The ingester lifecycle controls, which run last: the final one shuts
/// Loki's ingester down.
fn ingester_control_cases() -> Vec<Case> {
    [
        LifecycleCase {
            name: "ingester_flush",
            method: Method::POST,
            path: "/flush",
            query: "",
        },
        LifecycleCase {
            name: "ingester_prepare_shutdown_unset",
            method: Method::GET,
            path: "/ingester/prepare_shutdown",
            query: "",
        },
        LifecycleCase {
            name: "ingester_prepare_shutdown_set",
            method: Method::POST,
            path: "/ingester/prepare_shutdown",
            query: "",
        },
        LifecycleCase {
            name: "ingester_prepare_shutdown_after_set",
            method: Method::GET,
            path: "/ingester/prepare_shutdown",
            query: "",
        },
        LifecycleCase {
            name: "ingester_prepare_shutdown_clear",
            method: Method::DELETE,
            path: "/ingester/prepare_shutdown",
            query: "",
        },
        LifecycleCase {
            name: "ingester_prepare_shutdown_after_clear",
            method: Method::GET,
            path: "/ingester/prepare_shutdown",
            query: "",
        },
        LifecycleCase {
            name: "ingester_shutdown",
            method: Method::GET,
            path: "/ingester/shutdown",
            query: "flush=false&delete_ring_tokens=false&terminate=false",
        },
    ]
    .into_iter()
    .map(|case| {
        Case::new(case.name, case.method, case.path)
            .raw_query(case.query)
            .target(Target::Distributor)
            .shape(Shape::Typed)
    })
    .collect()
}

fn range_params<'a>(
    timeline: &Timeline,
    logql: &'a str,
    step_secs: i64,
) -> Vec<QueryParameter<'a>> {
    vec![
        QueryParameter::new("query", logql),
        QueryParameter::new("start", timeline.at(0).to_string()),
        QueryParameter::new("end", timeline.at(60).to_string()),
        QueryParameter::new("step", step_secs.to_string()),
        QueryParameter::new("direction", "forward"),
    ]
}

/// A window around the whole seeded corpus.
fn window(timeline: &Timeline) -> [QueryParameter<'static>; 2] {
    [
        QueryParameter::new("start", timeline.at(-60).to_string()),
        QueryParameter::new("end", timeline.at(120).to_string()),
    ]
}

// ---------------------------------------------------------------------------
// The corpus clock.
// ---------------------------------------------------------------------------

/// The corpus's base time, in nanoseconds since the epoch.
///
/// Loki refuses entries far outside its ingestion window and prunes by wall
/// clock, so the corpus cannot use the small fixed timestamps the rest of this
/// crate's suites do. It is anchored a few minutes in the past and rounded down
/// to a whole minute, so a step-aligned window is aligned on both sides.
struct Timeline {
    base_ns: i64,
}

impl Timeline {
    fn new() -> TestResult<Self> {
        let now_secs = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        )?;
        let base_secs = (now_secs - 300) - (now_secs - 300) % 60;
        Ok(Self {
            base_ns: base_secs * 1_000_000_000,
        })
    }

    /// The epoch nanosecond `offset_secs` after the base time.
    fn at(&self, offset_secs: i64) -> i64 {
        self.base_ns + offset_secs * 1_000_000_000
    }
}

// ---------------------------------------------------------------------------
// Seeding.
// ---------------------------------------------------------------------------

fn json_streams(timeline: &Timeline, streams: &[SeedStream]) -> Value {
    json!({
        "streams": streams
            .iter()
            .map(|stream| json!({
                "stream": stream
                    .labels
                    .iter()
                    .map(|pair| (pair.name.to_string(), json!(pair.value)))
                    .collect::<serde_json::Map<_, _>>(),
                "values": stream
                    .entries
                    .iter()
                    .map(|entry| {
                        let timestamp = json!(timeline.at(entry.offset_secs).to_string());
                        if entry.metadata.is_empty() {
                            json!([timestamp, entry.line])
                        } else {
                            json!([
                                timestamp,
                                entry.line,
                                entry
                                    .metadata
                                    .iter()
                                    .map(|pair| (pair.name.to_string(), json!(pair.value)))
                                    .collect::<serde_json::Map<_, _>>(),
                            ])
                        }
                    })
                    .collect::<Vec<_>>(),
            }))
            .collect::<Vec<_>>(),
    })
}

/// The snappy-compressed protobuf push body, in Loki's `logproto` shape.
fn proto_push_body(timeline: &Timeline) -> TestResult<Vec<u8>> {
    let request = LokiProtoPushRequest {
        streams: PROTO_STREAMS
            .iter()
            .map(|stream| LokiProtoStream {
                labels: proto_label_set(stream.labels),
                entries: stream
                    .entries
                    .iter()
                    .map(|entry| {
                        let timestamp_ns = timeline.at(entry.offset_secs);
                        LokiProtoEntry {
                            timestamp: Some(LokiProtoTimestamp {
                                seconds: timestamp_ns / 1_000_000_000,
                                nanos: i32::try_from(timestamp_ns % 1_000_000_000)
                                    .unwrap_or_default(),
                            }),
                            line: entry.line.to_string(),
                            structured_metadata: entry
                                .metadata
                                .iter()
                                .map(|pair| LokiProtoLabelPair {
                                    name: pair.name.to_string(),
                                    value: pair.value.to_string(),
                                })
                                .collect(),
                            parsed: Vec::new(),
                        }
                    })
                    .collect(),
                hash: 0,
            })
            .collect(),
    };
    Ok(snap::raw::Encoder::new().compress_vec(&request.encode_to_vec())?)
}

/// `{name="value", ...}`, the way logproto carries a stream's labels.
fn proto_label_set(labels: &[StaticLabelPair]) -> String {
    let pairs = labels
        .iter()
        .map(|pair| format!("{}=\"{}\"", pair.name, pair.value))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{pairs}}}")
}

async fn push_json(client: &reqwest::Client, base: &str, tenant: &str, body: &Value) -> TestResult {
    accept_push(
        client
            .post(format!("{base}/loki/api/v1/push"))
            .header("Content-Type", "application/json")
            .header("X-Scope-OrgID", tenant)
            .body(body.to_string())
            .send()
            .await?,
        base,
    )
    .await
}

async fn push_gzip_json(client: &reqwest::Client, base: &str, body: &Value) -> TestResult {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body.to_string().as_bytes())?;
    accept_push(
        client
            .post(format!("{base}/loki/api/v1/push"))
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .header("X-Scope-OrgID", TENANT)
            .body(encoder.finish()?)
            .send()
            .await?,
        base,
    )
    .await
}

async fn push_proto(client: &reqwest::Client, base: &str, body: &[u8]) -> TestResult {
    accept_push(
        client
            .post(format!("{base}/loki/api/v1/push"))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .header("X-Scope-OrgID", TENANT)
            .body(body.to_vec())
            .send()
            .await?,
        base,
    )
    .await
}

async fn accept_push(response: reqwest::Response, base: &str) -> TestResult {
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let body = response.text().await.unwrap_or_default();
    Err(format!("push to {base} returned {status}: {body}").into())
}

// ---------------------------------------------------------------------------
// Probing.
// ---------------------------------------------------------------------------

/// One side of the comparison: where each of its roles listens, and the delete
/// request id it last listed.
struct Side {
    querier: String,
    distributor: String,
    block_builder: String,
    request_id: Option<String>,
    last_response: Option<Value>,
}

impl Side {
    fn new(querier: &str, distributor: &str, block_builder: &str) -> Self {
        Self {
            querier: querier.to_string(),
            distributor: distributor.to_string(),
            block_builder: block_builder.to_string(),
            request_id: None,
            last_response: None,
        }
    }

    fn base(&self, target: Target) -> &str {
        match target {
            Target::Querier => &self.querier,
            Target::Distributor => &self.distributor,
            Target::BlockBuilder => &self.block_builder,
        }
    }

    /// Sends `case` to this side and returns the answer, reduced by its shape.
    async fn probe(&mut self, client: &reqwest::Client, case: &Case) -> TestResult<Value> {
        let query = self.request_id.as_deref().map_or_else(
            || case.query.clone(),
            |id| case.query.replace(REQUEST_ID, id),
        );
        let suffix = if query.is_empty() {
            String::new()
        } else {
            format!("?{query}")
        };
        let url = format!("{}{}{suffix}", self.base(case.target), case.path);
        if matches!(
            case.shape,
            Shape::Handshake | Shape::TailFrame | Shape::LiveTailFrame
        ) {
            let push = (case.shape == Shape::LiveTailFrame).then(|| LiveTailPush {
                client,
                url: format!("{}/loki/api/v1/push", self.distributor),
            });
            return probe_websocket(&url, case, push).await;
        }
        let mut request = client
            .request(case.method.clone(), url)
            .header("X-Scope-OrgID", case.tenant);
        for header in &case.headers {
            request = request.header(header.name, &header.value);
        }
        if !case.body.is_empty() {
            request = request.body(case.body.clone());
        }
        let response = request.send().await?;
        let status = response.status().as_u16();
        let content_type = media_type(response.headers().get(reqwest::header::CONTENT_TYPE));
        let text = response.text().await?;
        self.last_response = Some(json!({"status": status, "body": json_or_text(&text)}));
        if case.shape == Shape::Delete
            && let Some(request_id) = raw_delete_request_id(&text)
        {
            self.request_id = Some(request_id);
        }
        Ok(reduce(case.shape, status, &content_type, &text))
    }
}

/// A content type without its parameters.
fn media_type(value: Option<&reqwest::header::HeaderValue>) -> String {
    value
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Opens a websocket for `case` and reduces what it answers.
///
/// With `push`, the case's body goes to that push URL once the socket is
/// open, so the frame read afterwards is the live one.
async fn probe_websocket(
    url: &str,
    case: &Case,
    push: Option<LiveTailPush<'_>>,
) -> TestResult<Value> {
    let mut request = url.replacen("http://", "ws://", 1).into_client_request()?;
    request
        .headers_mut()
        .insert("X-Scope-OrgID", case.tenant.parse()?);
    for header in &case.headers {
        request
            .headers_mut()
            .insert(header.name, header.value.parse()?);
    }
    match connect_async(request).await {
        Ok(connection) => {
            let mut socket = connection.0;
            let response = connection.1;
            let status = response.status().as_u16();
            if case.shape == Shape::Handshake {
                return Ok(json!({"status": status, "contentType": "", "body": ""}));
            }
            if let Some(push) = push {
                tokio::time::sleep(LIVE_TAIL_SETTLE).await;
                accept_push(
                    push.client
                        .post(&push.url)
                        .header("Content-Type", "application/json")
                        .header("X-Scope-OrgID", case.tenant)
                        .body(case.body.clone())
                        .send()
                        .await?,
                    &push.url,
                )
                .await?;
            }
            let frame =
                tokio::time::timeout(TAIL_FRAME_TIMEOUT, next_text_frame(&mut socket)).await;
            let _ = socket.close(None).await;
            let text = match frame {
                Ok(Some(Ok(text))) => text,
                Ok(Some(Err(error))) => return Ok(json!({"transportError": error.to_string()})),
                Ok(None) => return Ok(json!({"status": status, "frame": "closed"})),
                Err(_) => return Ok(json!({"status": status, "frame": "none"})),
            };
            Ok(reduce_tail_frame(status, &text))
        }
        Err(WebSocketError::Http(response)) => {
            let body = response
                .body()
                .as_ref()
                .map_or_else(Default::default, |body| {
                    String::from_utf8_lossy(body).to_string()
                });
            Ok(json!({
                "status": response.status().as_u16(),
                "contentType": media_type(response.headers().get("content-type")),
                "body": body,
            }))
        }
        Err(error) => Ok(json!({"transportError": error.to_string()})),
    }
}

/// The next text frame with something in it.
///
/// Loki pings an open tail and can send an empty text frame while it waits.
/// Neither is an answer, so both are skipped.
async fn next_text_frame<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
) -> Option<Result<String, WebSocketError>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    while let Some(message) = socket.next().await {
        match message {
            Ok(Message::Text(text)) if !text.trim().is_empty() => {
                return Some(Ok(text.to_string()));
            }
            Ok(Message::Close(_)) => return None,
            Ok(_) => {}
            Err(error) => return Some(Err(error)),
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Reducing an answer to what is compared.
// ---------------------------------------------------------------------------

fn reduce(shape: Shape, status: u16, content_type: &str, text: &str) -> Value {
    match shape {
        Shape::Api => success_or_error(status, text, normalize),
        Shape::ApiAtNow => success_or_error(status, text, normalize_at_now),
        Shape::BuildInfo => success_or_error(status, text, stable_buildinfo),
        Shape::StatusText => json!({"status": status, "body": stable_status_text(text)}),
        Shape::Config => stable_config(status, text),
        Shape::MetricFamilies => stable_metric_families(status, text),
        Shape::Typed => typed(status, content_type, &json_or_text(text)),
        Shape::Delete => typed(
            status,
            content_type,
            &serde_json::from_str::<Value>(text)
                .map_or_else(|_| json!(text), |body| stable_delete(&body)),
        ),
        Shape::RingPage => stable_ring_page(status, content_type, text),
        Shape::IndexStats => success_or_error(status, text, stable_index_stats),
        Shape::IndexVolume => success_or_error(status, text, stable_index_volume),
        Shape::DetectedFields => success_or_error(status, text, stable_detected_fields),
        Shape::DetectedLabels => success_or_error(status, text, stable_detected_labels),
        Shape::Patterns => success_or_error(status, text, stable_patterns),
        Shape::Handshake | Shape::TailFrame | Shape::LiveTailFrame => {
            json!({"status": status, "body": text})
        }
    }
}

/// A 200 JSON answer reduced by `success`; any other answer by its error text.
fn success_or_error(status: u16, text: &str, success: fn(&Value) -> Value) -> Value {
    if status == 200
        && let Ok(body) = serde_json::from_str::<Value>(text)
    {
        return json!({"status": status, "body": success(&body)});
    }
    json!({"status": status, "body": stable_error_text(text)})
}

fn typed(status: u16, content_type: &str, body: &Value) -> Value {
    json!({"status": status, "contentType": content_type, "body": body})
}

fn json_or_text(text: &str) -> Value {
    serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!(text))
}

/// Removes the fields that are legitimately volatile, and puts the rest in an
/// order both sides can be compared in.
///
/// The only field dropped is `stats`: every member of it is a byte count, a
/// duration or a chunk count measured against one side's own storage layout,
/// and none of them describes the query's answer. `encodingFlags` stays: a case
/// that asks for an encoding is asking about the echo too.
///
/// Ordering is not dropped, it is fixed. `serde_json` keeps a JSON object's
/// keys in the order they were written, and Loki writes a `/series` entry's
/// labels unordered: `{"env":"stage","service_name":"meta","app":"meta"}` for
/// a label set Krabka writes sorted. That is not a difference in the answer,
/// so object keys are sorted on both sides before anything is compared. The
/// arrays that carry an *unordered set* (`result`, the bare array `/labels` and
/// `/series` answer with, and the `values` of the `/api/prom` label aliases)
/// are sorted for the same reason. The arrays that carry an ordered sequence,
/// an entry's `[timestamp, line]` and a series' `values`, are left exactly as
/// each side wrote them.
fn normalize(response: &Value) -> Value {
    let mut response = sort_object_keys(response);
    if let Some(array) = response.as_array_mut() {
        array.sort_by_key(ToString::to_string);
        return response;
    }
    if let Some(Value::Array(values)) = response.get_mut("values") {
        values.sort_by_key(ToString::to_string);
    }
    let Some(data) = response.get_mut("data") else {
        return response;
    };
    if let Some(array) = data.as_array_mut() {
        // `/labels`, `/label/{name}/values` and `/series` answer with a bare
        // array. Loki returns series unordered.
        array.sort_by_key(ToString::to_string);
        return response;
    }
    let Some(object) = data.as_object_mut() else {
        return response;
    };
    object.remove("stats");
    let result_type = object
        .get("resultType")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if !matches!(result_type.as_str(), "streams" | "matrix" | "vector") {
        return response;
    }
    if let Some(Value::Array(result)) = object.get_mut("result") {
        for item in result.iter_mut() {
            round_samples(&result_type, item);
        }
        result.sort_by_key(ToString::to_string);
    }
    response
}

/// [`normalize`], with a sample timestamp near the wall clock replaced.
///
/// Only the timestamp is replaced, and only when it is a number of seconds
/// within a minute of now. A timestamp in any other unit or far from now still
/// reads as itself.
fn normalize_at_now(response: &Value) -> Value {
    let mut response = normalize(response);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64());
    if let Some(Value::Array(result)) = response.pointer_mut("/data/result") {
        for sample in result.iter_mut().filter_map(|item| item.get_mut("value")) {
            if let Some(timestamp) = sample.get_mut(0)
                && timestamp
                    .as_f64()
                    .is_some_and(|seconds| (seconds - now).abs() < 60.0)
            {
                *timestamp = json!("<now>");
            }
        }
    }
    response
}

/// A tail frame: the order its top-level keys were written in, and its body
/// put in a comparable order.
///
/// The key order is kept on purpose. A client that decodes the frame in one
/// pass reads `encodingFlags` only where the server wrote it.
fn reduce_tail_frame(status: u16, text: &str) -> Value {
    let Ok(frame) = serde_json::from_str::<Value>(text) else {
        return json!({"status": status, "frame": text});
    };
    let keys = frame.as_object().map_or_else(Default::default, |object| {
        object.keys().cloned().collect::<Vec<_>>()
    });
    let mut body = sort_object_keys(&frame);
    if let Some(streams) = body.get_mut("streams") {
        *streams = tail_entries(streams);
    }
    json!({"status": status, "keys": keys, "frame": body})
}

/// A tail frame's streams as one `{stream, entry}` pair per entry, sorted.
///
/// How a frame groups its entries into stream objects is batching, and Loki
/// is not consistent about it. Loki 3.7.7 writes one stream object per entry
/// when it replays history into a tail, even where several entries share a
/// label set. Each entry and the labels it carries are the answer.
fn tail_entries(streams: &Value) -> Value {
    let mut entries = streams
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|stream| {
            stream["values"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|entry| json!({"stream": stream["stream"].clone(), "entry": entry}))
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(ToString::to_string);
    json!(entries)
}

/// Rounds sample values to six decimals, so the two engines' float formatting
/// does not read as a difference in the answer.
fn round_samples(result_type: &str, item: &mut Value) {
    match result_type {
        "matrix" => {
            if let Some(Value::Array(values)) = item.get_mut("values") {
                for sample in values.iter_mut() {
                    round_sample(sample);
                }
            }
        }
        "vector" => {
            if let Some(sample) = item.get_mut("value") {
                round_sample(sample);
            }
        }
        _ => {}
    }
}

fn round_sample(sample: &mut Value) {
    if let Some(array) = sample.as_array_mut()
        && let Some(Value::String(value)) = array.get_mut(1)
    {
        *value = round_float_string(value);
    }
}

/// The same JSON with every object's keys in sorted order.
fn sort_object_keys(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut sorted = std::collections::BTreeMap::new();
            for key in object.keys() {
                if let Some(value) = object.get(key) {
                    sorted.insert(key.clone(), sort_object_keys(value));
                }
            }
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(values) => Value::Array(values.iter().map(sort_object_keys).collect()),
        other => other.clone(),
    }
}

fn round_float_string(value: &str) -> String {
    match value {
        "NaN" | "+Inf" | "-Inf" => value.to_string(),
        _ => value
            .parse::<f64>()
            .map_or_else(|_| value.to_string(), |number| format!("{number:.6}")),
    }
}

/// An error body, with the parts that depend on the wall clock replaced.
fn stable_error_text(body: &str) -> Value {
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        return sort_object_keys(&value);
    }
    let oldest_marker = "oldest acceptable timestamp is: ";
    if let Some(marker_start) = body.find(oldest_marker) {
        let stable_prefix = marker_start + oldest_marker.len();
        return json!(format!("{}<oldest>", &body[..stable_prefix]));
    }
    let query_length_marker = "the query time range exceeds the limit (query length: ";
    if let Some(marker_start) = body.find(query_length_marker)
        && let Some(limit_start) = body[marker_start..].find(", limit: ")
    {
        let limit_start = marker_start + limit_start;
        return json!(format!(
            "{}<query-length>{}",
            &body[..marker_start + query_length_marker.len()],
            &body[limit_start..]
        ));
    }
    json!(body)
}

/// Build info with every string value, all of them build facts, replaced.
fn stable_buildinfo(body: &Value) -> Value {
    let Some(fields) = body.as_object() else {
        return body.clone();
    };
    let mut stable_fields = std::collections::BTreeMap::new();
    for key in fields.keys() {
        if let Some(value) = fields.get(key) {
            let value = if value.is_string() {
                json!("<string>")
            } else {
                value.clone()
            };
            stable_fields.insert(key.clone(), value);
        }
    }
    Value::Object(stable_fields.into_iter().collect())
}

/// A plain-text ops page. `/services` lists one service per line in no fixed
/// order, so those lines are sorted.
fn stable_status_text(text: &str) -> Value {
    if let Ok(body) = serde_json::from_str::<Value>(text) {
        return body;
    }
    if !text.is_empty() && text.lines().all(|line| line.contains("=> Running")) {
        let mut lines = text.lines().map(str::to_owned).collect::<Vec<_>>();
        lines.sort();
        return json!(lines);
    }
    json!(text)
}

/// `/config` reduced to the `auth_enabled` line and the presence of `target`.
///
/// `target` is deployment topology, not wire compatibility. Loki here runs
/// single-binary (`target: all`) against Krabka roles, so the two can only
/// agree on the key's presence, never its value.
fn stable_config(status: u16, text: &str) -> Value {
    let mut lines = text
        .lines()
        .filter(|line| line.starts_with("target:") || line.starts_with("auth_enabled:"))
        .map(|line| {
            if line.starts_with("target:") {
                "target:".to_string()
            } else {
                line.trim().to_string()
            }
        })
        .collect::<Vec<_>>();
    lines.sort();
    let body = if lines.is_empty() { text } else { "" };
    json!({"status": status, "body": body, "stableLines": lines})
}

/// The metric families a Loki dashboard keys on, where they are present.
fn stable_metric_families(status: u16, text: &str) -> Value {
    let selected = ["loki_boltdb_shipper_compactor_running", "loki_build_info"];
    let mut families = text
        .lines()
        .filter_map(metric_family_name)
        .filter(|name| selected.contains(name))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    families.sort();
    families.dedup();
    json!({"status": status, "metricFamilies": families})
}

fn metric_family_name(line: &str) -> Option<&str> {
    if let Some(rest) = line
        .strip_prefix("# HELP ")
        .or_else(|| line.strip_prefix("# TYPE "))
    {
        return rest.split_whitespace().next();
    }
    if line.starts_with('#') || line.is_empty() {
        return None;
    }
    line.split_once('{')
        .map_or(line, |parts| parts.0)
        .split_whitespace()
        .next()
}

/// A delete request list with the ids and creation times each side mints
/// replaced.
fn stable_delete(body: &Value) -> Value {
    let Some(requests) = body.as_array() else {
        return body.clone();
    };
    let mut requests = requests
        .iter()
        .map(|request| {
            let mut request = sort_object_keys(request);
            if let Some(fields) = request.as_object_mut() {
                for replacement in [
                    KeyReplacement {
                        key: "request_id",
                        placeholder: "<request-id>",
                    },
                    KeyReplacement {
                        key: "created_at",
                        placeholder: "<created-at>",
                    },
                ] {
                    if let Some(value) = fields.get_mut(replacement.key) {
                        *value = json!(replacement.placeholder);
                    }
                }
            }
            request
        })
        .collect::<Vec<_>>();
    requests.sort_by_key(ToString::to_string);
    json!(requests)
}

fn raw_delete_request_id(body: &str) -> Option<String> {
    serde_json::from_str::<Value>(body)
        .ok()?
        .as_array()?
        .first()?
        .get("request_id")?
        .as_str()
        .map(str::to_owned)
}

/// A ring page: its headings and the state words it shows.
fn stable_ring_page(status: u16, content_type: &str, text: &str) -> Value {
    let tokens = [
        "ACTIVE",
        "Healthy",
        "JOINING",
        "LEAVING",
        "PENDING",
        "Running",
        "UNHEALTHY",
        "Unhealthy",
    ]
    .into_iter()
    .filter(|token| text.contains(token))
    .collect::<Vec<_>>();
    json!({
        "status": status,
        "contentType": content_type,
        "headings": html_headings(text),
        "tokens": tokens,
    })
}

fn html_headings(body: &str) -> Vec<String> {
    let mut headings = Vec::new();
    for tag in ["h1", "h2"] {
        let mut rest = body;
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        while let Some(open_parts) = rest.split_once(&open) {
            let after_open = open_parts.1;
            let Some(close_parts) = after_open.split_once(&close) else {
                break;
            };
            let heading = close_parts.0;
            let after_close = close_parts.1;
            headings.push(heading.trim().to_owned());
            rest = after_close;
        }
    }
    headings.sort();
    headings
}

/// `/index/stats`: every counter is measured on one side's storage.
fn stable_index_stats(body: &Value) -> Value {
    let counter = |value: &Value| {
        if value.is_u64() {
            json!("<number>")
        } else {
            value.clone()
        }
    };
    json!({
        "streams": counter(&body["streams"]),
        "chunks": counter(&body["chunks"]),
        "entries": counter(&body["entries"]),
        "bytes": counter(&body["bytes"]),
    })
}

/// `/index/volume` and `/index/volume_range`: the series and their labels.
///
/// A byte count is measured on each side's storage, so it is replaced. An
/// instant sample keeps its timestamp, which is the request's `end`. A range
/// series keeps only the fact that it has samples: Loki writes one sample for
/// each step a chunk spans, so how many there are follows its chunk layout.
fn stable_index_volume(body: &Value) -> Value {
    let mut result = body["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for series in &mut result {
        if let Some(Value::Array(sample)) = series.get_mut("value")
            && let Some(bytes) = sample.get_mut(1)
        {
            *bytes = json!("<bytes>");
        }
        if let Some(values) = series.get_mut("values") {
            let sampled = values.as_array().is_some_and(|values| !values.is_empty());
            *values = json!(if sampled { "<samples>" } else { "<none>" });
        }
    }
    result.sort_by_key(|series| series["metric"].to_string());
    json!({
        "status": body["status"].clone(),
        "data": {
            "resultType": body["data"]["resultType"].clone(),
            "result": sort_object_keys(&json!(result)),
            "stats": body["data"].get("stats").is_some(),
        },
    })
}

fn stable_detected_fields(body: &Value) -> Value {
    if let Some(fields) = body["fields"].as_array() {
        let mut fields = fields.clone();
        for field in &mut fields {
            if let Some(Value::Array(parsers)) = field.get_mut("parsers") {
                parsers.sort_by_key(ToString::to_string);
            }
        }
        fields.sort_by_key(|field| field["label"].as_str().unwrap_or_default().to_string());
        return sort_object_keys(&json!({"fields": fields, "limit": body["limit"].clone()}));
    }
    let mut values = body["values"].as_array().cloned().unwrap_or_default();
    values.sort_by_key(ToString::to_string);
    json!({"values": values, "limit": body["limit"].clone()})
}

fn stable_detected_labels(body: &Value) -> Value {
    let mut labels = body["detectedLabels"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    labels.sort_by_key(|label| label["label"].as_str().unwrap_or_default().to_string());
    sort_object_keys(&json!(labels))
}

/// `/patterns`: sample timestamps are bucketed on each side's clock.
fn stable_patterns(body: &Value) -> Value {
    let mut patterns = body["data"].as_array().cloned().unwrap_or_default();
    for pattern in &mut patterns {
        if let Some(Value::Array(samples)) = pattern.get_mut("samples") {
            for sample in samples.iter_mut() {
                if let Some(sample) = sample.as_array_mut()
                    && sample.len() == 2
                {
                    sample[0] = json!("<timestamp>");
                }
            }
            samples.sort_by_key(ToString::to_string);
        }
    }
    patterns.sort_by_key(|pattern| pattern["pattern"].as_str().unwrap_or_default().to_string());
    json!({"status": body["status"].clone(), "data": sort_object_keys(&json!(patterns))})
}

// ---------------------------------------------------------------------------
// Reporting.
// ---------------------------------------------------------------------------

fn known_divergence(case: &'static str) -> Option<&'static Divergence> {
    LOKI_KNOWN_DIVERGENCE
        .iter()
        .find(|divergence| divergence.case == case)
}

fn report(case: &Case, krabka: &Value, loki: &Value) -> String {
    let mut headers = String::new();
    for header in &case.headers {
        let _ = write!(headers, "\n  {}: {}", header.name, header.value);
    }
    let body = if case.body.is_empty() {
        String::new()
    } else {
        let text = String::from_utf8_lossy(&case.body);
        format!("\n  body: {}", text.chars().take(300).collect::<String>())
    };
    format!(
        "case `{}` differed\n  request: {} {}?{} (tenant {}, {:?}){headers}{body}\n  krabka: {}\n  \
         loki:   {}",
        case.name,
        case.method,
        case.path,
        case.query,
        case.tenant,
        case.target,
        pretty(krabka),
        pretty(loki),
    )
}

fn summary(differences: &[String], healed: &[&Divergence]) -> String {
    let mut summary = String::new();
    if !differences.is_empty() {
        let _ = write!(
            summary,
            "{} case(s) disagreed with Loki:\n\n{}\n\n",
            differences.len(),
            differences.join("\n\n")
        );
    }
    for divergence in healed {
        let _ = writeln!(
            summary,
            "case `{}` is listed in LOKI_KNOWN_DIVERGENCE but now agrees with Loki. \
             Remove the entry. Its recorded reason was: {}",
            divergence.case, divergence.reason
        );
    }
    summary
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

// ---------------------------------------------------------------------------
// The two servers.
// ---------------------------------------------------------------------------

async fn start_loki() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    start_loki_with_config(LOKI_CONFIG, "overrides: {}\n").await
}

async fn start_loki_with_config(
    config: &str,
    overrides: &str,
) -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagreed
    // testcontainers pulled the image over the network and the suite compared
    // against whatever it got rather than against the pinned bytes.
    let tag = std::env::var("KRABKA_LOKI_IMAGE_TAG").expect(
        "KRABKA_LOKI_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under \
         cargo, set it to that image's tag in //bazel/images/images.bzl.",
    );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/loki".to_string(), tag)
            .with_exposed_port(LOKI_PORT.tcp())
            // No log-line wait condition. What a healthy single-binary Loki
            // logs at start-up is not a contract, and `wait_for_ready` polls
            // /ready instead, which is the answer the question actually wants.
            // Copied into the image rather than bind-mounted, so the test has
            // no host-filesystem prerequisites. The path is the one the image's
            // own default command already points at.
            .with_copy_to("/etc/loki/local-config.yaml", config.as_bytes().to_vec())
            .with_copy_to(
                "/etc/loki/experimental-overrides.yaml",
                overrides.as_bytes().to_vec(),
            )
            .start(),
    )
    .await??)
}

/// Installs a reloadable log filter, as the `krabka-observability` binary
/// does at start-up.
///
/// Without one, `POST /log_level` answers 501: the process has no handle to
/// move the level with. That is a property of this test process, not of the
/// endpoint Loki's clients reach.
fn install_log_level_control() {
    let logging = json_logging_layer("info", std::io::sink);
    Registry::default().with(logging.layer).init();
    LogLevelControl::install_process(logging.control);
}

async fn mapped_base_url(
    container: &testcontainers::ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port.tcp()).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

/// The Krabka side: a distributor and a querier over one shared WAL, and a
/// block builder for the delete API and its ring page.
///
/// Separate listeners rather than one merged router, because the roles are
/// separate routers: each registers the role's ops endpoints, so merging them
/// is an overlapping-route panic. A real deployment runs them as separate
/// roles too.
struct KrabkaStack {
    push_url: String,
    query_url: String,
    block_builder_url: String,
    shutdown: Vec<oneshot::Sender<()>>,
}

impl KrabkaStack {
    fn shutdown(self) {
        for tx in self.shutdown {
            let _ = tx.send(());
        }
    }
}

async fn start_krabka() -> TestResult<KrabkaStack> {
    start_krabka_with_overrides(OverridesProvider::new(Limits::default())).await
}

async fn start_krabka_with_overrides(overrides: OverridesProvider) -> TestResult<KrabkaStack> {
    let sink = InMemoryWalSink::default();
    let root = tempfile::tempdir()?.keep();
    // `i64::MIN`: nothing has been compacted, so every record the distributor
    // wrote is in the querier's hot tail. This is the same wiring
    // `build_service_router` uses for a querier with no compaction frontier.
    let state = QuerierState::new(root, LabelIndex::default(), BlockIndex::default())
        .with_hot_tail(sink.clone(), i64::MIN)
        .with_limits_overrides(overrides.clone());
    // Loki's own default limits, as a distributor with no overrides file runs
    // them. `distributor_router` alone enforces none, and Loki does.
    let distributor = distributor_router_with_overrides(sink, overrides);
    let block_builder = build_service_router(
        &block_builder_config(tempfile::tempdir()?.keep()),
        ServiceDependencies::default(),
        None,
    )
    .await?;
    let push = serve(distributor).await?;
    let query = serve(loki_router(state)).await?;
    let block_builder = serve(block_builder).await?;
    Ok(KrabkaStack {
        push_url: push.url,
        query_url: query.url,
        block_builder_url: block_builder.url,
        shutdown: vec![push.shutdown, query.shutdown, block_builder.shutdown],
    })
}

fn block_builder_config(data_root: std::path::PathBuf) -> ServiceConfig {
    ServiceConfig {
        target: Role::BlockBuilder,
        listen_addr: "127.0.0.1:0".parse().expect("a socket address"),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "loki-differential-block-builder".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        ..ServiceConfig::default()
    }
}

async fn serve(router: axum::Router) -> TestResult<ServedRouter> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let channel = oneshot::channel();
    let rx = channel.1;
    let tx = channel.0;
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    Ok(ServedRouter {
        url: format!("http://{addr}"),
        shutdown: tx,
    })
}

async fn wait_for_ready(client: &reqwest::Client, base: &str) -> TestResult {
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        if client
            .get(format!("{base}/ready"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("{base}/ready never answered 200").into())
}

/// Waits until `tenant` on `base` reports `expected` values for `label`.
///
/// Loki acknowledges a push before the entry is visible to a query, and a
/// differential that queries too early compares a full corpus against a partial
/// one and blames the engine.
async fn wait_for_seeded(
    client: &reqwest::Client,
    base: &str,
    timeline: &Timeline,
    tenant: &str,
    label: &str,
    expected: usize,
) -> TestResult {
    let url = format!(
        "{base}/loki/api/v1/label/{label}/values?{}",
        url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(
                window(timeline)
                    .iter()
                    .map(|parameter| (parameter.name, parameter.value.as_ref()))
            )
            .finish()
    );
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        let seeded = client
            .get(&url)
            .header("X-Scope-OrgID", tenant)
            .send()
            .await?
            .json::<Value>()
            .await
            .is_ok_and(|body| body["data"].as_array().map_or(0, Vec::len) >= expected);
        if seeded {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("{base} never returned all {expected} `{label}` values for {tenant}").into())
}

fn template_expected_response(timeline: &Timeline, name: &str, line: &str) -> Value {
    let stream = PARSER_STREAMS
        .iter()
        .find(|stream| {
            stream.labels.contains(&StaticLabelPair {
                name: "format",
                value: "logfmt",
            })
        })
        .expect("independent logfmt fixture exists");
    // The pinned distributor's word detector sees "error" only in the seed
    // lines at offsets 8 and 12. Detection happens before line_format, so its
    // structured metadata separates the returned streams even for a constant.
    let results: Vec<_> = ["error", "unknown"]
        .into_iter()
        .map(|level| {
            let values: Vec<_> = stream.entries.iter()
                .filter(|entry| matches!(entry.offset_secs, 8 | 12) == (level == "error"))
                .map(|entry| json!([
                    timeline.at(entry.offset_secs).to_string(),
                    if name == "line" { entry.line } else { line }
                ]))
                .collect();
            json!({
                "stream": {"app": "api", "env": "prod", "format": "logfmt", "service_name": "api", "detected_level": level},
                "values": values
            })
        }).collect();
    json!({"status": 200, "body": {"status": "success", "data": {
        "resultType": "streams", "result": results
    }}})
}

#[test]
fn template_expected_stream_retains_every_fixture_identity() {
    let timeline = Timeline {
        base_ns: 1_000_000_000,
    };
    let expected = template_expected_response(&timeline, "lower", "known");
    let expected_groups = [
        ExpectedTemplateLevel { level: "error", offsets: &[8, 12] },
        ExpectedTemplateLevel { level: "unknown", offsets: &[2, 3, 4, 7, 9, 10, 11, 13, 14, 15, 16, 20, 21, 22] },
    ].into_iter().map(|fixture| {
        let values: Vec<_> = fixture.offsets.iter()
            .map(|offset| json!([timeline.at(*offset).to_string(), "known"]))
            .collect();
        json!({"stream": {"app": "api", "env": "prod", "format": "logfmt", "service_name": "api", "detected_level": fixture.level}, "values": values})
    }).collect::<Vec<_>>();
    assert!(
        expected
            == json!({"status": 200, "body": {"status": "success", "data": {
                "resultType": "streams", "result": expected_groups
            }}})
    );
}
