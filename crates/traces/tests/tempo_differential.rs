//! Docker-backed differential probes against real Grafana Tempo.
//!
//! These tests are ignored by default because they pull and run upstream Docker
//! images. Run explicitly with:
//!
//! `cargo test -p krabka-traces --test tempo_differential -- --ignored --nocapture`

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use assert2::check;
use axum::{body::Body, http::StatusCode};
use base64::Engine as _;
use generated_differential::{CompareOp, TypedConstructor, TypedExpr};
use krabka_traceql::{AttrValue as TraceqlAttrValue, EngineOpts, TraceqlEngine};
use krabka_traces::{
    AttrValue,
    distributor::{self, DistributorState},
    metricsgen::{
        EdgeStore, MetricsGenConfig, RecordOutcome, Series, SeriesSample,
        SpanKind as MetricsSpanKind, StatusCode as MetricsStatusCode,
    },
};
use opentelemetry_proto::tonic::{
    common::v1::{
        AnyValue, ArrayValue, InstrumentationScope, KeyValue as OtlpKeyValue, KeyValueList,
        any_value::Value,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan, Status as OtlpStatus, TracesData},
};
use prost::Message as _;
use reqwest::StatusCode as ReqwestStatusCode;
use serde_json::{Value as JsonValue, json};
use testcontainers::{
    CopyDataSource, CopyTargetOptions, GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

mod container_url;
mod ingest_capture;
mod metrics_span;
mod search_json;
mod span_store;

use self::{
    container_url::mapped_base_url,
    ingest_capture::{CapturingSink, DoorPush, push_to_door, serve_until_shutdown, string_kv},
    metrics_span::MetricsSpan,
    search_json::search_contains_span_id_hex,
    span_store::{span_store_from_records, traceql_attr},
};

#[path = "../../metrics-service/tests/support/generated_differential.rs"]
mod generated_differential;
#[path = "../../metrics-service/tests/support/pinned_grafana_image.rs"]
mod pinned_grafana_image;
#[path = "../../observability/tests/support/rejection_responses.rs"]
mod rejection_responses;

use pinned_grafana_image::pinned_grafana_image;
use rejection_responses::{ImplementationAnswer, record_rejection_response};

const TENANT: &str = "tenant-a";
const TRACE_ID_HEX: &str = "01010101010101010101010101010101";
const CHILD_SPAN_ID_HEX: &str = "0303030303030303";
const ERROR_SPAN_ID_HEX: &str = "0404040404040404";
/// OTLP `STATUS_CODE_ERROR`.
const OTLP_STATUS_CODE_ERROR: i32 = 2;
const DOCKER_HOST_ALIAS: &str = "host.testcontainers.internal";
const GRAFANA_TEMPO_DATASOURCE_UID: &str = "krabka-traces";
/// Tempo query-frontend HTTP port inside the container. It matches
/// `http_listen_port` in [`TEMPO_CONFIG`].
const TEMPO_HTTP_PORT: u16 = 3200;
/// Tempo OTLP/HTTP receiver port inside the container. It matches the
/// `distributor.receivers.otlp` endpoint in [`TEMPO_CONFIG`].
const TEMPO_OTLP_PORT: u16 = 4318;
/// Grafana HTTP port inside the container.
const GRAFANA_HTTP_PORT: u16 = 3000;
const TEMPO_CONFIG: &str = r"
multitenancy_enabled: false
server:
  http_listen_port: 3200
distributor:
  receivers:
    otlp:
      protocols:
        http:
          endpoint: 0.0.0.0:4318
live_store:
  wal:
    path: /tmp/tempo/live-store
    ingestion_time_range_slack: 10m
storage:
  trace:
    backend: local
    block:
      version: vParquet5
    wal:
      path: /tmp/tempo/wal
    local:
      path: /tmp/tempo/blocks
";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryCaseKind {
    Selector,
    Structural,
    Pipeline,
}

#[derive(Clone, Copy, Debug)]
struct QueryCase {
    kind: QueryCaseKind,
    encoded_query: &'static str,
    expected_span_id: Option<&'static str>,
}

fn differential_search_corpus() -> Vec<QueryCase> {
    vec![
        QueryCase {
            kind: QueryCaseKind::Selector,
            encoded_query: "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D",
            expected_span_id: None,
        },
        QueryCase {
            kind: QueryCaseKind::Structural,
            encoded_query: "%7B%20.http.method%20%3D%20%22GET%22%20%7D%20%3E%3E%20%7B%20.db.system%20%3D%20%22postgresql%22%20%7D",
            expected_span_id: Some(CHILD_SPAN_ID_HEX),
        },
        QueryCase {
            // `| count() > 0` is a spanset count FILTER, valid in Tempo's search
            // API across versions. (`| by(...)` is a metrics-only stage that
            // real Tempo's /api/search rejects with a parse error, even though
            // Krabka accepts it as a superset.)
            kind: QueryCaseKind::Pipeline,
            encoded_query: "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D%20%7C%20count()%20%3E%200",
            expected_span_id: None,
        },
    ]
}

#[test]
fn differential_search_corpus_covers_selector_structural_and_pipeline_queries() {
    let corpus = differential_search_corpus();

    let kinds: Vec<QueryCaseKind> = corpus.iter().map(|case| case.kind).collect();
    let has_child_span_expectation = corpus
        .iter()
        .any(|case| case.expected_span_id == Some(CHILD_SPAN_ID_HEX));
    check!(
        kinds
            == vec![
                QueryCaseKind::Selector,
                QueryCaseKind::Structural,
                QueryCaseKind::Pipeline,
            ]
    );
    check!(has_child_span_expectation);
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/tempo image"]
async fn real_tempo_and_krabka_match_basic_by_id_and_search() -> TestResult {
    let client = reqwest::Client::new();
    let (_tempo, tempo_query, tempo_otlp) = start_ready_tempo(&client).await?;

    let query_range = "start=0&end=1";
    let mut fixture = TracesData::decode(sample_otlp_body().as_slice())?;
    fixture
        .resource_spans
        .extend(forest_otlp_fixture().resource_spans);
    let otlp_body = fixture.encode_to_vec();
    let krabka = start_krabka_pair(&otlp_body).await?;

    post_otlp(
        &client,
        &format!("{tempo_otlp}/v1/traces"),
        None,
        &otlp_body,
    )
    .await?;

    let tempo_trace = get_trace_by_id_until_found(&client, &tempo_query, None, query_range).await?;
    let krabka_trace =
        get_trace_by_id(&client, &krabka.base_url, Some(TENANT), query_range).await?;
    assert_trace_shape_matches(&tempo_trace, &krabka_trace);

    for case in differential_search_corpus() {
        let tempo_search = get_json_until_non_empty_traces(
            &client,
            &format!(
                "{tempo_query}/api/search?q={}&{query_range}&limit=10&spss=10",
                case.encoded_query
            ),
            None,
        )
        .await?;
        let krabka_search = get_json(
            &client,
            &format!(
                "{}/api/search?q={}&{query_range}&limit=10&spss=10",
                krabka.base_url, case.encoded_query
            ),
            Some(TENANT),
        )
        .await?;
        assert_search_shape_matches(&tempo_search, &krabka_search);
        let mut expected_spans = match case.kind {
            QueryCaseKind::Structural => vec![CHILD_SPAN_ID_HEX.to_string()],
            QueryCaseKind::Selector | QueryCaseKind::Pipeline => vec![
                "0202020202020202".to_string(),
                CHILD_SPAN_ID_HEX.to_string(),
                ERROR_SPAN_ID_HEX.to_string(),
            ],
        };
        expected_spans.sort();
        assert2::assert!(
            search_identities(&tempo_search)?
                == BTreeMap::from([(TRACE_ID_HEX.to_string(), expected_spans)]),
            "unexpected upstream selection: {tempo_search}"
        );
        if let Some(span_id) = case.expected_span_id {
            assert_search_contains_span_id(&tempo_search, span_id);
            assert_search_contains_span_id(&krabka_search, span_id);
        }
    }

    // Independently assigned trees mirror the behavioral relationships in the
    // pinned Tempo vParquet5 tests, including unrelated and partial branches.
    let mut forest_evidence = Vec::new();
    for (query, expected_ids) in [
        (
            "{ name = \"ancestor1\" } >> { .kind = \"descendant\" }",
            vec![11_u8, 12],
        ),
        (
            "{ name = \"ancestor2\" } > { .kind = \"descendant\" }",
            vec![21, 22],
        ),
        (
            "{ name = \"ancestor2\" } >> { .kind = \"descendant\" }",
            vec![21, 22, 23],
        ),
        (
            "{ name = \"ancestor1\" } !>> { .kind = \"descendant\" }",
            vec![21, 22, 23],
        ),
        (
            "{ name = \"ancestor1\" } &>> { .kind = \"descendant\" }",
            vec![10, 11, 12],
        ),
        (
            "{ name = \"descendant2a\" } ~ { .kind = \"descendant\" }",
            vec![22],
        ),
        ("{ .foo = .bar }", vec![11]),
        (
            "{ resource.service.name = \"forest\" && .kind = \"missing\" }",
            vec![],
        ),
    ] {
        let encoded = url_encoded(query);
        let upstream_url =
            format!("{tempo_query}/api/search?q={encoded}&{query_range}&limit=100&spss=100");
        let upstream = if expected_ids.is_empty() {
            get_json(&client, &upstream_url, None).await?
        } else {
            get_json_until_non_empty_traces(&client, &upstream_url, None).await?
        };
        let candidate_url = format!(
            "{}/api/search?q={encoded}&{query_range}&limit=100&spss=100",
            krabka.base_url
        );
        let actual = get_json(&client, &candidate_url, Some(TENANT)).await?;
        let expected = if expected_ids.is_empty() {
            BTreeMap::new()
        } else {
            BTreeMap::from([(
                "09090909090909090909090909090909".to_owned(),
                expected_ids
                    .iter()
                    .map(|id| hex::encode([*id; 8]))
                    .collect::<Vec<_>>(),
            )])
        };
        forest_evidence.push(
            json!({"query":query, "expected":expected, "upstream":upstream, "krabka":actual, "status": if search_identities(&upstream)? == expected && search_identities(&actual)? == expected { "matched" } else { "mismatch" }}),
        );
        let evidence_dir = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(
            || std::path::PathBuf::from("../../target"),
            std::path::PathBuf::from,
        );
        std::fs::create_dir_all(&evidence_dir)?;
        std::fs::write(
            evidence_dir.join("tempo-forest-conformance.json"),
            serde_json::to_vec_pretty(&forest_evidence)?,
        )?;
        assert2::assert!(
            search_identities(&upstream)? == expected,
            "pinned Tempo selection differs for {query}: {upstream}"
        );
        assert2::assert!(
            search_identities(&actual)? == expected,
            "Krabka selection differs for {query}: {actual}"
        );
    }

    compare_generated_trace_queries(QueryTargets {
        client: &client,
        oracle: &tempo_query,
        candidate: &krabka.base_url,
        query_range,
    })
    .await?;

    let tempo_tags = get_json(
        &client,
        &format!("{tempo_query}/api/v2/search/tags?{query_range}"),
        None,
    )
    .await?;
    let krabka_tags = get_json(
        &client,
        &format!("{}/api/v2/search/tags?{query_range}", krabka.base_url),
        Some(TENANT),
    )
    .await?;
    assert_required_tag_names_match(&tempo_tags, &krabka_tags);

    let tempo_service_values = get_json(
        &client,
        &format!("{tempo_query}/api/v2/search/tag/resource.service.name/values?{query_range}"),
        None,
    )
    .await?;
    let krabka_service_values = get_json(
        &client,
        &format!(
            "{}/api/v2/search/tag/resource.service.name/values?{query_range}",
            krabka.base_url
        ),
        Some(TENANT),
    )
    .await?;
    assert_required_tag_values_match(&tempo_service_values, &krabka_service_values, "checkout");

    krabka.shutdown();
    Ok(())
}

async fn compare_generated_trace_queries(targets: QueryTargets<'_>) -> TestResult {
    let generated_output = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(
        || std::env::temp_dir().join(format!("krabka-trace-generated-{}", std::process::id())),
        std::path::PathBuf::from,
    );
    let expected_checkout = BTreeMap::from([(
        TRACE_ID_HEX.to_owned(),
        vec![
            "0202020202020202".to_owned(),
            CHILD_SPAN_ID_HEX.to_owned(),
            ERROR_SPAN_ID_HEX.to_owned(),
        ],
    )]);
    let checkout =
        TypedExpr::trace_string_compare("resource", "service.name", CompareOp::Eq, "checkout");
    generated_differential::run_typed(
        "traceql",
        std::slice::from_ref(&checkout),
        &[
            TypedConstructor::TraceAnd(TypedExpr::trace_duration_compare(
                "duration",
                CompareOp::Gt,
                0,
            )),
            TypedConstructor::TraceAnd(checkout.clone()),
            TypedConstructor::TraceAnd(TypedExpr::trace_duration_field_compare(
                "span:duration",
                CompareOp::Gt,
                "event:timeSinceStart",
            )),
            TypedConstructor::TraceAnd(TypedExpr::trace_duration_times_integer_compare(
                "duration",
                2,
                CompareOp::Gt,
                "duration",
            )),
            TypedConstructor::TraceAnd(TypedExpr::trace_field_compare(
                "",
                "span:name",
                CompareOp::Eq,
                "",
                "event:name",
            )),
            TypedConstructor::TraceOr(TypedExpr::trace_string_compare(
                "",
                "name",
                CompareOp::Eq,
                "missing",
            )),
        ],
        &generated_output,
        |expression| compare_generated_trace_selector(targets, expression, &expected_checkout),
    )
    .await?;

    let expected_field_match = BTreeMap::from([("09".repeat(16), vec!["0b".repeat(8)])]);
    generated_differential::run_typed(
        "traceql-field-comparisons",
        &[TypedExpr::trace_field_compare(
            "",
            "foo",
            CompareOp::Eq,
            "",
            "bar",
        )],
        &[
            TypedConstructor::TraceAnd(TypedExpr::trace_duration_compare(
                "duration",
                CompareOp::Gt,
                0,
            )),
            TypedConstructor::TraceOr(TypedExpr::trace_string_compare(
                "",
                "name",
                CompareOp::Eq,
                "missing",
            )),
            TypedConstructor::Paren,
        ],
        &generated_output,
        |expression| compare_generated_trace_selector(targets, expression, &expected_field_match),
    )
    .await?;

    let rejection_responses = Mutex::new(Vec::new());
    generated_differential::run(
        "traceql-rejections",
        &[r"resource.service.name =", "duration >", ".foo =~ 1"],
        &[
            "({expr})",
            "({expr}) && duration > 0ns",
            r#"({expr}) || name = "missing""#,
        ],
        &generated_output,
        |expression| {
            compare_generated_trace_rejection(
                targets,
                expression,
                RejectionEvidence {
                    output: &generated_output,
                    observations: &rejection_responses,
                },
            )
        },
    )
    .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/tempo image"]
async fn real_tempo_and_krabka_match_traceql_metrics_query_range() -> TestResult {
    let client = reqwest::Client::new();
    let (_tempo, tempo_query, tempo_otlp) = start_ready_tempo(&client).await?;

    let (trace_start_secs, query_range) = metrics_window()?;
    let otlp_body = reservoir_otlp_body_at(trace_start_secs * 1_000_000_000);
    let krabka = start_krabka_pair(&otlp_body).await?;

    post_otlp(
        &client,
        &format!("{tempo_otlp}/v1/traces"),
        None,
        &otlp_body,
    )
    .await?;

    let metrics_query = "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D%20%7C%20count_over_time()%20with(exemplars=false)";
    let tempo_metrics = get_json_until_positive_metric_total(
        &client,
        &format!("{tempo_query}/api/metrics/query_range?q={metrics_query}&{query_range}&step=30s"),
        None,
    )
    .await?;
    let krabka_metrics = get_json(
        &client,
        &format!(
            "{}/api/metrics/query_range?q={metrics_query}&{query_range}&step=30s",
            krabka.base_url
        ),
        Some(TENANT),
    )
    .await?;
    assert_metric_totals_match(&tempo_metrics, &krabka_metrics);
    let targets = QueryTargets {
        client: &client,
        oracle: &tempo_query,
        candidate: &krabka.base_url,
        query_range: &query_range,
    };
    let pipeline_result = compare_live_pipeline_hints(targets, trace_start_secs).await;
    let instant_result = compare_live_instant_bounds(targets, trace_start_secs).await;
    let numeric_result = compare_live_numeric_metrics(targets).await;
    let exemplar_result = compare_live_singleton_exemplar(targets, trace_start_secs).await;
    let arithmetic_result = compare_live_field_arithmetic(targets).await;
    let supported_result = compare_live_supported_scopes(targets).await;
    let output_result = compare_live_expression_outputs(targets, trace_start_secs).await;
    let retrieval_result = compare_live_retrieval_shapes(targets).await;
    let reservoir_result = compare_live_exemplar_reservoir(targets, trace_start_secs).await;
    pipeline_result?;
    instant_result?;
    numeric_result?;
    exemplar_result?;
    arithmetic_result?;
    supported_result?;
    reservoir_result?;
    output_result?;
    retrieval_result?;
    krabka.shutdown();
    Ok(())
}

/// The pinned Tempo (`oracle`) and Krabka (`candidate`) query APIs that a
/// live comparison sends the same request to, the client it sends with, and
/// the `start=...&end=...` window it asks over.
#[derive(Clone, Copy)]
struct QueryTargets<'a> {
    client: &'a reqwest::Client,
    oracle: &'a str,
    candidate: &'a str,
    query_range: &'a str,
}

/// Where a live comparison records each case's evidence and each failure.
struct ComparisonLedger<'a> {
    cases: &'a mut Vec<JsonValue>,
    failures: &'a mut Vec<String>,
}

/// Where a generated rejection case archives the responses it observed: the
/// output directory, and the ledger shared by every case of the run.
struct RejectionEvidence<'a> {
    output: &'a std::path::Path,
    observations: &'a Mutex<Vec<JsonValue>>,
}

async fn compare_live_instant_bounds(targets: QueryTargets<'_>, anchor: u64) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        ..
    } = targets;
    let end = anchor + 90;
    let rows = [
        ("default-since", format!("end={end}")),
        ("empty-start", format!("start=&end={end}")),
        ("explicit-since", format!("end={end}&since=2m")),
        ("ignored-time", format!("end={end}&time=bogus")),
        ("nanoseconds", format!("end={end}123456789")),
        ("fractional-seconds", format!("end={end}.123456789")),
        ("default-clock", "time=bogus".into()),
        ("start-only", format!("start={}", anchor - 60)),
        ("short-since", format!("end={end}&since=30s")),
        ("rate", format!("end={end}&since=2m")),
    ];
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for (id, params) in rows {
        let operation = if id == "rate" {
            "rate"
        } else {
            "count_over_time"
        };
        let query = format!(
            "{{ resource.service.name = \"checkout\" }} | {operation}() with(exemplars=false)"
        );
        let encoded = url_encoded(&query);
        // Three independently constructed checkout spans lie inside two-minute
        // windows. Tempo retains its default zero count for the empty short window.
        let value = if id == "rate" { 3.0 / 120.0 } else { 3.0 };
        let expected = if id == "short-since" {
            json!([{"labels":[{"key":"__name__","value":{"stringValue":operation}}]}])
        } else {
            json!([{
                "labels":[{"key":"__name__","value":{"stringValue":operation}}],
                "value":value,
            }])
        };
        let suffix = format!("/api/metrics/query?q={encoded}&{params}");
        let mut observations = Vec::new();
        for (base, tenant) in [(oracle, None), (candidate, Some(TENANT))] {
            let before = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            let response = get_json(client, &format!("{base}{suffix}"), tenant).await;
            let after = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            let body = json_or_error(&response);
            let result = response.and_then(|body| {
                let series = body.get("series").map_or(Ok(&[][..]), |value| {
                    value
                        .as_array()
                        .map(Vec::as_slice)
                        .ok_or("invalid instant series")
                })?;
                let expected_rows = expected.as_array().unwrap();
                if series.len() != expected_rows.len() {
                    return Err("instant series count differs".into());
                }
                for (actual, expected) in series.iter().zip(expected_rows) {
                    let fields = actual.as_object().ok_or("invalid instant series")?;
                    let actual_value = actual.get("value").map_or(Ok(0.0), |value| {
                        value.as_f64().ok_or("invalid instant scalar")
                    })?;
                    let expected_value = expected
                        .get("value")
                        .and_then(JsonValue::as_f64)
                        .unwrap_or(0.0);
                    if fields.len() != expected.as_object().unwrap().len()
                        || actual["labels"] != expected["labels"]
                        || !actual_value.is_finite()
                        || (actual_value - expected_value).abs() >= f64::EPSILON
                    {
                        return Err(
                            "instant scalar series differs from independent span/window ledger"
                                .into(),
                        );
                    }
                }
                Ok(())
            });
            if let Err(error) = &result {
                failures.push(format!("{id}/{base}: {error}"));
            }
            observations.push(json!({"body":body,"clock_ms":[before,after],
                "error":result.err().map(|error|error.to_string())}));
        }
        let matched = observations.iter().all(|value| value["error"].is_null());
        cases.push(
            json!({"stableid":format!("tempo-instant-{id}"),"query":query,
            "request":{"params":params},"fixture_anchor":anchor,
            "independent_expected":expected,
            "upstream":observations[0],"krabka":observations[1],
            "status":if matched {"matched"}else{"mismatch"}}),
        );
    }
    write_conformance_report(
        "tempo-instant-bounds-conformance.json",
        &json!({"planned":10,"cases":cases}),
    )?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

async fn compare_live_pipeline_hints(targets: QueryTargets<'_>, anchor: u64) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range: range,
    } = targets;
    let timestamp = |offset: i64| -> TestResult<i64> {
        Ok(i64::try_from(anchor)?
            .checked_add(offset)
            .ok_or("fixture timestamp overflow")?
            .checked_mul(1000)
            .ok_or("fixture timestamp overflow")?)
    };
    let count = pipeline_count_expectation(anchor, (1.0, 2.0))?;
    let zero = pipeline_count_expectation(anchor, (0.0, 0.0))?;
    let occupied = |samples: JsonValue| json!({"series":[{"labels":[{"key":"__name__","value":{"stringValue":"count_over_time"}}],"samples":samples,"exemplars":[]}]});
    let first = occupied(json!([{"timestampMs":timestamp(0)?.to_string(),"value":1.0}]));
    let both = occupied(
        json!([{"timestampMs":timestamp(0)?.to_string(),"value":1.0},{"timestampMs":timestamp(30)?.to_string(),"value":2.0}]),
    );
    let ranked = json!({"series":[
        {"labels":[{"key":"name","value":{"stringValue":"GET /checkout"}}],"samples":[{"timestampMs":timestamp(0)?.to_string(),"value":0.5}],"exemplars":[]},
        {"labels":[{"key":"name","value":{"stringValue":"charge card"}}],"samples":[{"timestampMs":timestamp(30)?.to_string(),"value":0.14}],"exemplars":[]}
    ]});
    let selector = "{ resource.service.name = \"checkout\" }";
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for (id, pipeline, hints, expected) in [
        (
            "ordered-ranks",
            "sum_over_time(duration) by(name) > 0 | topk(2) | bottomk(1)",
            "exemplars=false",
            ranked,
        ),
        (
            "chained-filters",
            "count_over_time() > 0 < 2",
            "exemplars=false",
            first,
        ),
        (
            "scalar-before-metrics",
            "count() > 2 | count_over_time()",
            "exemplars=false",
            count.clone(),
        ),
        (
            "scalar-before-metrics-negative",
            "count() > 3 | count_over_time()",
            "exemplars=false",
            zero.clone(),
        ),
        (
            "group-filter-before-metrics",
            "by(span.limit) | count() > 2 | count_over_time()",
            "exemplars=false",
            count.clone(),
        ),
        (
            "group-filter-before-metrics-negative",
            "by(name) | count() > 1 | count_over_time()",
            "exemplars=false",
            zero,
        ),
        (
            "typed-unknown-hints",
            "count_over_time()",
            "exemplars=false, unused=\"opaque\", most_recent=5, timeout=1s",
            count.clone(),
        ),
        (
            "typed-float-hint",
            "count_over_time()",
            "exemplars=false, sample=1.0",
            count,
        ),
        (
            "scalar-rank-filter-composition",
            "avg(span.limit) >= 3 | count_over_time() > 0 < 3 | topk(2) | bottomk(1)",
            "exemplars=false, sample=0.0",
            both,
        ),
    ] {
        let query = format!("{selector} | {pipeline} with({hints})");
        let OracleAndCandidate {
            upstream: upstream_result,
            actual: actual_result,
        } = query_range_from_both(targets, &query).await;
        let upstream = json_or_error(&upstream_result);
        let actual = json_or_error(&actual_result);
        let result = upstream_result.and_then(|upstream| {
            actual_result.and_then(|actual| {
                metric_series_match(&expected, &upstream)?;
                metric_series_match(&expected, &actual)?;
                metric_series_match(&upstream, &actual)
            })
        });
        if let Err(error) = &result {
            failures.push(format!("{id}: {error}"));
        }
        cases.push(json!({"stableid":format!("tempo-live-pipeline-{id}"), "query":query,"request":{"range":range,"step":"30s"},"expected_outcome":"query-result","comparison_kind":"exact-whole-series","independent_expected":expected,"upstream":upstream,"krabka":actual,"status":if result.is_ok(){"matched"}else{"mismatch"},"error":result.err().map(|error|error.to_string())}));
    }
    compare_live_sampled_pipelines(
        targets,
        anchor,
        ComparisonLedger {
            cases: &mut cases,
            failures: &mut failures,
        },
    )
    .await?;
    for (id, expression) in [
        (
            "aggregate-result-arithmetic-rejected",
            "count() + sum(span.limit) > 10 | count_over_time()",
        ),
        (
            "aggregate-result-comparison-rejected",
            "count() > sum(span.limit) | count_over_time()",
        ),
        (
            "dynamic-hint-rejected",
            "count_over_time() with(sample=span.limit)",
        ),
    ] {
        let query = format!("{selector} | {expression}");
        let encoded = url_encoded(&query);
        let suffix = format!("/api/metrics/query_range?q={encoded}&{range}&step=30s");
        let mut responses = Vec::new();
        let mut rejected = true;
        for (implementation, base, tenant) in [
            ("upstream", oracle, None),
            ("krabka", candidate, Some(TENANT)),
        ] {
            let mut request = client.get(format!("{base}{suffix}"));
            if let Some(tenant) = tenant {
                request = request.header("x-scope-orgid", tenant);
            }
            let response = request.send().await?;
            let status = response.status().as_u16();
            let body = response.text().await?;
            // Unsupported aggregate-result shapes are rejected in the pinned
            // validator before execution; unrelated transport errors cannot pass.
            let error_code = if body.to_ascii_lowercase().contains("scalar filter") {
                Some("unsupported-scalar-filter")
            } else {
                traceql_query_rejection_kind(status, &body)
            };
            let query_error = status == 400 && error_code.is_some();
            rejected &= query_error;
            responses.push(json!({"implementation":implementation,"http_status":status,"body":body,"query_rejection":query_error,"canonical":{"status":status,"error_code":error_code}}));
        }
        if !rejected {
            failures.push(format!("{id}: expected pinned query rejection"));
        }
        cases.push(json!({"stableid":format!("tempo-live-pipeline-{id}"),"query":query,"request":{"range":range,"step":"30s"},"expected_outcome":"query-rejection","classification":"paired-expected-error","independent_expected":{"outcome":"query-rejection","http_status":400},"oracle":{"status":responses[0]["http_status"],"error_code":if responses[0]["query_rejection"] == json!(true){"query-rejection"}else{"unclassified-error"}},"candidate":{"status":responses[1]["http_status"],"error_code":if responses[1]["query_rejection"] == json!(true){"query-rejection"}else{"unclassified-error"}},"upstream_normalized":responses[0]["canonical"],"krabka_normalized":responses[1]["canonical"],"responses":responses,"status":if rejected{"matched"}else{"mismatch"}}));
    }
    write_conformance_report(
        "tempo-pipeline-hint-conformance.json",
        &json!({"planned":15,"upstream_source":{"repository":"grafana/tempo","version":"3.0.3","revision":"1900ed7bb5cad1a3edc285783d7d4ac4278337dc"},"cases":cases}),
    )?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

fn pipeline_count_expectation(anchor: u64, occupied: (f64, f64)) -> TestResult<JsonValue> {
    let anchor_ms = i64::try_from(anchor)?
        .checked_mul(1000)
        .ok_or("fixture timestamp overflow")?;
    let samples = (-2_i64..=4).map(|index| json!({"timestampMs":(anchor_ms + index * 30_000).to_string(),"value":match index { 0 => occupied.0, 1 => occupied.1, _ => 0.0 }})).collect::<Vec<_>>();
    Ok(
        json!({"series":[{"labels":[{"key":"__name__","value":{"stringValue":"count_over_time"}}],"samples":samples,"exemplars":[]}]}),
    )
}

fn sampled_pipeline_expectations(anchor: u64, traces: bool) -> TestResult<Vec<JsonValue>> {
    // Nine spans: checkout has 3, and three reservoir traces have 2 each.
    // A periodic span sampler admits 5 of 9, hence scales by 9/5. A trace
    // sampler admits 2 of 4 complete traces and scales by 2. Physical storage
    // order can decide whether the checkout root belongs to the admitted set.
    let occupied = if traces {
        [(0.0, 8.0), (2.0, 8.0)]
    } else {
        [(0.0, 9.0), (9.0 / 5.0, 4.0 * (9.0 / 5.0))]
    };
    occupied
        .into_iter()
        .map(|counts| pipeline_count_expectation(anchor, counts))
        .collect()
}

fn sampled_average_expectations(anchor: u64) -> TestResult<Vec<JsonValue>> {
    let anchor_ms = i64::try_from(anchor)?
        .checked_mul(1000)
        .ok_or("fixture timestamp overflow")?;
    // Every one of the nine spans has value7. Sampling admits5 spans; the
    // first bucket contains only the checkout root, the second the other8.
    // Thus the second bucket is necessarily populated, independent of order.
    Ok([false, true].into_iter().map(|root_admitted| {
        let mut samples = Vec::new();
        if root_admitted { samples.push(json!({"timestampMs":anchor_ms.to_string(), "value":7.0})); }
        samples.push(json!({"timestampMs":(anchor_ms + 30_000).to_string(), "value":7.0}));
        json!({"series":[{"labels":[{"key":"__name__","value":{"stringValue":"avg_over_time"}}],"samples":samples,"exemplars":[]}]})
    }).collect())
}

fn check_sampled_pipeline_ledger(response: &JsonValue, allowed: &[JsonValue]) -> TestResult {
    if allowed
        .iter()
        .any(|expected| metric_series_match(expected, response).is_ok())
    {
        Ok(())
    } else {
        Err("sampled output differs from independently enumerated complete sampling cohorts".into())
    }
}

async fn compare_live_sampled_pipelines(
    targets: QueryTargets<'_>,
    anchor: u64,
    ledger: ComparisonLedger<'_>,
) -> TestResult {
    let QueryTargets {
        query_range: range, ..
    } = targets;
    let ComparisonLedger { cases, failures } = ledger;
    for (id, expression, traces, average) in [
        (
            "fractional-span-sampling",
            "{} | count_over_time() with(sample=0.5, exemplars=false)",
            false,
            false,
        ),
        (
            "fractional-trace-sampling",
            "{} | count() > 1 | count_over_time() with(sample=0.5, exemplars=false)",
            true,
            false,
        ),
        (
            "fractional-span-average",
            "{} | avg_over_time(span.sample.constant) with(sample=0.5, exemplars=false)",
            false,
            true,
        ),
    ] {
        let allowed = if average {
            sampled_average_expectations(anchor)?
        } else {
            sampled_pipeline_expectations(anchor, traces)?
        };
        let OracleAndCandidate {
            upstream: upstream_result,
            actual: actual_result,
        } = query_range_from_both(targets, expression).await;
        let upstream = json_or_error(&upstream_result);
        let actual = json_or_error(&actual_result);
        let result = upstream_result.and_then(|upstream| {
            actual_result.and_then(|actual| {
                check_sampled_pipeline_ledger(&upstream, &allowed)?;
                check_sampled_pipeline_ledger(&actual, &allowed)
            })
        });
        if let Err(error) = &result {
            failures.push(format!("{id}: {error}"));
        }
        let mut independent = json!({"span_count":9,"trace_sizes":[3,2,2,2],"sampling_interval":2,"sampled_entries":if traces{2}else{5},"scaling_factor":if traces{2.0}else{9.0/5.0},"allowed_complete_outputs":allowed});
        if average {
            independent["constant_value"] = json!(7);
            independent["average_value_multiplier"] = json!(1);
        }
        cases.push(json!({"stableid":format!("tempo-live-pipeline-{id}"),"query":expression,"request":{"range":range,"step":"30s"},"expected_outcome":"query-result","comparison_kind":"independent-sampling-domain","independent_expected":independent,"upstream":upstream,"krabka":actual,"status":if result.is_ok(){"matched"}else{"mismatch"},"error":result.err().map(|error|error.to_string())}));
    }
    Ok(())
}

#[test]
fn sampled_pipeline_ledger_rejects_partial_traces_wrong_scaling_and_wrong_buckets() {
    let allowed = sampled_pipeline_expectations(300, true).unwrap();
    check_sampled_pipeline_ledger(&allowed[0], &allowed).unwrap();
    check_sampled_pipeline_ledger(&allowed[1], &allowed).unwrap();
    let partial_trace = pipeline_count_expectation(300, (9.0 / 5.0, 4.0 * (9.0 / 5.0))).unwrap();
    assert2::assert!(check_sampled_pipeline_ledger(&partial_trace, &allowed).is_err());
    for (path, value) in [
        ("/series/0/samples/3/value", json!(6)),
        ("/series/0/samples/2/timestampMs", json!("300001")),
        ("/series/0/labels/0/value", json!({"stringValue":"rate"})),
    ] {
        let mut wrong = allowed[1].clone();
        *wrong.pointer_mut(path).unwrap() = value;
        assert2::assert!(
            check_sampled_pipeline_ledger(&wrong, &allowed).is_err(),
            "{path}"
        );
    }
    let span_allowed = sampled_pipeline_expectations(300, false).unwrap();
    for valid in &span_allowed {
        check_sampled_pipeline_ledger(valid, &span_allowed).unwrap();
    }
    assert2::assert!(check_sampled_pipeline_ledger(&allowed[1], &span_allowed).is_err());
}

#[test]
fn sampled_average_domain_rejects_value_scaling_and_missing_mandatory_bucket() {
    let allowed = sampled_average_expectations(300).unwrap();
    for expected in &allowed {
        check_sampled_pipeline_ledger(expected, &allowed).unwrap();
        let mut scaled = expected.clone();
        for sample in scaled["series"][0]["samples"].as_array_mut().unwrap() {
            sample["value"] = json!(7.0 * (9.0 / 5.0));
        }
        assert2::assert!(check_sampled_pipeline_ledger(&scaled, &allowed).is_err());
    }
    let mut missing = allowed[1].clone();
    missing["series"][0]["samples"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert2::assert!(check_sampled_pipeline_ledger(&missing, &allowed).is_err());
}

async fn compare_live_retrieval_shapes(targets: QueryTargets<'_>) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range,
    } = targets;
    let upstream_result = get_trace_by_id_until_found(client, oracle, None, query_range).await;
    let actual_result = get_trace_by_id(client, candidate, Some(TENANT), query_range).await;
    let upstream = json_or_error(&upstream_result);
    let actual = json_or_error(&actual_result);
    let upstream_protobuf = get_retrieval_protobuf(client, oracle, None, query_range).await;
    let actual_protobuf =
        get_retrieval_protobuf(client, candidate, Some(TENANT), query_range).await;
    let upstream_v1 = upstream_protobuf.as_ref().map_or_else(
        |error| json!({"error":error.to_string()}),
        |(_, value)| value.clone(),
    );
    let actual_v1 = actual_protobuf.as_ref().map_or_else(
        |error| json!({"error":error.to_string()}),
        |(_, value)| value.clone(),
    );
    if let Some(output) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        for (name, result) in [
            ("upstream", &upstream_protobuf),
            ("krabka", &actual_protobuf),
        ] {
            if let Ok((bytes, _)) = result {
                std::fs::write(
                    std::path::PathBuf::from(&output).join(format!("tempo-retrieval-{name}.pb")),
                    bytes,
                )?;
            }
        }
    }
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for (id, expected) in retrieval_expected_values() {
        let key = format!("retrieval.{id}");
        let result: TestResult = (|| {
            let expected = canonical_metric_any_value(&expected)?;
            let oracle_values = retrieval_values(&upstream, &key)?;
            let candidate_values = retrieval_values(&actual, &key)?;
            let independent = BTreeMap::from([
                ("span", expected.clone()),
                ("event", expected.clone()),
                ("link", expected),
            ]);
            let oracle_v1 = retrieval_values(&upstream_v1, &key)?;
            let candidate_v1 = retrieval_values(&actual_v1, &key)?;
            if oracle_values != independent
                || candidate_values != independent
                || oracle_v1 != independent
                || candidate_v1 != independent
            {
                return Err(format!("{key}: expected {independent:?}, TempoJSON {oracle_values:?}, KrabkaJSON {candidate_values:?}, TempoProtobuf {oracle_v1:?}, KrabkaProtobuf {candidate_v1:?}").into());
            }
            Ok(())
        })();
        if let Err(error) = &result {
            failures.push(format!("{id}: {error}"));
        }
        cases.push(json!({"stableid":format!("tempo-live-retrieval-shape-{id}"),"request":{"path":format!("/api/v2/traces/{TRACE_ID_HEX}"),"range":query_range,"formats":["v2-json","v1-protobuf"]},"independent_expected":{"trace_id":TRACE_ID_HEX,"span_id":"0202020202020202","attribute_key":key,"locations":["span","event","link"],"value":expected},"upstream":upstream,"krabka":actual,"v1_protobuf":{"upstream":upstream_v1,"krabka":actual_v1,"upstream_artifact":"tempo-retrieval-upstream.pb","krabka_artifact":"tempo-retrieval-krabka.pb"},"status":if result.is_ok(){"matched"}else{"mismatch"},"error":result.err().map(|error:Box<dyn std::error::Error + Send + Sync>|error.to_string())}));
    }
    write_conformance_report(
        "tempo-retrieval-shape-conformance.json",
        &json!({"planned":12,"cases":cases}),
    )?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

async fn get_retrieval_protobuf(
    client: &reqwest::Client,
    endpoint: &str,
    tenant: Option<&str>,
    range: &str,
) -> TestResult<(Vec<u8>, JsonValue)> {
    let mut request = client
        .get(format!("{endpoint}/api/traces/{TRACE_ID_HEX}?{range}"))
        .header("Accept", "application/protobuf");
    if let Some(tenant) = tenant {
        request = request.header("x-scope-orgid", tenant);
    }
    let response = request.send().await?;
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let bytes = response.bytes().await?.to_vec();
    if !status.is_success() || content_type != "application/protobuf" {
        return Err(format!(
            "trace protobuf {status}/{content_type}: {}",
            String::from_utf8_lossy(&bytes)
        )
        .into());
    }
    let trace = TracesData::decode(bytes.as_slice())?;
    let mut value = serde_json::to_value(&trace)?;
    for (index, resource) in trace.resource_spans.iter().enumerate() {
        let json = &mut value["resourceSpans"][index];
        if let Some(resource) = &resource.resource {
            restore_retrieval_attributes(&mut json["resource"], &resource.attributes);
        }
        for (index, scope) in resource.scope_spans.iter().enumerate() {
            let json = &mut json["scopeSpans"][index];
            if let Some(scope) = &scope.scope {
                restore_retrieval_attributes(&mut json["scope"], &scope.attributes);
            }
            for (index, span) in scope.spans.iter().enumerate() {
                let json = &mut json["spans"][index];
                restore_retrieval_attributes(json, &span.attributes);
                for (index, event) in span.events.iter().enumerate() {
                    restore_retrieval_attributes(&mut json["events"][index], &event.attributes);
                }
                for (index, link) in span.links.iter().enumerate() {
                    restore_retrieval_attributes(&mut json["links"][index], &link.attributes);
                }
            }
        }
    }
    Ok((bytes, json!({"trace":value})))
}

fn restore_retrieval_attributes(owner: &mut JsonValue, attributes: &[OtlpKeyValue]) {
    for (index, attribute) in attributes.iter().enumerate() {
        if let Some(value) = &attribute.value {
            owner["attributes"][index]["value"] = AttrValue::encode_otlp_json(value);
        }
    }
}

fn retrieval_values(
    response: &JsonValue,
    key: &str,
) -> TestResult<BTreeMap<&'static str, JsonValue>> {
    let spans = response["trace"]["resourceSpans"]
        .as_array()
        .ok_or("trace resources missing")?
        .iter()
        .flat_map(|resource| resource["scopeSpans"].as_array().into_iter().flatten())
        .flat_map(|scope| scope["spans"].as_array().into_iter().flatten())
        .filter(|span| span["name"] == "GET /checkout")
        .collect::<Vec<_>>();
    if spans.len() != 1 {
        return Err("retrieval root span missing or duplicated".into());
    }
    let span = spans[0];
    for (field, expected) in [("traceId", vec![1; 16]), ("spanId", vec![2; 8])] {
        let value = span[field].as_str().ok_or("retrieval identity missing")?;
        let decoded = if value.len() == expected.len() * 2
            && value.bytes().all(|value| value.is_ascii_hexdigit())
        {
            hex::decode(value)?
        } else {
            base64::engine::general_purpose::STANDARD.decode(value)?
        };
        if decoded != expected {
            return Err(format!("retrieval {field} differs").into());
        }
    }
    let mut result = BTreeMap::new();
    for (location, owner) in [
        ("span", span),
        ("event", &span["events"][0]),
        ("link", &span["links"][0]),
    ] {
        let values = owner["attributes"]
            .as_array()
            .ok_or("retrieval attributes missing")?
            .iter()
            .filter(|attribute| attribute["key"] == key)
            .collect::<Vec<_>>();
        if values.len() != 1 {
            return Err(format!("retrieval {key}/{location} missing or duplicated").into());
        }
        result.insert(location, canonical_metric_any_value(&values[0]["value"])?);
    }
    Ok(result)
}

fn retrieval_expected_values() -> Vec<(&'static str, JsonValue)> {
    vec![
        ("empty", json!({"arrayValue":{"values":[]}})),
        ("empty-oneof", json!({"arrayValue":{"values":[{}]}})),
        (
            "singleton-int",
            json!({"arrayValue":{"values":[{"intValue":"9223372036854775807"}]}}),
        ),
        (
            "multi-int",
            json!({"arrayValue":{"values":[{"intValue":"1"},{"intValue":"2"}]}}),
        ),
        (
            "multi-float",
            json!({"arrayValue":{"values":[{"doubleValue":1.5},{"doubleValue":2.5}]}}),
        ),
        (
            "multi-string",
            json!({"arrayValue":{"values":[{"stringValue":"one"},{"stringValue":"two"}]}}),
        ),
        (
            "multi-bool",
            json!({"arrayValue":{"values":[{"boolValue":true},{"boolValue":false}]}}),
        ),
        (
            "mixed",
            json!({"arrayValue":{"values":[{"intValue":"1"},{"stringValue":"1"}]}}),
        ),
        (
            "nested",
            json!({"arrayValue":{"values":[{"arrayValue":{"values":[{"intValue":"1"},{"intValue":"2"}]}}]}}),
        ),
        ("bytes", json!({"bytesValue":"AP8="})),
        (
            "kvlist",
            json!({"kvlistValue":{"values":[{"key":"answer","value":{"intValue":"9223372036854775807"}}]}}),
        ),
        (
            "nonfinite-mixed",
            json!({"arrayValue":{"values":[{"doubleValue":"Infinity"},{"doubleValue":"-Infinity"},{"doubleValue":"NaN"},{"stringValue":"finite-decoy"}]}}),
        ),
    ]
}

async fn compare_live_numeric_metrics(targets: QueryTargets<'_>) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range,
    } = targets;
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for (stableid, operation) in [
        ("avg-duration", "avg_over_time(duration)"),
        ("min-duration", "min_over_time(duration)"),
        ("max-duration", "max_over_time(duration)"),
        ("sum-duration", "sum_over_time(duration)"),
        ("quantile-duration", "quantile_over_time(duration, .5, .9)"),
        ("histogram-duration", "histogram_over_time(duration)"),
    ] {
        let query = format!(
            r#"{{ resource.service.name = "checkout" }} | {operation} with(exemplars=false)"#
        );
        let encoded = url_encoded(&query);
        let suffix = format!("/api/metrics/query_range?q={encoded}&{query_range}&step=30s");
        let upstream =
            get_json_until_positive_metric_total(client, &format!("{oracle}{suffix}"), None)
                .await?;
        let candidate_result =
            get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await;
        let actual = json_or_error(&candidate_result);
        let result = metric_series_match(&upstream, &actual);
        if let Err(error) = &result {
            failures.push(format!("{stableid}: {error}"));
        }
        cases.push(json!({"stableid":format!("tempo-live-metric-{stableid}"),"query":query,
            "request":{"range":query_range,"step":"30s","exemplars":false},
            "expected_fixture_duration_seconds":[0.5,0.15,0.14],
            "upstream":upstream,"krabka":actual,"status":if result.is_ok() {"matched"} else {"mismatch"},
            "error":result.err().map(|error|error.to_string())}));
    }
    write_conformance_report(
        "tempo-numeric-metric-conformance.json",
        &json!({"cases":cases}),
    )?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

/// A set of checkout span searches to run against Tempo and Krabka.
struct CheckoutSpanSearches<'a> {
    oracle: &'a str,
    candidate: &'a str,
    query_range: &'a str,
    /// Prefixes each case's stable id in the artifact.
    case_prefix: &'a str,
    /// The file name the cases are written to.
    artifact: &'a str,
    /// Each case's stable id, its predicate, and the span IDs it must select.
    search_cases: Vec<(&'a str, &'a str, Vec<&'a str>)>,
}

// Search Tempo and Krabka for the checkout spans each case's predicate
// selects, write every case to `artifact` among the undeclared test outputs,
// and fail when either side does not return exactly the expected span IDs.
async fn compare_checkout_span_searches(
    client: &reqwest::Client,
    searches: CheckoutSpanSearches<'_>,
) -> TestResult {
    let CheckoutSpanSearches {
        oracle,
        candidate,
        query_range,
        case_prefix,
        artifact,
        search_cases,
    } = searches;
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for (stableid, predicate, expected_span_ids) in search_cases {
        let query = format!("{{ resource.service.name = \"checkout\" && {predicate} }}");
        let encoded = url_encoded(&query);
        let suffix = format!("/api/search?q={encoded}&{query_range}&limit=10&spss=10");
        let upstream_result =
            get_json_until_non_empty_traces(client, &format!("{oracle}{suffix}"), None).await;
        let candidate_result =
            get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await;
        let upstream = json_or_error(&upstream_result);
        let actual = json_or_error(&candidate_result);
        let expected_count = expected_span_ids.len();
        let expected = BTreeMap::from([(
            TRACE_ID_HEX.to_owned(),
            expected_span_ids.into_iter().map(str::to_owned).collect(),
        )]);
        let result = upstream_result.and_then(|upstream| {
            candidate_result.and_then(|actual| {
                let upstream_ids = search_identities(&upstream)?;
                let actual_ids = search_identities(&actual)?;
                if upstream_ids != expected || actual_ids != expected {
                    return Err(format!(
                        "{query}: expected {expected:?}, upstream {upstream_ids:?}, Krabka {actual_ids:?}"
                    ).into());
                }
                Ok(())
            })
        });
        if let Err(error) = &result {
            failures.push(format!("{stableid}: {error}"));
        }
        cases.push(
            json!({"stableid":format!("{case_prefix}-{stableid}"),"query":query,
            "request":{"range":query_range,"limit":10,"spss":10},
            "independent_expected":{"span_ids_by_trace":expected,"span_count":expected_count},
            "upstream":upstream,"krabka":actual,
            "status":if result.is_ok(){"matched"}else{"mismatch"},
            "error":result.err().map(|error|error.to_string())}),
        );
    }
    write_conformance_report(artifact, &json!({"cases":cases}))?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

async fn compare_live_field_arithmetic(targets: QueryTargets<'_>) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range,
    } = targets;
    // The existing checkout fixture has three positive durations, attached to
    // independently assigned IDs 2 (GET), 3 (SELECT), and 4 (charge). Name
    // predicates provide excluded witnesses for each arithmetic expression.
    compare_checkout_span_searches(
        client,
        CheckoutSpanSearches {
            oracle,
            candidate,
            query_range,
            case_prefix: "tempo-live-arithmetic",
            artifact: "tempo-field-arithmetic-conformance.json",
            search_cases: vec![
                (
                    "multiply-duration",
                    "duration * 2 > duration && name != \"GET /checkout\"",
                    vec![CHILD_SPAN_ID_HEX, ERROR_SPAN_ID_HEX],
                ),
                (
                    "divide-duration",
                    "duration / 2 < duration && name = \"SELECT cart\"",
                    vec![CHILD_SPAN_ID_HEX],
                ),
                (
                    "add-duration",
                    "duration + duration >= duration && name != \"charge card\"",
                    vec!["0202020202020202", CHILD_SPAN_ID_HEX],
                ),
                (
                    "negate-duration",
                    "-duration < 0s && name = \"charge card\"",
                    vec![ERROR_SPAN_ID_HEX],
                ),
            ],
        },
    )
    .await
}

async fn compare_live_expression_outputs(targets: QueryTargets<'_>, anchor: u64) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range,
    } = targets;
    let checkout = "{ resource.service.name = \"checkout\" }";
    let all_ids = ["0202020202020202", CHILD_SPAN_ID_HEX, ERROR_SPAN_ID_HEX];
    let set = |ids: &[&str], attributes: JsonValue| json!({"span_ids":ids,"matched":ids.len(),"attributes":attributes});
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for (id, expression, expected) in [
        (
            "aggregate-arithmetic",
            "sum(span.limit + 1) > 10",
            json!([set(
                &all_ids,
                json!([
                    {"key":"sum(span.limit + 1)","value":{"intValue":"12"}}
                ])
            )]),
        ),
        (
            "aggregate-average",
            "avg(span.limit + 1) = 4",
            json!([set(
                &all_ids,
                json!([
                    {"key":"avg(span.limit + 1)","value":{"doubleValue":4.0}}
                ])
            )]),
        ),
        (
            "boolean-group-filter",
            "by(duration > 200ms) | count() > 1",
            json!([set(
                &all_ids[1..],
                json!([
                    {"key":"by(duration > 200ms)","value":{"boolValue":false}},
                    {"key":"count()","value":{"intValue":"2"}}
                ])
            )]),
        ),
        (
            "boolean-group-coalesce",
            "by(duration > 200ms) | coalesce() | count() > 2",
            json!([set(
                &all_ids,
                json!([
                    {"key":"count()","value":{"intValue":"3"}}
                ])
            )]),
        ),
        (
            "array-group-attributes",
            "by(span.numbers)",
            json!([
                set(
                    &all_ids[..1],
                    json!([{"key":"by(span.numbers)","value":{"arrayValue":{"values":[{"intValue":"2"},{"intValue":"4"}]}}}])
                ),
                set(
                    &all_ids[1..2],
                    json!([{"key":"by(span.numbers)","value":{"arrayValue":{"values":[{"intValue":"1"},{"intValue":"2"}]}}}])
                ),
                set(
                    &all_ids[2..],
                    json!([{"key":"by(span.numbers)","value":{"arrayValue":{"values":[{"intValue":"6"},{"intValue":"8"}]}}}])
                ),
            ]),
        ),
    ] {
        let query = format!("{checkout} | {expression}");
        let encoded = url_encoded(&query);
        let suffix = format!("/api/search?q={encoded}&{query_range}&limit=10&spss=10");
        let upstream_result =
            get_json_until_non_empty_traces(client, &format!("{oracle}{suffix}"), None).await;
        let actual_result = get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await;
        let upstream = json_or_error(&upstream_result);
        let actual = json_or_error(&actual_result);
        let expected = canonical_spanset_ledger(&expected)?;
        let result = upstream_result.and_then(|upstream| {
            actual_result.and_then(|actual| {
                if search_spanset_ledger(&upstream)? != expected
                    || search_spanset_ledger(&actual)? != expected
                {
                    return Err(format!(
                        "{id}: spanset output differs from independent fixture ledger"
                    )
                    .into());
                }
                Ok(())
            })
        });
        if let Err(error) = &result {
            failures.push(error.to_string());
        }
        cases.push(json!({"stableid":format!("tempo-live-output-{id}"),"query":query,"request":{"range":query_range,"limit":10,"spss":10},"independent_expected":{"span_sets":expected},"upstream":upstream,"krabka":actual,"status":if result.is_ok(){"matched"}else{"mismatch"},"error":result.err().map(|error|error.to_string())}));
    }
    compare_live_metric_label_outputs(
        targets,
        anchor,
        ComparisonLedger {
            cases: &mut cases,
            failures: &mut failures,
        },
    )
    .await?;
    write_conformance_report(
        "tempo-expression-output-conformance.json",
        &json!({"cases":cases}),
    )?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

async fn compare_live_metric_label_outputs(
    targets: QueryTargets<'_>,
    anchor: u64,
    ledger: ComparisonLedger<'_>,
) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range,
    } = targets;
    let ComparisonLedger { cases, failures } = ledger;
    let checkout = "{ resource.service.name = \"checkout\" }";
    for (id, field) in [
        ("bare-name-label", "name"),
        ("scoped-name-label", "span:name"),
        ("array-metric-label", "span.numbers"),
    ] {
        let selector = if field == "span.numbers" {
            // Keep the predicate to independently establish that arrays exist.
            // Tempo 3.0.3 StaticFromAnyValue drops ArrayValue at its metric
            // frontend, so the public result coalesces these labels to nil.
            "{ resource.service.name = \"checkout\" && span.numbers != nil }"
        } else {
            checkout
        };
        if field == "span.numbers" {
            record_array_metric_frontend_observation(targets, anchor).await?;
        }
        let query = format!("{selector} | count_over_time() by({field}) with(exemplars=false)");
        let label_key = if field == "span:name" { "name" } else { field };
        let encoded = url_encoded(&query);
        let suffix = format!("/api/metrics/query_range?q={encoded}&{query_range}&step=30s");
        let upstream_result =
            get_json_until_positive_metric_total(client, &format!("{oracle}{suffix}"), None).await;
        let actual_result = get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await;
        let upstream = json_or_error(&upstream_result);
        let actual = json_or_error(&actual_result);
        let expected_values = if field == "span.numbers" {
            json!([{"stringValue":"nil"}])
        } else {
            json!([{"stringValue":"GET /checkout"},{"stringValue":"SELECT cart"},{"stringValue":"charge card"}])
        };
        let expected_legends = if field == "span.numbers" {
            vec![r#"{"span.numbers"="<nil>"}"#]
        } else {
            vec![
                r#"{name="GET /checkout"}"#,
                r#"{name="SELECT cart"}"#,
                r#"{name="charge card"}"#,
            ]
        };
        let independent_series = (field == "span.numbers")
            .then(|| array_metric_frontend_expectation(anchor))
            .transpose()?;
        let each_total = if field == "span.numbers" { 3.0 } else { 1.0 };
        let result = upstream_result.and_then(|upstream| {
            actual_result.and_then(|actual| {
                metric_series_match(&upstream, &actual)?;
                if let Some(expected) = &independent_series {
                    metric_series_match(expected, &upstream)?;
                    metric_series_match(expected, &actual)?;
                }
                let mut legends = metric_prom_labels_list(&actual);
                legends.sort();
                if legends != expected_legends {
                    return Err("metric legend differs from independent fixture".into());
                }
                for response in [&upstream, &actual] {
                    let series = response["series"].as_array().ok_or("missing series")?;
                    let mut values = Vec::new();
                    for series in series {
                        let labels = sorted_metric_labels(series)?;
                        if labels.len() != 1 || labels[0]["key"] != label_key {
                            return Err("group label key/arity differs".into());
                        }
                        if (metric_points_total(&json!({"series":[series]})) - each_total).abs()
                            > f64::EPSILON
                        {
                            return Err("group count differs".into());
                        }
                        values.push(labels[0]["value"].clone());
                    }
                    values.sort_by_cached_key(JsonValue::to_string);
                    let mut expected = expected_values.as_array().unwrap().clone();
                    expected.sort_by_cached_key(JsonValue::to_string);
                    if values != expected {
                        return Err("typed group labels differ from independent fixture".into());
                    }
                }
                Ok(())
            })
        });
        if let Err(error) = &result {
            failures.push(error.to_string());
        }
        cases.push(json!({"stableid":format!("tempo-live-output-{id}"),"query":query,"request":{"range":query_range,"step":"30s"},"independent_expected":{"label_key":label_key,"label_values":expected_values,"prom_labels":expected_legends,"group_count":expected_values.as_array().unwrap().len(),"each_total":each_total,"complete_series":independent_series},"upstream":upstream,"krabka":actual,"status":if result.is_ok(){"matched"}else{"mismatch"},"error":result.err().map(|error|error.to_string())}));
    }
    Ok(())
}

fn array_metric_frontend_expectation(anchor: u64) -> TestResult<JsonValue> {
    let mut expected = pipeline_count_expectation(anchor, (1.0, 2.0))?;
    expected["series"][0]["labels"] = json!([{"key":"span.numbers","value":{"stringValue":"nil"}}]);
    Ok(expected)
}

async fn record_array_metric_frontend_observation(
    targets: QueryTargets<'_>,
    anchor: u64,
) -> TestResult {
    let QueryTargets { query_range, .. } = targets;
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    // Three source spans occupy two buckets: root 500ms, children 150ms/140ms.
    // Independent reducers pool the two child values after frontend nil decoding.
    for (id, metric, values) in [
        ("count", "count_over_time()", (1.0, 2.0)),
        ("sum", "sum_over_time(duration)", (0.5, 0.29)),
        ("avg", "avg_over_time(duration)", (0.5, 0.145)),
        ("min", "min_over_time(duration)", (0.5, 0.14)),
        ("max", "max_over_time(duration)", (0.5, 0.15)),
        ("avg-filter", "avg_over_time(duration)", (0.5, 0.145)),
    ] {
        let stages = if id == "avg-filter" {
            " > 0.14 < 0.2"
        } else {
            ""
        };
        let query = format!(
            "{{ resource.service.name = \"checkout\" }} | {metric} by(span.numbers){stages} with(exemplars=false)"
        );
        let encoded = url_encoded(&query);
        let suffix = format!("/api/metrics/query_range?q={encoded}&{query_range}&step=30s");
        let OracleAndCandidate {
            upstream: upstream_result,
            actual: actual_result,
        } = get_json_from_both(targets, &suffix).await;
        let upstream = json_or_error(&upstream_result);
        let actual = json_or_error(&actual_result);
        let mut expected = array_metric_frontend_expectation(anchor)?;
        let samples = expected["series"][0]["samples"].as_array_mut().unwrap();
        // Count fills the complete grid; other reducers omit empty buckets.
        if id != "count" {
            samples.retain(|sample| sample["value"].as_f64().unwrap() > 0.0);
            samples[0]["value"] = json!(values.0);
            samples[1]["value"] = json!(values.1);
            if id == "avg-filter" {
                samples.remove(0);
            }
        }
        let result = upstream_result.and_then(|upstream| {
            actual_result.and_then(|actual| {
                metric_series_match(&expected, &upstream)?;
                metric_series_match(&expected, &actual)
            })
        });
        if let Err(error) = &result {
            failures.push(format!("{id}: {error}"));
        }
        cases.push(json!({"stableid":format!("tempo-array-frontend-{id}"),"query":query,"request":{"range":query_range,"step":"30s"},"independent_expected":expected,"upstream":upstream,"krabka":actual,"status":if result.is_ok(){"matched"}else{"mismatch"},"error":result.err().map(|error|error.to_string())}));
    }
    write_conformance_report(
        "tempo-array-metric-frontend-conformance.json",
        &json!({"planned":6,"cases":cases,"note":"Tempo 3.0.3 StaticFromAnyValue omits ArrayValue in metric frontend decoding. Raw arrays remain present in trace retrieval and span grouping. These public reducer witnesses do not establish distinct array metric labels."}),
    )?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

fn search_spanset_ledger(response: &JsonValue) -> TestResult<JsonValue> {
    let traces = response["traces"].as_array().ok_or("missing traces")?;
    if traces.len() != 1
        || canonical_hex_id(traces[0]["traceID"].as_str().ok_or("missing trace ID")?, 32)?
            != TRACE_ID_HEX
    {
        return Err("unexpected trace identity/count".into());
    }
    let sets = traces[0]["spanSets"].as_array().ok_or("missing spansets")?;
    let mut ledger = Vec::new();
    for set in sets {
        let spans = set["spans"].as_array().ok_or("missing spans")?;
        let ids = spans
            .iter()
            .map(|span| canonical_hex_id(span["spanID"].as_str().ok_or("missing span ID")?, 16))
            .collect::<TestResult<Vec<_>>>()?;
        ledger.push(json!({"span_ids":ids,"matched":set["matched"],"attributes":set.get("attributes").cloned().unwrap_or_else(|| json!([]))}));
    }
    canonical_spanset_ledger(&json!(ledger))
}

fn canonical_spanset_ledger(ledger: &JsonValue) -> TestResult<JsonValue> {
    let mut sets = ledger.as_array().ok_or("invalid spanset ledger")?.clone();
    for set in &mut sets {
        let ids = set["span_ids"].as_array_mut().ok_or("invalid span IDs")?;
        ids.sort_by_cached_key(JsonValue::to_string);
        let attrs = set["attributes"]
            .as_array_mut()
            .ok_or("invalid spanset attributes")?;
        for attr in attrs.iter_mut() {
            attr["value"] = canonical_metric_any_value(&attr["value"])?;
        }
        attrs.sort_by_cached_key(JsonValue::to_string);
    }
    sets.sort_by_cached_key(JsonValue::to_string);
    Ok(json!(sets))
}

#[test]
fn spanset_output_ledger_rejects_wrong_groups_attributes_and_counts() {
    let expected = json!({"traces":[{"traceID":TRACE_ID_HEX,"spanSets":[{"matched":1,"spans":[{"spanID":CHILD_SPAN_ID_HEX}],"attributes":[{"key":"by(span.cost > 1)","value":{"boolValue":false}},{"key":"count()","value":{"intValue":"1"}}]}]}]});
    let expected_ledger = search_spanset_ledger(&expected).unwrap();
    for (path, value) in [
        ("/traces/0/spanSets/0/matched", json!(2)),
        (
            "/traces/0/spanSets/0/attributes/0/value",
            json!({"stringValue":"false"}),
        ),
        ("/traces/0/spanSets/0/attributes/1/key", json!("avg()")),
        (
            "/traces/0/spanSets/0/spans/0/spanID",
            json!(ERROR_SPAN_ID_HEX),
        ),
    ] {
        let mut corrupt = expected.clone();
        *corrupt.pointer_mut(path).unwrap() = value;
        assert2::assert!(
            search_spanset_ledger(&corrupt).unwrap() != expected_ledger,
            "{path}"
        );
    }
}

async fn compare_live_supported_scopes(targets: QueryTargets<'_>) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range,
    } = targets;
    compare_checkout_span_searches(
        client,
        CheckoutSpanSearches {
            oracle,
            candidate,
            query_range,
            case_prefix: "tempo-live-supported",
            artifact: "tempo-supported-scopes-conformance.json",
            search_cases: vec![
                (
                    "event-arithmetic-types",
                    "event.cost + 1 = span.limit",
                    vec!["0202020202020202"],
                ),
                (
                    "event-field-comparison",
                    "event.peer = name",
                    vec!["0202020202020202", CHILD_SPAN_ID_HEX, ERROR_SPAN_ID_HEX],
                ),
                (
                    "instrumentation-event-composition",
                    "instrumentation.cost + event.cost > span.limit",
                    vec!["0202020202020202", CHILD_SPAN_ID_HEX],
                ),
                (
                    "instrumentation-intrinsic",
                    "instrumentation:name = instrumentation.peer",
                    vec!["0202020202020202", CHILD_SPAN_ID_HEX, ERROR_SPAN_ID_HEX],
                ),
                (
                    "event-time-intrinsic",
                    "event:timeSinceStart * 2 < duration && event:name = name",
                    vec!["0202020202020202", CHILD_SPAN_ID_HEX, ERROR_SPAN_ID_HEX],
                ),
                (
                    "link-arithmetic",
                    "link.cost * 2 = span.limit + 1",
                    vec!["0202020202020202"],
                ),
                (
                    "id-intrinsic-composition",
                    "span:id = link:spanID && trace:id = link:traceID",
                    vec!["0202020202020202", CHILD_SPAN_ID_HEX],
                ),
                (
                    "trace-child-intrinsic",
                    "trace:duration >= duration && span:childCount + 1 > 2",
                    vec!["0202020202020202"],
                ),
                (
                    "trace-root-intrinsic",
                    "trace:rootName = name",
                    vec!["0202020202020202"],
                ),
                (
                    "integer-power-outside-range",
                    "span.exponent ^ span.base < 0",
                    vec!["0202020202020202", ERROR_SPAN_ID_HEX],
                ),
                (
                    "array-scalar-ordering",
                    "span.numbers > span.limit",
                    vec!["0202020202020202", ERROR_SPAN_ID_HEX],
                ),
                (
                    "scalar-array-ordering",
                    "span.limit < span.numbers && name != \"GET /checkout\"",
                    vec![ERROR_SPAN_ID_HEX],
                ),
            ],
        },
    )
    .await
}

async fn compare_live_exemplar_reservoir(
    targets: QueryTargets<'_>,
    anchor_secs: u64,
) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range: range,
    } = targets;
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for (stableid, grouping) in [("ungrouped", ""), ("grouped", " by(span.lane)")] {
        let query = format!(
            "{{ resource.service.name = \"reservoir\" && span:id != \"\" }} | count_over_time(){grouping}"
        );
        let encoded = url_encoded(&query);
        let suffix = format!("/api/metrics/query_range?q={encoded}&{range}&step=30s&exemplars=100");
        let upstream =
            get_json_until_positive_metric_total(client, &format!("{oracle}{suffix}"), None)
                .await?;
        let actual = get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await?;
        // Identity is sampled independently by each implementation. Check the
        // exact value payload and the independently known set of valid choices.
        let mut upstream_values = upstream.clone();
        let mut actual_values = actual.clone();
        for response in [&mut upstream_values, &mut actual_values] {
            for series in response["series"]
                .as_array_mut()
                .ok_or("missing metric series")?
            {
                series["exemplars"] = json!([]);
            }
        }
        let result = metric_series_match(&upstream_values, &actual_values).and_then(|()| {
            validate_reservoir_response(&upstream, anchor_secs, !grouping.is_empty())?;
            validate_reservoir_response(&actual, anchor_secs, !grouping.is_empty())
        });
        if let Err(error) = &result {
            failures.push(format!("{stableid}: {error}"));
        }
        cases.push(json!({"stableid":format!("tempo-live-exemplar-reservoir-{stableid}"),"query":query,
            "request":{"range":range,"step":"30s","exemplars":100},
            "independent_expected":{"span_count":6,"trace_count":3,"exemplar_count":2,"one_per_trace":true,"occupied_timestamp_ms":(anchor_secs+30)*1000,
                "exemplar_timestamp_precision_ms":15000,
                "valid_choices":(0_u8..6).map(|index|json!({"trace_id":format!("{:02x}",0xa1+index/2).repeat(16),"span_id":format!("{:02x}",0xb1+index).repeat(8),"span_start_ms":anchor_secs*1000+5000+u64::from(index),"timestamp_ms":(anchor_secs+15)*1000})).collect::<Vec<_>>()},
            "upstream":upstream,"krabka":actual,"status":if result.is_ok(){"matched"}else{"mismatch"},"error":result.err().map(|error|error.to_string())}));
    }
    write_conformance_report(
        "tempo-exemplar-reservoir-conformance.json",
        &json!({"cases":cases}),
    )?;
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

fn validate_reservoir_response(
    response: &JsonValue,
    anchor_secs: u64,
    grouped: bool,
) -> TestResult {
    let series = response["series"]
        .as_array()
        .ok_or("missing metric series")?;
    if series.len() != 1 {
        return Err("reservoir fixture must have one metric series".into());
    }
    if (metric_points_total(response) - 6.0).abs() > f64::EPSILON {
        return Err("reservoir metric count differs from six input spans".into());
    }
    let samples = series[0]["samples"]
        .as_array()
        .ok_or("missing metric samples")?;
    let occupied = samples
        .iter()
        .filter_map(|sample| match metric_value(sample) {
            Ok(value) if value.to_bits() == 0.0_f64.to_bits() => None,
            result => Some(result.map(|value| (sample, value))),
        })
        .collect::<TestResult<Vec<_>>>()?;
    if occupied.len() != 1
        || occupied[0].1.to_bits() != 6.0_f64.to_bits()
        || metric_timestamp(occupied[0].0)? != i64::try_from((anchor_secs + 30) * 1000)?
    {
        return Err(
            "reservoir spans must occupy exactly the independently expected right-closed bucket"
                .into(),
        );
    }
    let exemplars = series[0]["exemplars"]
        .as_array()
        .ok_or("missing exemplars")?;
    if exemplars.len() != 2 {
        return Err(format!(
            "two exemplars must fit the occupied time stratum; got {}",
            exemplars.len()
        )
        .into());
    }
    let mut seen = BTreeSet::new();
    for exemplar in exemplars {
        let labels = exemplar["labels"]
            .as_array()
            .ok_or("missing exemplar labels")?;
        let actual = labels
            .iter()
            .map(|label| {
                Ok((
                    label["key"]
                        .as_str()
                        .ok_or("missing exemplar label key")?
                        .to_owned(),
                    canonical_metric_any_value(&label["value"])?,
                ))
            })
            .collect::<TestResult<BTreeMap<_, _>>>()?;
        if actual.len() != labels.len() {
            return Err("duplicate exemplar label keys".into());
        }
        let trace = actual
            .get("trace:id")
            .and_then(|value| value["stringValue"].as_str())
            .ok_or("missing trace identity")?;
        if !seen.insert(trace.to_owned()) {
            return Err("reservoir selected two spans from one trace".into());
        }
        let span = actual
            .get("span:id")
            .and_then(|value| value["stringValue"].as_str())
            .ok_or("missing span identity")?;
        let timestamp = exemplar["timestampMs"]
            .as_str()
            .ok_or("missing exemplar timestamp")?
            .parse::<u64>()?;
        (0_u8..6)
            .find(|index| {
                format!("{:02x}", 0xa1 + index / 2).repeat(16) == trace
                    && format!("{:02x}", 0xb1 + index).repeat(8) == span
                    && (anchor_secs + 15) * 1000 == timestamp
            })
            .ok_or("exemplar does not identify an ingested span")?;
        let mut expected = BTreeMap::from([
            ("trace:id".into(), json!({"stringValue":trace})),
            ("span:id".into(), json!({"stringValue":span})),
            (
                "resource.service.name".into(),
                json!({"stringValue":"reservoir"}),
            ),
        ]);
        if grouped {
            expected.insert("span.lane".into(), json!({"stringValue":"main"}));
        }
        if actual != expected || metric_value(exemplar)?.to_bits() != 6.0_f64.to_bits() {
            return Err(format!("invalid exemplar payload {exemplar}").into());
        }
    }
    Ok(())
}

#[test]
fn reservoir_contract_rejects_duplicate_trace_foreign_span_and_payload_corruption() {
    let exemplar = |trace: &str, span: &str, timestamp: &str| {
        json!({
            "labels":[
                {"key":"trace:id","value":{"stringValue":trace}},
                {"key":"span:id","value":{"stringValue":span}},
                {"key":"resource.service.name","value":{"stringValue":"reservoir"}},
            ],"timestampMs":timestamp,"value":6
        })
    };
    let response = json!({"series":[{"samples":[{"timestampMs":"30000","value":6}],"exemplars":[
        exemplar(&"a1".repeat(16), &"b1".repeat(8), "15000"),
        exemplar(&"a2".repeat(16), &"b3".repeat(8), "15000"),
    ]}]});
    check!(validate_reservoir_response(&response, 0, false).is_ok());
    for path in [
        "trace",
        "span",
        "timestamp",
        "value",
        "extra-label",
        "duplicate-label",
        "sample-timestamp",
        "sample-value",
    ] {
        let mut invalid = response.clone();
        match path {
            "sample-timestamp" => {
                invalid["series"][0]["samples"][0]["timestampMs"] = json!("29999");
            }
            "sample-value" => invalid["series"][0]["samples"][0]["value"] = json!(5),
            "trace" => {
                invalid["series"][0]["exemplars"][1]["labels"][0]["value"]["stringValue"] =
                    json!("a1".repeat(16));
            }
            "span" => {
                invalid["series"][0]["exemplars"][1]["labels"][1]["value"]["stringValue"] =
                    json!("ff".repeat(8));
            }
            "timestamp" => invalid["series"][0]["exemplars"][1]["timestampMs"] = json!("5003"),
            "value" => invalid["series"][0]["exemplars"][1]["value"] = json!(5),
            "extra-label" => invalid["series"][0]["exemplars"][1]["labels"]
                .as_array_mut()
                .unwrap()
                .push(json!({"key":"extra","value":{"stringValue":"x"}})),
            _ => {
                let label = invalid["series"][0]["exemplars"][1]["labels"][0].clone();
                invalid["series"][0]["exemplars"][1]["labels"]
                    .as_array_mut()
                    .unwrap()
                    .push(label);
            }
        }
        check!(
            validate_reservoir_response(&invalid, 0, false).is_err(),
            "{path}"
        );
    }
}

async fn compare_live_singleton_exemplar(
    targets: QueryTargets<'_>,
    trace_start_secs: u64,
) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range,
    } = targets;
    // Tempo uses reservoir sampling within a trace. This selector contains one
    // matching span, so identity is independent of RNG. The aligned 30s metric
    // fetch uses Tempo's pre-rounded 15s start column, whose right endpoint is
    // 15s after this fixture's epoch-aligned anchor.
    let query = r#"{ name = "SELECT cart" } | count_over_time()"#;
    let encoded = url_encoded(query);
    let suffix =
        format!("/api/metrics/query_range?q={encoded}&{query_range}&step=30s&exemplars=100");
    let upstream =
        get_json_until_positive_metric_total(client, &format!("{oracle}{suffix}"), None).await?;
    let actual = get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await?;
    let output = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(
        || std::env::temp_dir().join("krabka-tempo-exemplar"),
        std::path::PathBuf::from,
    );
    let expected_exemplar = json!({"labels": [
        {"key": "trace:id", "value": {"stringValue": TRACE_ID_HEX.trim_start_matches('0')}},
        {"key": "name", "value": {"stringValue": "SELECT cart"}},
    ], "value": 1.0, "timestampMs": ((trace_start_secs + 15) * 1000).to_string()});
    let result = metric_series_match(&upstream, &actual).and_then(|()| {
        for response in [&upstream, &actual] {
            let exemplars = response["series"][0]["exemplars"].as_array().ok_or("singleton exemplar missing")?;
            if exemplars.len() != 1 || sorted_metric_labels(&exemplars[0])? != sorted_metric_labels(&expected_exemplar)?
                || metric_timestamp(&exemplars[0])? != i64::try_from((trace_start_secs + 15) * 1000)?
                || metric_value(&exemplars[0])?.to_bits() != 1.0_f64.to_bits()
            {
                return Err(format!("singleton exemplar differs from fixture ledger: expected={expected_exemplar}, response={response}").into());
            }
        }
        Ok(())
    });
    std::fs::create_dir_all(&output)?;
    std::fs::write(
        output.join("tempo-live-exemplar-conformance.json"),
        serde_json::to_vec_pretty(
            &json!({"cases":[{"stableid":"tempo-live-exemplar-singleton-count", "query": query, "request": {"range":query_range,"step":"30s","exemplars":100}, "upstream": upstream, "krabka": actual,
            "independent_expected": expected_exemplar, "expected_trace_id": TRACE_ID_HEX, "span_start_ms": trace_start_secs * 1000 + 100, "exemplar_timestamp_precision_ms":15000, "expected_timestamp_ms": (trace_start_secs + 15) * 1000,
            "status": if result.is_ok() {"matched"} else {"mismatch"}, "error":result.as_ref().err().map(ToString::to_string)}]}),
        )?,
    )?;
    result
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/tempo image"]
async fn real_tempo_and_krabka_accept_query_param_alias() -> TestResult {
    // The Grafana Tempo datasource — and therefore the Traces Drilldown
    // breakdown — sends the TraceQL metrics query under `query=`, not `q=`.
    // Real Tempo accepts both spellings; krabka must too, or every breakdown
    // panel 400s with "missing query parameter q" and renders blank. The
    // existing tests all send `q=`, so they could not catch this — this leg
    // mirrors the live datasource exactly.
    let client = reqwest::Client::new();
    let (_tempo, tempo_query, tempo_otlp) = start_ready_tempo(&client).await?;

    let (trace_start_secs, query_range) = metrics_window()?;
    let otlp_body = sample_otlp_body_at(trace_start_secs * 1_000_000_000);
    let krabka = start_krabka_pair(&otlp_body).await?;

    post_otlp(
        &client,
        &format!("{tempo_otlp}/v1/traces"),
        None,
        &otlp_body,
    )
    .await?;

    // Identical query to the `q=` test, sent under the `query=` alias.
    let metrics_query = "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D%20%7C%20count_over_time()%20with(exemplars=false)";
    let tempo_metrics = get_json_until_positive_metric_total(
        &client,
        &format!(
            "{tempo_query}/api/metrics/query_range?query={metrics_query}&{query_range}&step=30s"
        ),
        None,
    )
    .await?;
    let krabka_metrics = get_json(
        &client,
        &format!(
            "{}/api/metrics/query_range?query={metrics_query}&{query_range}&step=30s",
            krabka.base_url
        ),
        Some(TENANT),
    )
    .await?;
    assert_metric_totals_match(&tempo_metrics, &krabka_metrics);

    krabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/tempo image"]
async fn real_tempo_and_krabka_match_traceql_metrics_by_labels() -> TestResult {
    // Regression for the Grafana Traces Drilldown breakdown: its per-attribute
    // panels key on the FULL scoped attribute (e.g. `resource.service.name`), so
    // the grouped-series label key must match real Tempo exactly. Krabka
    // previously emitted the scope-stripped key (`service.name`), which left the
    // breakdown blank even though the data and totals were correct.
    let client = reqwest::Client::new();
    let (_tempo, tempo_query, tempo_otlp) = start_ready_tempo(&client).await?;

    let (trace_start_secs, query_range) = metrics_window()?;
    let otlp_body = typed_grouping_otlp_body_at(trace_start_secs * 1_000_000_000);
    let krabka = start_krabka_pair(&otlp_body).await?;

    post_otlp(
        &client,
        &format!("{tempo_otlp}/v1/traces"),
        None,
        &otlp_body,
    )
    .await?;

    // `{ resource.service.name = "checkout" }` matches every span in the sample
    // trace; group by a resource- and a span-scoped attribute.
    let base = "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D";
    for by in ["resource.service.name", "span.http.method"] {
        let metrics_query = format!("{base}%20%7C%20rate()%20by%20({by})%20with(exemplars=false)");
        let tempo_metrics = get_json_until_positive_metric_total(
            &client,
            &format!(
                "{tempo_query}/api/metrics/query_range?q={metrics_query}&{query_range}&step=30s"
            ),
            None,
        )
        .await?;
        let krabka_metrics = get_json(
            &client,
            &format!(
                "{}/api/metrics/query_range?q={metrics_query}&{query_range}&step=30s",
                krabka.base_url
            ),
            Some(TENANT),
        )
        .await?;
        let tempo_keys = metric_series_label_keys(&tempo_metrics);
        let krabka_keys = metric_series_label_keys(&krabka_metrics);
        eprintln!(
            "by({by}): Tempo keys={tempo_keys:?} promLabels={:?}; Krabka keys={krabka_keys:?} promLabels={:?}",
            metric_prom_labels_list(&tempo_metrics),
            metric_prom_labels_list(&krabka_metrics),
        );
        assert2::assert!(krabka_keys == tempo_keys);
        assert_metric_totals_match(&tempo_metrics, &krabka_metrics);
    }
    compare_live_typed_groups(QueryTargets {
        client: &client,
        oracle: &tempo_query,
        candidate: &krabka.base_url,
        query_range: &query_range,
    })
    .await?;
    krabka.shutdown();
    Ok(())
}

#[tokio::test]
async fn krabka_tenant_b_cannot_see_tenant_a_traces_tags_or_values() -> TestResult {
    let client = reqwest::Client::new();
    let query_range = "start=0&end=1";
    let query = "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D";
    let krabka = start_krabka_pair(&sample_otlp_body()).await?;

    let tenant_b_trace_status = get_status(
        &client,
        &format!(
            "{}/api/v2/traces/{TRACE_ID_HEX}?{query_range}",
            krabka.base_url
        ),
        Some("tenant-b"),
    )
    .await?;
    assert2::assert!(tenant_b_trace_status == ReqwestStatusCode::NOT_FOUND);

    let tenant_b_search = get_json(
        &client,
        &format!("{}/api/search?q={query}&{query_range}", krabka.base_url),
        Some("tenant-b"),
    )
    .await?;
    assert_search_empty(&tenant_b_search);

    let tenant_b_tags = get_json(
        &client,
        &format!("{}/api/v2/search/tags?{query_range}", krabka.base_url),
        Some("tenant-b"),
    )
    .await?;
    assert_tag_names_do_not_contain(&tenant_b_tags, "service.name");

    let tenant_b_values = get_json(
        &client,
        &format!(
            "{}/api/v2/search/tag/resource.service.name/values?{query_range}",
            krabka.base_url
        ),
        Some("tenant-b"),
    )
    .await?;
    assert_tag_values_do_not_contain(&tenant_b_values, "checkout");

    krabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana image"]
async fn grafana_accepts_tempo_datasource_pointing_at_krabka() -> TestResult {
    let client = reqwest::Client::new();
    let otlp_body = sample_otlp_body();
    let krabka = start_krabka_pair_reachable_from_container(&otlp_body).await?;

    let grafana = start_grafana().await?;
    let grafana_base = mapped_base_url(&grafana, GRAFANA_HTTP_PORT).await?;
    wait_for_http_ok(&client, &grafana_base, &["/api/health"]).await?;

    let payload = json!({
        "name": "Krabka Traces",
        "uid": GRAFANA_TEMPO_DATASOURCE_UID,
        "type": "tempo",
        "access": "proxy",
        "url": krabka.container_base_url,
        "isDefault": true,
        "jsonData": {
            "httpMethod": "GET",
            "httpHeaderName1": "X-Scope-OrgID"
        },
        "secureJsonData": {
            "httpHeaderValue1": TENANT
        }
    });
    let fetched = create_and_fetch_datasource(&client, &grafana_base, &payload).await?;

    assert2::assert!(fetched.get("type").and_then(JsonValue::as_str) == Some("tempo"));
    assert2::assert!(
        fetched.get("url").and_then(JsonValue::as_str) == Some(krabka.container_base_url.as_str())
    );

    // Probe the datasource proxy with a seeded trace query from Tempo's API.
    let trace_response = client
        .get(format!(
            "{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/api/v2/traces/{TRACE_ID_HEX}?start=0&end=1"
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?;
    let trace_status = trace_response.status();
    let trace_body = trace_response.text().await?;
    assert2::assert!(
        trace_status.is_success(),
        "Grafana trace response: {trace_body}"
    );
    let trace: JsonValue = serde_json::from_str(&trace_body)?;
    assert2::assert!(
        trace["trace"]["resourceSpans"]
            .as_array()
            .is_some_and(|spans| !spans.is_empty())
    );

    let search_query = "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D";
    let search: JsonValue = client
        .get(format!(
            "{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/api/search?q={search_query}&start=0&end=1"
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert2::assert!(
        search["traces"]
            .as_array()
            .is_some_and(|traces| !traces.is_empty())
    );

    // LEG 4 (error-span TraceQL): drive a status=error selector through the same
    // Grafana → Tempo-datasource → Krabka proxy and assert it returns the seeded
    // error trace. The seed body carries one `STATUS_CODE_ERROR` span (span id
    // 0404…), so `{ span:status = error }` must match its trace.
    let error_query = "%7B%20span%3Astatus%20%3D%20error%20%7D";
    let error_search: JsonValue = client
        .get(format!(
            "{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/api/search?q={error_query}&start=0&end=1"
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert2::assert!(error_search["traces"].as_array().is_some_and(|traces| {
        traces
            .iter()
            .any(|trace| trace["traceID"].as_str() == Some(TRACE_ID_HEX))
    }));
    assert_search_contains_span_id(&error_search, ERROR_SPAN_ID_HEX);

    krabka.shutdown();
    Ok(())
}

/// LEG 5 (Service Graph), the loop-closing leg.
///
/// In Grafana, a Tempo endpoint does NOT serve the Tempo datasource's
/// **Service Graph** tab. Grafana renders it from `traces_service_graph_*`
/// series in a **Prometheus** datasource, per spec §7.2/§8. The full production
/// loop is
///   traces → metrics-generator → Prometheus `remote_write` → Prometheus → Grafana.
///
/// What this test FAITHFULLY covers (no fabrication):
///   1. The **Grafana-side wiring**. Grafana accepts and round-trips a
///      `prometheus`-type datasource, the Service-Graph backend. That proves
///      the datasource half of the loop is configured exactly as the Tempo
///      datasource's `serviceMap.datasourceUid` would point at.
///   2. The **Krabka-side production**. The real in-process metrics-generator
///      `EdgeStore` from Slice 7 pairs the seed's client↔server span pair into
///      the exact `traces_service_graph_request_total` series that Grafana's
///      Service Graph queries, with the `client`, `server` and
///      `connection_type` edge labels. This is the series the loop depends on.
///
/// What this test does NOT stand up (a documented gap, not faked on purpose):
///   The metrics-generator → Prometheus `remote_write` ingestion path, and a
///   live Prometheus container. `krabka-traces` has no in-process Prometheus
///   `/api/v1/query` endpoint, because the metrics-generator emits through a
///   `RemoteWriteSink` rather than a query API. A live `POST /api/ds/query`
///   against the Prometheus datasource therefore cannot return real data within
///   this harness. This test proves the two ends of the loop separately, and
///   does not assert a passing query against data the harness never produces.
#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana image"]
async fn grafana_service_graph_prometheus_datasource_and_series() -> TestResult {
    let client = reqwest::Client::new();

    let grafana = start_grafana().await?;
    let grafana_base = mapped_base_url(&grafana, GRAFANA_HTTP_PORT).await?;
    wait_for_http_ok(&client, &grafana_base, &["/api/health"]).await?;

    // (1) Grafana-side wiring: provision the Prometheus datasource that backs the
    // Tempo datasource's Service Graph and assert Grafana round-trips it. In a
    // full deployment its URL points at Krabka's metrics querier / a Prometheus
    // scraping Krabka's metrics-generator `remote_write` output.
    let prom_uid = "krabka-service-graph";
    let payload = json!({
        "name": "Krabka Service Graph",
        "uid": prom_uid,
        "type": "prometheus",
        "access": "proxy",
        // Placeholder: the metrics-generator → Prometheus ingestion path is not
        // stood up in this harness (see the doc comment). The assertion below is
        // strictly the datasource round-trip, not a query against this URL.
        "url": format!("http://{DOCKER_HOST_ALIAS}:9090"),
        "isDefault": false,
        "jsonData": {
            "httpMethod": "GET",
            "httpHeaderName1": "X-Scope-OrgID"
        },
        "secureJsonData": {
            "httpHeaderValue1": TENANT
        }
    });
    let fetched = create_and_fetch_datasource(&client, &grafana_base, &payload).await?;
    assert2::assert!(fetched.get("type").and_then(JsonValue::as_str) == Some("prometheus"));

    // (2) Krabka-side production: the real metrics-generator EdgeStore pairs the
    // seed's client↔server pair into the Service-Graph series Grafana queries.
    let series = service_graph_series_for_seed_edge();
    let request_total = series
        .iter()
        .find(|s| s.name == "traces_service_graph_request_total")
        .expect("metrics-generator must emit traces_service_graph_request_total for a paired edge");
    let SeriesSample::Counter(count) = request_total.sample else {
        panic!("traces_service_graph_request_total must be a counter")
    };
    let has_edge_label = |key: &str, value: &str| {
        request_total
            .labels
            .iter()
            .any(|(k, v)| k == key && v == value)
    };
    check!(
        (count - 1.0).abs() < 1e-9,
        "expected one paired request for the seed edge, got count={count}"
    );
    check!(
        has_edge_label("client", "checkout-frontend"),
        "expected the client=checkout-frontend label on the seed edge, got labels={:?}",
        request_total.labels
    );
    check!(
        has_edge_label("server", "cart-backend"),
        "expected the server=cart-backend label on the seed edge, got labels={:?}",
        request_total.labels
    );

    Ok(())
}

/// Drive the real Slice 7 `EdgeStore` over a client↔server span pair, and
/// return the emitted service-graph series.
///
/// The pair has the same caller/callee shape that the seed's
/// `GET /checkout` → `SELECT cart` models.
fn service_graph_series_for_seed_edge() -> Vec<Series> {
    let mut store = EdgeStore::new(&MetricsGenConfig::default());
    let client = MetricsSpan {
        service: "checkout-frontend",
        span_id: [0xA; 8],
        parent: [0; 8],
        kind: MetricsSpanKind::Client,
        status: MetricsStatusCode::Ok,
        duration_ns: 10_000_000,
    }
    .record();
    let server = MetricsSpan {
        service: "cart-backend",
        span_id: [0xB; 8],
        parent: [0xA; 8],
        kind: MetricsSpanKind::Server,
        status: MetricsStatusCode::Ok,
        duration_ns: 8_000_000,
    }
    .record();
    assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
    assert2::assert!(store.record_span(&server, 1) == RecordOutcome::Completed);
    store.drain(1_000)
}

struct KrabkaPair {
    base_url: String,
    container_base_url: String,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

impl KrabkaPair {
    fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

async fn start_krabka_pair(otlp_body: &[u8]) -> TestResult<KrabkaPair> {
    start_krabka_pair_on(otlp_body, "127.0.0.1", "127.0.0.1").await
}

async fn start_krabka_pair_reachable_from_container(otlp_body: &[u8]) -> TestResult<KrabkaPair> {
    start_krabka_pair_on(otlp_body, "0.0.0.0", DOCKER_HOST_ALIAS).await
}

async fn start_krabka_pair_on(
    otlp_body: &[u8],
    bind_host: &str,
    container_host: &str,
) -> TestResult<KrabkaPair> {
    let sink = CapturingSink::default();
    let distributor_state = Arc::new(DistributorState::new(Arc::new(sink.clone())));
    push_to_door(
        authenticated(distributor::router(distributor_state)),
        DoorPush {
            uri: "/v1/traces",
            content_type: "application/x-protobuf",
            tenant: TENANT,
            body: Body::from(otlp_body.to_vec()),
            expected_status: StatusCode::OK,
        },
    )
    .await?;

    let records = sink.snapshot()?;
    let store = Arc::new(TraceqlEngine::new(
        Arc::new(span_store_from_records(&records)),
        EngineOpts {
            max_exemplars: 100,
            ..EngineOpts::default()
        },
    ));
    let app = authenticated(krabka_traces::querier::http::router(store));
    let listener = tokio::net::TcpListener::bind(format!("{bind_host}:0")).await?;
    let addr = listener.local_addr()?;
    let port = addr.port();
    let tx = serve_until_shutdown(listener, app);

    Ok(KrabkaPair {
        base_url: format!("http://127.0.0.1:{port}"),
        container_base_url: format!("http://{container_host}:{port}"),
        shutdown: tx,
    })
}

// Start Tempo and wait until it is ready, returning the container with its
// query and OTLP base URLs.
async fn start_ready_tempo(
    client: &reqwest::Client,
) -> TestResult<(testcontainers::ContainerAsync<GenericImage>, String, String)> {
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, TEMPO_HTTP_PORT).await?;
    let tempo_otlp = mapped_base_url(&tempo, TEMPO_OTLP_PORT).await?;
    wait_for_http_ok(client, &tempo_query, &["/ready", "/status"]).await?;
    Ok((tempo, tempo_query, tempo_otlp))
}

// A trace start three minutes back, aligned to 30s, and the `start`/`end`
// query range that covers it with a minute before and two after.
fn metrics_window() -> TestResult<(u64, String)> {
    let trace_start_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .saturating_sub(180)
        / 30
        * 30;
    let query_start = trace_start_secs.saturating_sub(60);
    let query_end = trace_start_secs + 120;
    Ok((
        trace_start_secs,
        format!("start={query_start}&end={query_end}"),
    ))
}

// A result's JSON, or an `error` object naming why there is none, so a
// mismatch report shows both sides either way.
fn json_or_error(result: &TestResult<JsonValue>) -> JsonValue {
    result
        .as_ref()
        .map_or_else(|error| json!({"error":error.to_string()}), Clone::clone)
}

fn url_encoded(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

async fn start_tempo() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagreed
    // testcontainers pulled the image over the network and the suite compared
    // against whatever it got rather than against the pinned bytes.
    let tag = std::env::var("KRABKA_TEMPO_IMAGE_TAG").expect(
        "KRABKA_TEMPO_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under \
         cargo, set it to that image's tag in //bazel/images/images.bzl.",
    );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/tempo".to_string(), tag)
            .with_exposed_port(TEMPO_HTTP_PORT.tcp())
            .with_exposed_port(TEMPO_OTLP_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Tempo started"))
            .with_copy_to(
                CopyTargetOptions::new("/tmp/tempo.yaml").with_mode(0o644),
                CopyDataSource::Data(TEMPO_CONFIG.as_bytes().to_vec()),
            )
            .with_cmd(["-target=all", "-config.file=/tmp/tempo.yaml"])
            .with_user("root")
            .start(),
    )
    .await??)
}

async fn start_grafana() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let container = tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        pinned_grafana_image()
            .with_exposed_port(GRAFANA_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::seconds(5))
            .with_env_var("GF_PLUGINS_PREINSTALL_DISABLED", "true")
            .with_env_var("GF_SECURITY_ADMIN_PASSWORD", "admin")
            .with_host(DOCKER_HOST_ALIAS, Host::HostGateway)
            .start(),
    )
    .await??;
    // HTTP health can precede registration of the bundled datasource plugins.
    let base = mapped_base_url(&container, GRAFANA_HTTP_PORT).await?;
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        let mut ready = true;
        for plugin in ["tempo", "prometheus"] {
            ready &= client
                .get(format!("{base}/api/plugins/{plugin}/settings"))
                .basic_auth("admin", Some("admin"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
        }
        if ready {
            return Ok(container);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("timed out waiting for Grafana datasource plugins at {base}").into())
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, paths: &[&str]) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        for path in paths {
            if client
                .get(format!("{base}{path}"))
                .send()
                .await
                .is_ok_and(|resp| resp.status().is_success())
            {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("timed out waiting for {base}").into())
}

async fn post_otlp(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
    body: &[u8],
) -> TestResult {
    let mut req = client
        .post(url)
        .header("content-type", "application/x-protobuf")
        .body(body.to_vec());
    if let Some(tenant) = tenant {
        req = req.header("x-scope-orgid", tenant);
    }
    let status = req.send().await?.status();
    assert2::assert!(status.is_success());
    Ok(())
}

async fn get_trace_by_id(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    query_range: &str,
) -> TestResult<JsonValue> {
    get_json(
        client,
        &format!("{base}/api/v2/traces/{TRACE_ID_HEX}?{query_range}"),
        tenant,
    )
    .await
}

async fn compare_generated_trace_selector(
    targets: QueryTargets<'_>,
    expression: String,
    expected: &BTreeMap<String, Vec<String>>,
) -> TestResult<Option<String>> {
    let QueryTargets {
        client,
        oracle: oracle_base,
        candidate: krabka_base,
        query_range,
    } = targets;
    let query = format!("{{ {expression} }}");
    let encoded = url_encoded(&query);
    let suffix = format!("/api/search?q={encoded}&{query_range}&limit=10&spss=10");
    // Return transport/protocol failures so the generator can archive them.
    let upstream: JsonValue = client
        .get(format!("{oracle_base}{suffix}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let actual: JsonValue = client
        .get(format!("{krabka_base}{suffix}"))
        .header("x-scope-orgid", TENANT)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let upstream_ids = search_identities(&upstream)?;
    let actual_ids = search_identities(&actual)?;
    if upstream_ids != actual_ids || &upstream_ids != expected || &actual_ids != expected {
        return Ok(Some(format!(
            "{query}: expected {expected:?}, upstream {upstream_ids:?}, Krabka {actual_ids:?}"
        )));
    }
    Ok(None)
}

async fn compare_generated_trace_rejection(
    targets: QueryTargets<'_>,
    expression: String,
    evidence: RejectionEvidence<'_>,
) -> TestResult<Option<String>> {
    let QueryTargets {
        client,
        oracle: oracle_base,
        candidate: krabka_base,
        query_range,
    } = targets;
    let RejectionEvidence {
        output,
        observations,
    } = evidence;
    let query = format!("{{ {expression} }}");
    let encoded = url_encoded(&query);
    let mut responses = Vec::with_capacity(2);
    let mut rejected = true;
    for (implementation, base, tenant) in [
        ("upstream", oracle_base, None),
        ("krabka", krabka_base, Some(TENANT)),
    ] {
        let mut request = client.get(format!("{base}/api/search?q={encoded}&{query_range}"));
        if let Some(tenant) = tenant {
            request = request.header("x-scope-orgid", tenant);
        }
        let response = request.send().await?;
        rejected &= record_rejection_response(
            &mut responses,
            ImplementationAnswer {
                implementation,
                response,
            },
            traceql_query_rejection_kind,
        )
        .await?;
    }
    let mut observations = observations
        .lock()
        .map_err(|_| "rejection ledger poisoned")?;
    observations.push(json!({"expression": expression, "query": query,
        "expected_outcome": "query-rejection", "matched": rejected, "responses": responses}));
    std::fs::create_dir_all(output)?;
    std::fs::write(
        output.join("traceql-rejection-responses.json"),
        serde_json::to_vec_pretty(&*observations)?,
    )?;
    Ok((!rejected).then(|| format!("expected parser/type rejection for {query}: {responses:?}")))
}

fn traceql_query_rejection_kind(status: u16, body: &str) -> Option<&'static str> {
    if status != 400 {
        return None;
    }
    if let Ok(json) = serde_json::from_str::<JsonValue>(body)
        && (json.get("traces").is_some()
            || json.get("data").is_some()
            || json["status"] == "success")
    {
        return None;
    }
    let message = body.to_ascii_lowercase();
    if message.contains("parse error") || message.contains("syntax error") {
        Some("parser-error")
    } else if [
        "requires string",
        "invalid type",
        "same type",
        "not valid for types",
        "illegal operation for the given types",
    ]
    .iter()
    .any(|marker| message.contains(marker))
    {
        Some("type-error")
    } else {
        None
    }
}

#[test]
fn traceql_rejection_comparator_rejects_success_and_unrelated_failures() {
    check!(
        traceql_query_rejection_kind(
            400,
            "invalid TraceQL query: illegal operation for the given types: .foo =~ 1"
        ) == Some("type-error")
    );
    check!(
        traceql_query_rejection_kind(400, "parse error: expected value") == Some("parser-error")
    );
    check!(
        traceql_query_rejection_kind(400, "plan error: regex comparison requires string value")
            == Some("type-error")
    );
    check!(traceql_query_rejection_kind(200, "parse error: expected value").is_none());
    check!(traceql_query_rejection_kind(500, "parse error: expected value").is_none());
    check!(traceql_query_rejection_kind(400, "missing query parameter q").is_none());
    check!(traceql_query_rejection_kind(400, r#"{"traces":[],"message":"parse error"}"#).is_none());
}

/// What the oracle and the candidate each answered to one request.
struct OracleAndCandidate {
    upstream: TestResult<JsonValue>,
    actual: TestResult<JsonValue>,
}

/// GETs the 30-second-step `query_range` of `query`, over the targets' query
/// window, from both the oracle and the candidate.
async fn query_range_from_both(targets: QueryTargets<'_>, query: &str) -> OracleAndCandidate {
    let encoded = url_encoded(query);
    let range = targets.query_range;
    let suffix = format!("/api/metrics/query_range?q={encoded}&{range}&step=30s");
    get_json_from_both(targets, &suffix).await
}

/// GETs `suffix` from the oracle without a tenant, and from the candidate as
/// [`TENANT`].
async fn get_json_from_both(targets: QueryTargets<'_>, suffix: &str) -> OracleAndCandidate {
    let QueryTargets {
        client,
        oracle,
        candidate,
        ..
    } = targets;
    OracleAndCandidate {
        upstream: get_json(client, &format!("{oracle}{suffix}"), None).await,
        actual: get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await,
    }
}

/// Creates a Grafana datasource from `payload`, then reads it back by its
/// `uid`, as Grafana stored it.
async fn create_and_fetch_datasource(
    client: &reqwest::Client,
    grafana_base: &str,
    payload: &JsonValue,
) -> TestResult<JsonValue> {
    let uid = payload["uid"]
        .as_str()
        .ok_or("datasource payload has a uid")?;
    let _created: JsonValue = client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth("admin", Some("admin"))
        .json(payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(client
        .get(format!("{grafana_base}/api/datasources/uid/{uid}"))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn get_json(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
) -> TestResult<JsonValue> {
    let mut req = client.get(url);
    if let Some(tenant) = tenant {
        req = req.header("x-scope-orgid", tenant);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let body = resp.bytes().await?;
    if status != ReqwestStatusCode::OK {
        return Err(format!("{url}: {status}: {}", String::from_utf8_lossy(&body)).into());
    }
    Ok(serde_json::from_slice(&body)?)
}

async fn get_status(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
) -> TestResult<ReqwestStatusCode> {
    let mut req = client.get(url);
    if let Some(tenant) = tenant {
        req = req.header("x-scope-orgid", tenant);
    }
    Ok(req.send().await?.status())
}

async fn get_json_until_non_empty_traces(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
) -> TestResult<JsonValue> {
    get_json_until(
        client,
        PollTarget {
            url,
            tenant,
            waiting_for: "non-empty traces",
        },
        |json| {
            json["traces"]
                .as_array()
                .is_some_and(|traces| !traces.is_empty())
        },
    )
    .await
}

async fn get_json_until_positive_metric_total(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
) -> TestResult<JsonValue> {
    get_json_until(
        client,
        PollTarget {
            url,
            tenant,
            waiting_for: "positive metric total",
        },
        |json| metric_points_total(json) > 0.0,
    )
    .await
}

/// One JSON endpoint to poll, and what the poll waits for, as the timeout
/// error names it.
struct PollTarget<'a> {
    url: &'a str,
    tenant: Option<&'a str>,
    waiting_for: &'static str,
}

/// Polls `target` every 500 ms for up to 30 s until `ready` accepts its JSON.
async fn get_json_until(
    client: &reqwest::Client,
    target: PollTarget<'_>,
    ready: impl Fn(&JsonValue) -> bool,
) -> TestResult<JsonValue> {
    let PollTarget {
        url,
        tenant,
        waiting_for,
    } = target;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = JsonValue::Null;
    while Instant::now() < deadline {
        let json = get_json(client, url, tenant).await?;
        if ready(&json) {
            return Ok(json);
        }
        last = json;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("timed out waiting for {waiting_for} from {url}: {last}").into())
}

/// Real Tempo has ingestion latency. A freshly pushed trace is not immediately
/// queryable by id, and `/api/v2/traces/{id}` returns 404 until the ingester
/// flushes the span out.
///
/// This function polls until the trace materialises, which mirrors how the
/// search legs poll for non-empty results. Krabka serves its in-process store
/// synchronously, so only the real-Tempo side needs this.
async fn get_trace_by_id_until_found(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    query_range: &str,
) -> TestResult<JsonValue> {
    let url = format!("{base}/api/v2/traces/{TRACE_ID_HEX}?{query_range}");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = String::new();
    while Instant::now() < deadline {
        let mut req = client.get(&url);
        if let Some(tenant) = tenant {
            req = req.header("x-scope-orgid", tenant);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.bytes().await?;
        if status == ReqwestStatusCode::OK {
            let json: JsonValue = serde_json::from_slice(&body)?;
            if json["trace"]["resourceSpans"]
                .as_array()
                .is_some_and(|spans| !spans.is_empty())
            {
                return Ok(json);
            }
            last = json.to_string();
        } else {
            last = format!("{status} {}", String::from_utf8_lossy(&body));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("timed out waiting for trace by id from {url}: {last}").into())
}

fn assert_trace_shape_matches(tempo: &JsonValue, krabka: &JsonValue) {
    if !tempo["status"].is_null() {
        assert2::assert!(tempo["status"] == krabka["status"]);
    }
    assert2::assert!(
        tempo["trace"]["resourceSpans"]
            .as_array()
            .is_some_and(|spans| !spans.is_empty())
    );
    assert2::assert!(
        krabka["trace"]["resourceSpans"]
            .as_array()
            .is_some_and(|spans| !spans.is_empty())
    );
}

fn assert_search_shape_matches(tempo: &JsonValue, krabka: &JsonValue) {
    check!(
        tempo["traces"]
            .as_array()
            .is_some_and(|traces| !traces.is_empty()),
        "search shape mismatch; Tempo search response: {tempo}; Krabka search response: {krabka}"
    );
    check!(
        krabka["traces"]
            .as_array()
            .is_some_and(|traces| !traces.is_empty()),
        "search shape mismatch; Tempo search response: {tempo}; Krabka search response: {krabka}"
    );
    check!(
        krabka["traces"][0]["traceID"].as_str() == Some(TRACE_ID_HEX),
        "search shape mismatch; Tempo search response: {tempo}; Krabka search response: {krabka}"
    );
    assert2::assert!(
        search_identities(tempo).expect("upstream search identities")
            == search_identities(krabka).expect("Krabka search identities"),
        "selected trace/span identities differ: Tempo={tempo}; Krabka={krabka}"
    );
}

fn canonical_hex_id(value: &str, width: usize) -> TestResult<String> {
    if value.is_empty()
        || value.len() > width
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("invalid hexadecimal identity".into());
    }
    Ok(format!("{:0>width$}", value.to_ascii_lowercase()))
}

fn search_identities(response: &JsonValue) -> TestResult<BTreeMap<String, Vec<String>>> {
    let Some(traces) = response.get("traces") else {
        return Ok(BTreeMap::new());
    };
    let traces = traces.as_array().ok_or("invalid traces")?;
    let mut identities = BTreeMap::new();
    for trace in traces {
        let trace_id = canonical_hex_id(trace["traceID"].as_str().ok_or("missing trace ID")?, 32)?;
        let sets = trace["spanSets"].as_array().ok_or("missing span sets")?;
        let mut spans = Vec::new();
        for set in sets {
            for span in set["spans"].as_array().ok_or("missing spans")? {
                spans.push(canonical_hex_id(
                    span["spanID"].as_str().ok_or("missing span ID")?,
                    16,
                )?);
            }
        }
        spans.sort();
        if identities.insert(trace_id.clone(), spans).is_some() {
            return Err(format!("duplicate trace ID {trace_id}").into());
        }
    }
    Ok(identities)
}

fn assert_search_contains_span_id(search: &JsonValue, span_id: &str) {
    assert2::assert!(search_contains_span_id_hex(search, span_id));
}

fn assert_search_empty(search: &JsonValue) {
    assert2::assert!(search_identities(search).unwrap().is_empty());
}

fn assert_metric_totals_match(tempo: &JsonValue, krabka: &JsonValue) {
    static CASE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let result = metric_series_match(tempo, krabka);
    if let Some(directory) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        let ordinal = CASE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::fs::write(std::path::PathBuf::from(directory).join(format!("tempo-metric-result-{ordinal}.json")),
            serde_json::to_vec_pretty(&json!({"cases":[{"status":if result.is_ok() {"matched"} else {"mismatch"},
                "upstream":tempo, "krabka":krabka, "error":result.as_ref().err().map(ToString::to_string),
                "exemplars_enabled":false, "finite_tolerance":f64::EPSILON}]})).unwrap()).unwrap();
    }
    assert2::assert!(metric_points_total(tempo) > 0.0);
    result
        .unwrap_or_else(|error| panic!("metrics differ: {error}; Tempo={tempo}; Krabka={krabka}"));
}

fn sorted_metric_labels(item: &JsonValue) -> TestResult<Vec<JsonValue>> {
    let mut labels = match item.get("labels") {
        None => Vec::new(),
        Some(labels) => labels.as_array().ok_or("invalid metric labels")?.clone(),
    };
    for label in &mut labels {
        let value = label.get_mut("value").ok_or("metric label omitted value")?;
        *value = canonical_metric_any_value(value)?;
    }
    labels.sort_by_cached_key(JsonValue::to_string);
    Ok(labels)
}

fn canonical_metric_any_value(value: &JsonValue) -> TestResult<JsonValue> {
    let mut canonical = value.clone();
    if let Some(array) = value.get("arrayValue") {
        let elements = array["values"]
            .as_array()
            .map_or_else(Vec::new, Clone::clone);
        canonical["arrayValue"]["values"] = json!(
            elements
                .iter()
                .map(canonical_metric_any_value)
                .collect::<TestResult<Vec<_>>>()?
        );
    }

    if let Some(double) = value.get("doubleValue") {
        // ProtoJSON permits both 1 and 1.0 for a double. Normalize only that
        // field's numeric representation; retain its AnyValue discriminator.
        canonical["doubleValue"] = match double {
            JsonValue::Number(number) => {
                json!(number.as_f64().ok_or("invalid double metric label")?)
            }
            JsonValue::String(number)
                if matches!(number.as_str(), "NaN" | "Infinity" | "-Infinity") =>
            {
                double.clone()
            }
            _ => return Err("invalid double metric label".into()),
        };
    }
    Ok(canonical)
}

fn metric_series_by_labels(response: &JsonValue) -> TestResult<BTreeMap<String, &JsonValue>> {
    let series = response["series"]
        .as_array()
        .ok_or("missing metric series")?;
    let mut indexed = BTreeMap::new();
    for item in series {
        if !item.is_object() {
            return Err("invalid metric series".into());
        }
        let key = serde_json::to_string(&sorted_metric_labels(item)?)?;
        if indexed.insert(key.clone(), item).is_some() {
            return Err(format!("duplicate metric series {key}").into());
        }
    }
    Ok(indexed)
}

fn metric_timestamp(item: &JsonValue) -> TestResult<i64> {
    if !item.is_object() {
        return Err("invalid metric sample or exemplar".into());
    }
    match item.get("timestampMs") {
        None => Ok(0),
        Some(JsonValue::String(timestamp)) => Ok(timestamp.parse()?),
        Some(timestamp) => timestamp
            .as_i64()
            .ok_or_else(|| "invalid metric timestamp".into()),
    }
}

fn metric_value(item: &JsonValue) -> TestResult<f64> {
    match item.get("value") {
        None => Ok(0.0),
        Some(JsonValue::String(value)) => match value.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(format!("invalid metric value {value}").into()),
        },
        Some(value) => value.as_f64().ok_or_else(|| "invalid metric value".into()),
    }
}

fn metric_series_match(expected: &JsonValue, actual: &JsonValue) -> TestResult {
    let expected = metric_series_by_labels(expected)?;
    let actual = metric_series_by_labels(actual)?;
    if !expected.keys().eq(actual.keys()) {
        return Err("metric labels differ".into());
    }
    for (labels, expected_series) in expected {
        let actual_series = actual[&labels];
        // Pinned SeriesSet::ToProto omits promLabels. When an oracle response
        // carries it, require the exact legend; independent live ledgers also
        // require the candidate legend when the pinned field is omitted.
        if let Some(legend) = expected_series.get("promLabels")
            && (!legend.is_string() || actual_series.get("promLabels") != Some(legend))
        {
            return Err(format!("{labels}: legend differs").into());
        }
        for field in ["samples", "exemplars"] {
            let expected_items = metric_items(expected_series, field)?;
            let actual_items = metric_items(actual_series, field)?;
            if expected_items.len() != actual_items.len() {
                return Err(format!("{labels}/{field}: different lengths").into());
            }
            // Tempo guarantees oldest-first samples and exemplars; retain that order.
            for (expected_item, actual_item) in expected_items.iter().zip(actual_items) {
                if metric_timestamp(expected_item)? != metric_timestamp(actual_item)? {
                    return Err(format!("{labels}/{field}: timestamp differs").into());
                }
                let expected_value = metric_value(expected_item)?;
                let actual_value = metric_value(actual_item)?;
                let equal = expected_value.to_bits() == actual_value.to_bits()
                    || (expected_value.is_nan() && actual_value.is_nan())
                    || (field == "samples"
                        && expected_value.is_finite()
                        && actual_value.is_finite()
                        && (expected_value - actual_value).abs() < f64::EPSILON);
                if !equal {
                    return Err(format!("{labels}/{field}: value differs").into());
                }
                if field == "exemplars"
                    && sorted_metric_labels(expected_item)? != sorted_metric_labels(actual_item)?
                {
                    return Err(format!("{labels}/{field}: exemplar identities differ").into());
                }
            }
        }
    }
    Ok(())
}

fn metric_items<'a>(series: &'a JsonValue, field: &str) -> TestResult<&'a [JsonValue]> {
    match series.get(field) {
        None => Ok(&[]),
        Some(items) => items
            .as_array()
            .map(Vec::as_slice)
            .ok_or_else(|| format!("invalid {field}").into()),
    }
}

#[test]
fn metrics_comparator_canonicalizes_double_spelling_without_erasing_label_types() {
    let expected = json!({"series":[{
        "labels":[{"key":"span.mixed","value":{"doubleValue":1}}],
        "samples":[{"timestampMs":"1","value":1}],
        "exemplars":[{"timestampMs":"1","value":1,
            "labels":[{"key":"span.mixed","value":{"doubleValue":1}}]}]
    }]});
    let mut equivalent = expected.clone();
    equivalent["series"][0]["labels"][0]["value"] = json!({"doubleValue":1.0});
    equivalent["series"][0]["exemplars"][0]["labels"][0]["value"] = json!({"doubleValue":1.0});
    assert2::assert!(metric_series_match(&expected, &equivalent).is_ok());
    for value in [
        json!({"intValue":"1"}),
        json!({"stringValue":"1"}),
        json!({"boolValue":true}),
        json!({"doubleValue":1.5}),
        json!({"doubleValue":null}),
    ] {
        for path in [
            "/series/0/labels/0/value",
            "/series/0/exemplars/0/labels/0/value",
        ] {
            let mut wrong = equivalent.clone();
            *wrong.pointer_mut(path).unwrap() = value.clone();
            assert2::assert!(
                metric_series_match(&expected, &wrong).is_err(),
                "{path}: {value}"
            );
        }
    }
}

#[test]
fn metrics_comparator_rejects_labels_timestamps_cancelling_values_and_exemplars() {
    let expected = json!({"series": [{
        "labels": [{"key": "span.method", "value": {"stringValue": "GET"}}],
        "promLabels": "{\"span.method\"=\"GET\"}",
        "samples": [{"timestampMs": "1000", "value": 1.0},
                    {"timestampMs": "2000", "value": 4.0}],
        "exemplars": [{"timestampMs": "1000", "value": 1.0,
            "labels": [
                {"key": "traceID", "value": {"stringValue": TRACE_ID_HEX}},
                {"key": "spanID", "value": {"stringValue": CHILD_SPAN_ID_HEX}}
            ]}]
    }]});
    metric_series_match(&expected, &expected).unwrap();
    let mut reordered_labels = expected.clone();
    reordered_labels["series"][0]["exemplars"][0]["labels"]
        .as_array_mut()
        .unwrap()
        .reverse();
    metric_series_match(&expected, &reordered_labels).unwrap();
    for (path, replacement) in [
        ("/series/0/labels/0/value/stringValue", json!("POST")),
        ("/series/0/promLabels", json!("{span_method=\"GET\"}")),
        ("/series/0/samples/0/timestampMs", json!("1001")),
        (
            "/series/0/exemplars/0/labels/0/value/stringValue",
            json!("wrong-trace"),
        ),
        (
            "/series/0/exemplars/0/labels/1/value/stringValue",
            json!(ERROR_SPAN_ID_HEX),
        ),
        ("/series/0/exemplars/0/timestampMs", json!("1001")),
        ("/series/0/exemplars/0/value", json!(2.0)),
    ] {
        let mut actual = expected.clone();
        *actual.pointer_mut(path).expect("fixture path") = replacement;
        assert2::assert!(metric_series_match(&expected, &actual).is_err(), "{path}");
    }
    let mut cancelling = expected.clone();
    cancelling["series"][0]["samples"][0]["value"] = json!(2.0);
    cancelling["series"][0]["samples"][1]["value"] = json!(3.0);
    assert2::assert!(
        metric_points_total(&expected).to_bits() == metric_points_total(&cancelling).to_bits()
    );
    assert2::assert!(metric_series_match(&expected, &cancelling).is_err());
}

#[test]
fn metrics_comparator_preserves_nonfinite_values_and_time_order() {
    let expected = json!({"series": [{"samples": [
        {"timestampMs": "1000", "value": "NaN"},
        {"timestampMs": "2000", "value": "Infinity"},
        {"timestampMs": "3000", "value": "-Infinity"}
    ]}]});
    metric_series_match(&expected, &expected).unwrap();
    let mut actual = expected.clone();
    actual["series"][0]["samples"][0]["value"] = JsonValue::Null;
    assert2::assert!(metric_series_match(&expected, &actual).is_err());
    actual = expected.clone();
    actual["series"][0]["samples"][1]["value"] = json!("-Infinity");
    assert2::assert!(metric_series_match(&expected, &actual).is_err());
    actual = expected.clone();
    actual["series"][0]["samples"]
        .as_array_mut()
        .unwrap()
        .reverse();
    assert2::assert!(metric_series_match(&expected, &actual).is_err());
}

#[test]
fn search_comparator_rejects_wrong_trace_and_span_selection() {
    let expected = json!({"traces": [{"traceID": TRACE_ID_HEX,
        "spanSets": [{"spans": [{"spanID": CHILD_SPAN_ID_HEX}]}]}]});
    let identities = search_identities(&expected).unwrap();
    for (path, replacement) in [
        ("/traces/0/traceID", "ffffffffffffffffffffffffffffffff"),
        ("/traces/0/spanSets/0/spans/0/spanID", ERROR_SPAN_ID_HEX),
    ] {
        let mut actual = expected.clone();
        *actual.pointer_mut(path).expect("fixture path") = json!(replacement);
        assert2::assert!(search_identities(&actual).unwrap() != identities, "{path}");
    }
}

/// The set of `series[].labels[].key` strings across a TraceQL-metrics
/// response. These are the grouped-attribute names Grafana renders panels by.
fn metric_series_label_keys(resp: &JsonValue) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    if let Some(series) = resp.get("series").and_then(JsonValue::as_array) {
        for s in series {
            if let Some(labels) = s.get("labels").and_then(JsonValue::as_array) {
                for kv in labels {
                    if let Some(k) = kv.get("key").and_then(JsonValue::as_str) {
                        keys.insert(k.to_string());
                    }
                }
            }
        }
    }
    keys
}

/// The `series[].promLabels` strings, Grafana's legend form, for diagnostics.
fn metric_prom_labels_list(resp: &JsonValue) -> Vec<String> {
    resp.get("series")
        .and_then(JsonValue::as_array)
        .map(|series| {
            series
                .iter()
                .filter_map(|s| {
                    s.get("promLabels")
                        .and_then(JsonValue::as_str)
                        .map(String::from)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn metric_points_total(value: &JsonValue) -> f64 {
    value["series"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|series| series["samples"].as_array().into_iter().flatten())
        .filter_map(|sample| sample["value"].as_f64())
        .sum()
}

fn assert_required_tag_names_match(tempo: &JsonValue, krabka: &JsonValue) {
    let tempo_tags = tag_names(tempo);
    let krabka_tags = tag_names(krabka);
    for required in ["service.name", "http.method", "db.system"] {
        assert2::assert!(tempo_tags.contains(required));
        assert2::assert!(krabka_tags.contains(required));
    }
}

fn assert_required_tag_values_match(tempo: &JsonValue, krabka: &JsonValue, required: &str) {
    let tempo_values = tag_values(tempo);
    let krabka_values = tag_values(krabka);
    assert2::assert!(tempo_values.contains(required));
    assert2::assert!(krabka_values.contains(required));
}

fn assert_tag_names_do_not_contain(value: &JsonValue, forbidden: &str) {
    let names = tag_names(value);
    assert2::assert!(!names.contains(forbidden));
}

fn assert_tag_values_do_not_contain(value: &JsonValue, forbidden: &str) {
    let values = tag_values(value);
    assert2::assert!(!values.contains(forbidden));
}

fn tag_names(value: &JsonValue) -> BTreeSet<String> {
    value["scopes"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|scope| {
            scope["tags"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(JsonValue::as_str)
                .map(str::to_string)
        })
        .collect()
}

fn tag_values(value: &JsonValue) -> BTreeSet<String> {
    value["tagValues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tag_value| tag_value["value"].as_str())
        .map(str::to_string)
        .collect()
}

fn sample_otlp_body() -> Vec<u8> {
    sample_otlp_body_at(1_000)
}

fn sample_otlp_body_at(start_ns: u64) -> Vec<u8> {
    let mut data = TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "checkout")],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "krabka-differential".into(),
                    version: "1.0.0".into(),
                    ..InstrumentationScope::default()
                }),
                spans: vec![
                    OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "GET /checkout".into(),
                        start_time_unix_nano: start_ns,
                        end_time_unix_nano: start_ns + 500_000_000,
                        attributes: vec![string_kv("http.method", "GET")],
                        ..OtlpSpan::default()
                    },
                    OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![3; 8],
                        parent_span_id: vec![2; 8],
                        name: "SELECT cart".into(),
                        start_time_unix_nano: start_ns + 100_000_000,
                        end_time_unix_nano: start_ns + 250_000_000,
                        attributes: vec![string_kv("db.system", "postgresql")],
                        ..OtlpSpan::default()
                    },
                    // An error-status span so the Grafana TraceQL `{ span:status = error }`
                    // leg (LEG 4) has a real error trace to find. Pushed identically to
                    // both Tempo and Krabka, so the differential corpus stays equal.
                    OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![4; 8],
                        parent_span_id: vec![2; 8],
                        name: "charge card".into(),
                        start_time_unix_nano: start_ns + 260_000_000,
                        end_time_unix_nano: start_ns + 400_000_000,
                        attributes: vec![string_kv("http.method", "POST")],
                        status: Some(OtlpStatus {
                            code: OTLP_STATUS_CODE_ERROR,
                            message: "payment declined".into(),
                        }),
                        ..OtlpSpan::default()
                    },
                ],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    };
    let scope = &mut data.resource_spans[0].scope_spans[0];
    scope.scope.as_mut().unwrap().attributes.extend([
        scalar_kv("cost", Value::IntValue(3)),
        string_kv("peer", "krabka-differential"),
    ]);
    for (index, span) in scope.spans.iter_mut().enumerate() {
        span.attributes.extend([
            scalar_kv("limit", Value::IntValue(3)),
            scalar_kv("exponent", Value::IntValue([63, 62, 1024][index])),
            scalar_kv("base", Value::IntValue(2)),
            scalar_kv(
                "numbers",
                Value::ArrayValue(ArrayValue {
                    values: [[2, 4], [1, 2], [6, 8]][index]
                        .into_iter()
                        .map(|value| AnyValue {
                            value: Some(Value::IntValue(value)),
                        })
                        .collect(),
                }),
            ),
        ]);
        span.events
            .push(opentelemetry_proto::tonic::trace::v1::span::Event {
                time_unix_nano: span.start_time_unix_nano + 1_000_000,
                name: span.name.clone(),
                attributes: vec![
                    scalar_kv(
                        "cost",
                        if index == 2 {
                            Value::StringValue("2".into())
                        } else {
                            Value::IntValue([2, 4][index])
                        },
                    ),
                    string_kv("peer", &span.name),
                ],
                ..Default::default()
            });
        if index == 1 {
            span.events
                .push(opentelemetry_proto::tonic::trace::v1::span::Event {
                    time_unix_nano: span.start_time_unix_nano + 2_000_000,
                    name: "later-event-decoy".into(),
                    attributes: vec![
                        scalar_kv("cost", Value::IntValue(2)),
                        string_kv("peer", "later-event-decoy"),
                    ],
                    ..Default::default()
                });
        }
        span.links
            .push(opentelemetry_proto::tonic::trace::v1::span::Link {
                trace_id: span.trace_id.clone(),
                span_id: if index == 2 {
                    vec![9; 8]
                } else {
                    span.span_id.clone()
                },
                attributes: vec![scalar_kv(
                    "cost",
                    Value::IntValue(i64::try_from(index).unwrap() + 2),
                )],
                ..Default::default()
            });
    }
    data.encode_to_vec()
}

fn reservoir_otlp_body_at(start_ns: u64) -> Vec<u8> {
    let mut data = TracesData::decode(sample_otlp_body_at(start_ns).as_slice()).unwrap();
    let root = &mut data.resource_spans[0].scope_spans[0].spans[0];
    let attributes = retrieval_otlp_attributes();
    root.attributes.extend(attributes.clone());
    root.events[0].attributes.extend(attributes.clone());
    root.links[0].attributes.extend(attributes);
    let spans = (0..6)
        .map(|index| OtlpSpan {
            trace_id: vec![0xa1 + index / 2; 16],
            span_id: vec![0xb1 + index; 8],
            name: format!("reservoir-{index}"),
            start_time_unix_nano: start_ns + 5_000_000_000 + u64::from(index) * 1_000_000,
            end_time_unix_nano: start_ns + 5_010_000_000 + u64::from(index) * 1_000_000,
            attributes: vec![string_kv("lane", "main")],
            ..OtlpSpan::default()
        })
        .collect();
    data.resource_spans.push(ResourceSpans {
        resource: Some(Resource {
            attributes: vec![string_kv("service.name", "reservoir")],
            ..Resource::default()
        }),
        scope_spans: vec![ScopeSpans {
            spans,
            ..ScopeSpans::default()
        }],
        ..ResourceSpans::default()
    });
    for resource in &mut data.resource_spans {
        for scope in &mut resource.scope_spans {
            for span in &mut scope.spans {
                span.attributes
                    .push(scalar_kv("sample.constant", Value::IntValue(7)));
            }
        }
    }
    data.encode_to_vec()
}

fn retrieval_otlp_attributes() -> Vec<OtlpKeyValue> {
    let array = |values: Vec<Value>| {
        Value::ArrayValue(ArrayValue {
            values: values
                .into_iter()
                .map(|value| AnyValue { value: Some(value) })
                .collect(),
        })
    };
    vec![
        scalar_kv("retrieval.empty", array(Vec::new())),
        scalar_kv(
            "retrieval.empty-oneof",
            Value::ArrayValue(ArrayValue {
                values: vec![AnyValue { value: None }],
            }),
        ),
        scalar_kv(
            "retrieval.singleton-int",
            array(vec![Value::IntValue(i64::MAX)]),
        ),
        scalar_kv(
            "retrieval.multi-int",
            array(vec![Value::IntValue(1), Value::IntValue(2)]),
        ),
        scalar_kv(
            "retrieval.multi-float",
            array(vec![Value::DoubleValue(1.5), Value::DoubleValue(2.5)]),
        ),
        scalar_kv(
            "retrieval.multi-string",
            array(vec![
                Value::StringValue("one".into()),
                Value::StringValue("two".into()),
            ]),
        ),
        scalar_kv(
            "retrieval.multi-bool",
            array(vec![Value::BoolValue(true), Value::BoolValue(false)]),
        ),
        scalar_kv(
            "retrieval.mixed",
            array(vec![Value::IntValue(1), Value::StringValue("1".into())]),
        ),
        scalar_kv(
            "retrieval.nested",
            array(vec![array(vec![Value::IntValue(1), Value::IntValue(2)])]),
        ),
        scalar_kv("retrieval.bytes", Value::BytesValue(vec![0, 255])),
        scalar_kv(
            "retrieval.kvlist",
            Value::KvlistValue(KeyValueList {
                values: vec![scalar_kv("answer", Value::IntValue(i64::MAX))],
            }),
        ),
        scalar_kv(
            "retrieval.nonfinite-mixed",
            array(vec![
                Value::DoubleValue(f64::INFINITY),
                Value::DoubleValue(f64::NEG_INFINITY),
                Value::DoubleValue(f64::NAN),
                Value::StringValue("finite-decoy".into()),
            ]),
        ),
    ]
}

#[test]
fn retrieval_shape_ledger_rejects_type_collapse_order_and_nonfinite_corruption() {
    assert2::assert!(
        traceql_attr(&AttrValue::Bytes(vec![0, 255]))
            == Some(TraceqlAttrValue::Unsupported(
                r#"{"bytesValue":"AP8="}"#.into()
            ))
    );
    let attributes = retrieval_otlp_attributes();
    let span = json!({"name":"GET /checkout","traceId": "01010101010101010101010101010101", "spanId":"0202020202020202",
        "attributes": attributes.iter().map(|attribute|json!({"key":attribute.key,"value":AttrValue::encode_otlp_json(attribute.value.as_ref().unwrap())})).collect::<Vec<_>>()});
    let mut span = span;
    span["events"] = json!([{"attributes":span["attributes"]}]);
    span["links"] = json!([{"attributes":span["attributes"]}]);
    let response = json!({"trace":{"resourceSpans":[{"scopeSpans":[{"spans":[span]}]}]}});
    for (id, expected) in retrieval_expected_values() {
        let values = retrieval_values(&response, &format!("retrieval.{id}")).unwrap();
        assert2::assert!(values.values().all(|value| value == &expected));
    }
    for (key, corrupted) in [
        ("retrieval.empty", json!({"stringValue":"nil"})),
        ("retrieval.empty-oneof", json!({"arrayValue":{"values":[]}})),
        (
            "retrieval.singleton-int",
            json!({"intValue":"9223372036854775807"}),
        ),
        (
            "retrieval.multi-int",
            json!({"arrayValue":{"values":[{"intValue":"2"},{"intValue":"1"}]}}),
        ),
        (
            "retrieval.mixed",
            json!({"arrayValue":{"values":[{"intValue":"1"},{"intValue":"1"}]}}),
        ),
        (
            "retrieval.nonfinite-mixed",
            json!({"arrayValue":{"values":[{"doubleValue":null},{"doubleValue":"-Infinity"},{"doubleValue":"NaN"},{"stringValue":"finite-decoy"}]}}),
        ),
    ] {
        let expected = retrieval_values(&response, key).unwrap();
        let mut wrong = response.clone();
        let attrs = wrong["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"]
            .as_array_mut()
            .unwrap();
        attrs
            .iter_mut()
            .find(|attribute| attribute["key"] == key)
            .unwrap()["value"] = corrupted;
        assert2::assert!(retrieval_values(&wrong, key).map_or(true, |values| values != expected));
    }
}

fn forest_otlp_fixture() -> TracesData {
    let spans = [
        (10_u8, 0_u8, "ancestor1", "ancestor"),
        (11, 10, "descendant1a", "descendant"),
        (12, 10, "descendant1b", "descendant"),
        (20, 0, "ancestor2", "ancestor"),
        (21, 20, "descendant2a", "descendant"),
        (22, 20, "descendant2b", "descendant"),
        (23, 22, "descendant2bb", "descendant"),
        (30, 99, "partial", "disconnected"),
    ]
    .into_iter()
    .map(|(id, parent, name, kind)| {
        let mut attributes = vec![string_kv("kind", kind)];
        if id == 11 {
            attributes.extend([string_kv("foo", "same"), string_kv("bar", "same")]);
        }
        if id == 12 {
            attributes.push(string_kv("foo", "same"));
            attributes.push(OtlpKeyValue {
                key: "bar".into(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(5)),
                }),
                ..OtlpKeyValue::default()
            });
        }
        OtlpSpan {
            trace_id: vec![9; 16],
            span_id: vec![id; 8],
            parent_span_id: if parent == 0 { vec![] } else { vec![parent; 8] },
            name: name.into(),
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 500_000_000,
            attributes,
            ..OtlpSpan::default()
        }
    })
    .collect();
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "forest")],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
}

fn scalar_kv(key: &str, value: Value) -> OtlpKeyValue {
    OtlpKeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(value) }),
        ..OtlpKeyValue::default()
    }
}

/// Writes `report` as pretty JSON to `file_name` among Bazel's undeclared
/// test outputs, when the run has that directory.
fn write_conformance_report(file_name: &str, report: &JsonValue) -> TestResult {
    if let Some(output) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        std::fs::write(
            std::path::PathBuf::from(output).join(file_name),
            serde_json::to_vec_pretty(report)?,
        )?;
    }
    Ok(())
}

// The routers read the principal from the request extensions, where the
// authentication layer puts it. This is that layer with no security flags,
// which serves every request as unauthenticated.
fn authenticated(router: axum::Router) -> axum::Router {
    krabka_observability::server_security::authenticate_requests(
        router,
        &krabka_observability::server_security::ServerSecurity::default(),
    )
}

fn typed_grouping_otlp_body_at(start_ns: u64) -> Vec<u8> {
    let mut data = TracesData::decode(sample_otlp_body_at(start_ns).as_slice()).unwrap();
    let spans = [
        Some(Value::IntValue(1)),
        Some(Value::IntValue(1)),
        Some(Value::StringValue("1".into())),
        Some(Value::DoubleValue(1.0)),
        Some(Value::BoolValue(true)),
        Some(Value::BoolValue(false)),
        None,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, mixed)| {
        let mut attributes = vec![OtlpKeyValue {
            key: "present".into(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(42)),
            }),
            ..OtlpKeyValue::default()
        }];
        if let Some(value) = mixed {
            attributes.push(OtlpKeyValue {
                key: "mixed".into(),
                value: Some(AnyValue { value: Some(value) }),
                ..OtlpKeyValue::default()
            });
        }
        OtlpSpan {
            trace_id: vec![0x44; 16],
            span_id: vec![u8::try_from(index + 1).unwrap(); 8],
            name: format!("typed-{index}"),
            start_time_unix_nano: start_ns + 100_000_000,
            end_time_unix_nano: start_ns + 101_000_000,
            attributes,
            ..OtlpSpan::default()
        }
    })
    .collect();
    data.resource_spans.push(ResourceSpans {
        resource: Some(Resource {
            attributes: vec![string_kv("service.name", "typed")],
            ..Resource::default()
        }),
        scope_spans: vec![ScopeSpans {
            spans,
            ..ScopeSpans::default()
        }],
        ..ResourceSpans::default()
    });
    data.encode_to_vec()
}

async fn compare_live_typed_groups(targets: QueryTargets<'_>) -> TestResult {
    let QueryTargets {
        client,
        oracle,
        candidate,
        query_range: range,
    } = targets;
    let output = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR").map_or_else(
        || std::env::temp_dir().join("krabka-tempo-typed-groups"),
        std::path::PathBuf::from,
    );
    std::fs::create_dir_all(&output)?;
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    for (stableid, grouping) in [
        ("tempo-live-group-heterogeneous-scalars", "span.mixed"),
        (
            "tempo-live-group-heterogeneous-and-missing",
            "span.mixed,span.present",
        ),
    ] {
        let query = format!(
            "{{ resource.service.name = \"typed\" }} | count_over_time() by ({grouping}) with(exemplars=false)"
        );
        let encoded = url_encoded(&query);
        let suffix = format!("/api/metrics/query_range?q={encoded}&{range}&step=30s");
        let upstream =
            get_json_until_positive_metric_total(client, &format!("{oracle}{suffix}"), None)
                .await?;
        let actual = get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await?;
        let independent = typed_group_expectation(grouping);
        let result = metric_series_match(&upstream, &actual).and_then(|()| {
            check_typed_group_ledger(&upstream, grouping, &independent)?;
            check_typed_group_ledger(&actual, grouping, &independent)
        });
        if let Err(error) = &result {
            failures.push(format!("{stableid}: {error}"));
        }
        cases.push(json!({"stableid":stableid,"query":query,"request":{"range":range,"step":"30s","exemplars":false},"independent_expected":independent,"upstream":upstream,"krabka":actual,"status":if result.is_ok(){"matched"}else{"mismatch"},"error":result.err().map(|error|error.to_string())}));
        std::fs::write(
            output.join("tempo-typed-group-conformance.json"),
            serde_json::to_vec_pretty(&json!({"cases":cases}))?,
        )?;
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

fn typed_group_expectation(grouping: &str) -> JsonValue {
    let mut mixed = vec![
        json!({"intValue":"1"}),
        json!({"stringValue":"1"}),
        json!({"doubleValue":1.0}),
        json!({"boolValue":true}),
        json!({"boolValue":false}),
    ];
    if grouping == "span.mixed" {
        mixed.push(json!({"stringValue":"nil"}));
    }
    let mut expected = json!({"group_count":6,"total":7,"mixed_values":mixed,"int_one_count":2});
    if grouping != "span.mixed" {
        expected["present"] = json!({"intValue":"42"});
    }
    expected
}

fn check_typed_group_ledger(
    response: &JsonValue,
    grouping: &str,
    expected: &JsonValue,
) -> TestResult {
    let groups = response["series"]
        .as_array()
        .ok_or("response omitted typed groups")?;
    let observed = groups
        .iter()
        .flat_map(|series| series["labels"].as_array().into_iter().flatten())
        .filter(|label| label["key"] == "span.mixed")
        .map(|label| canonical_metric_any_value(&label["value"]).map(|value| value.to_string()))
        .collect::<TestResult<BTreeSet<_>>>()?;
    let values = expected["mixed_values"]
        .as_array()
        .ok_or("expected label ledger")?
        .iter()
        .map(|value| canonical_metric_any_value(value).map(|value| value.to_string()))
        .collect::<TestResult<BTreeSet<_>>>()?;
    if groups.len() != 6
        || observed != values
        || metric_points_total(response).to_bits() != 7.0_f64.to_bits()
    {
        return Err(format!(
            "typed group independent ledger differs: expected={expected}, response={response}"
        )
        .into());
    }
    for series in groups {
        let labels = series["labels"].as_array().ok_or("missing group labels")?;
        if grouping != "span.mixed"
            && !labels.iter().any(|label| {
                label["key"] == "span.present" && label["value"] == expected["present"]
            })
        {
            return Err("composition group lost typed int42 label".into());
        }
        let count = if labels
            .iter()
            .any(|label| label["key"] == "span.mixed" && label["value"] == json!({"intValue":"1"}))
        {
            2.0_f64
        } else {
            1.0_f64
        };
        let total = metric_points_total(&json!({"series":[series]}));
        if total.to_bits() != count.to_bits() {
            return Err(format!("typed per-group count differs: {series}").into());
        }
    }
    Ok(())
}

#[test]
fn typed_group_ledger_rejects_type_coercion_with_unchanged_counts() {
    let expected = typed_group_expectation("span.mixed");
    let groups = expected["mixed_values"].as_array().unwrap().iter().map(|value| {
        let count = if value == &json!({"intValue":"1"}) { 2.0 } else { 1.0 };
        json!({"labels":[{"key":"span.mixed","value":value}],"samples":[{"timestampMs":"1","value":count}]})
    }).collect::<Vec<_>>();
    let correct = json!({"series":groups});
    assert2::assert!(check_typed_group_ledger(&correct, "span.mixed", &expected).is_ok());
    let mut equivalent = correct.clone();
    equivalent["series"][2]["labels"][0]["value"] = json!({"doubleValue":1});
    assert2::assert!(check_typed_group_ledger(&equivalent, "span.mixed", &expected).is_ok());
    for (group, replacement) in [
        (0, json!({"stringValue":"1"})),
        (1, json!({"doubleValue":1})),
        (2, json!({"intValue":"1"})),
        (3, json!({"intValue":"1"})),
        (4, json!({"boolValue":true})),
    ] {
        let mut conflated = equivalent.clone();
        conflated["series"][group]["labels"][0]["value"] = replacement;
        assert2::assert!(metric_points_total(&conflated).to_bits() == 7.0_f64.to_bits());
        assert2::assert!(check_typed_group_ledger(&conflated, "span.mixed", &expected).is_err());
    }
}
