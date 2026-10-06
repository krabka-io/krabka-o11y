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
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use generated_differential::{CompareOp, TypedConstructor, TypedExpr};
use http_body_util::BodyExt as _;
use krabka_traceql::{
    AttrValue as TraceqlAttrValue, EngineOpts, InMemorySpanStore, InputSpan, TraceqlEngine,
};
use krabka_traces::{
    AttrValue, Span, SpanRecord, TracesError,
    distributor::{self, DistributorState, WalSink},
    metricsgen::{
        EdgeStore, MetricsGenConfig, RecordOutcome, Series, SeriesSample,
        SpanKind as MetricsSpanKind, SpanRecord as MetricsSpanRecord,
        StatusCode as MetricsStatusCode,
    },
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue as OtlpKeyValue, any_value::Value},
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
use tower::ServiceExt as _;

#[path = "../../metrics-service/tests/support/generated_differential.rs"]
mod generated_differential;

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

#[derive(Clone, Default)]
struct CapturingSink {
    records: Arc<Mutex<Vec<SpanRecord>>>,
}

#[async_trait::async_trait]
impl WalSink for CapturingSink {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
        self.records
            .lock()
            .map_err(|_| TracesError::Wal("capturing sink lock poisoned".into()))?
            .push(rec);
        Ok(())
    }
}

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
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, TEMPO_HTTP_PORT).await?;
    let tempo_otlp = mapped_base_url(&tempo, TEMPO_OTLP_PORT).await?;
    wait_for_http_ok(&client, &tempo_query, &["/ready", "/status"]).await?;

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
        let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
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

    compare_generated_trace_queries(&client, &tempo_query, &krabka.base_url, query_range).await?;

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

async fn compare_generated_trace_queries(
    client: &reqwest::Client,
    tempo_query: &str,
    krabka_query: &str,
    query_range: &str,
) -> TestResult {
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
            TypedConstructor::TraceOr(TypedExpr::trace_string_compare(
                "",
                "name",
                CompareOp::Eq,
                "missing",
            )),
        ],
        &generated_output,
        |expression| {
            compare_generated_trace_selector(
                client,
                tempo_query,
                krabka_query,
                query_range,
                expression,
                &expected_checkout,
            )
        },
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
        |expression| {
            compare_generated_trace_selector(
                client,
                tempo_query,
                krabka_query,
                query_range,
                expression,
                &expected_field_match,
            )
        },
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
                client,
                tempo_query,
                krabka_query,
                query_range,
                expression,
                &generated_output,
                &rejection_responses,
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
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, TEMPO_HTTP_PORT).await?;
    let tempo_otlp = mapped_base_url(&tempo, TEMPO_OTLP_PORT).await?;
    wait_for_http_ok(&client, &tempo_query, &["/ready", "/status"]).await?;

    let trace_start_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .saturating_sub(180)
        / 30
        * 30;
    let query_start = trace_start_secs.saturating_sub(60);
    let query_end = trace_start_secs + 120;
    let query_range = format!("start={query_start}&end={query_end}");
    let otlp_body = sample_otlp_body_at(trace_start_secs * 1_000_000_000);
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
    let numeric_result =
        compare_live_numeric_metrics(&client, &tempo_query, &krabka.base_url, &query_range).await;
    let exemplar_result = compare_live_singleton_exemplar(
        &client,
        &tempo_query,
        &krabka.base_url,
        &query_range,
        trace_start_secs,
    )
    .await;
    let arithmetic_result =
        compare_live_field_arithmetic(&client, &tempo_query, &krabka.base_url, &query_range).await;
    numeric_result?;
    exemplar_result?;
    arithmetic_result?;

    krabka.shutdown();
    Ok(())
}

async fn compare_live_numeric_metrics(
    client: &reqwest::Client,
    oracle: &str,
    candidate: &str,
    query_range: &str,
) -> TestResult {
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
        let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
        let suffix = format!("/api/metrics/query_range?q={encoded}&{query_range}&step=30s");
        let upstream =
            get_json_until_positive_metric_total(client, &format!("{oracle}{suffix}"), None)
                .await?;
        let candidate_result =
            get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await;
        let actual = candidate_result
            .as_ref()
            .map_or_else(|error| json!({"error":error.to_string()}), Clone::clone);
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
    if let Some(output) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        std::fs::write(
            std::path::PathBuf::from(output).join("tempo-numeric-metric-conformance.json"),
            serde_json::to_vec_pretty(&json!({"cases":cases}))?,
        )?;
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

async fn compare_live_field_arithmetic(
    client: &reqwest::Client,
    oracle: &str,
    candidate: &str,
    query_range: &str,
) -> TestResult {
    let mut cases = Vec::new();
    let mut failures = Vec::new();
    // The existing checkout fixture has three positive durations, attached to
    // independently assigned IDs 2 (GET), 3 (SELECT), and 4 (charge). Name
    // predicates provide excluded witnesses for each arithmetic expression.
    for (stableid, predicate, expected_span_ids) in [
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
    ] {
        let query = format!("{{ resource.service.name = \"checkout\" && {predicate} }}");
        let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
        let suffix = format!("/api/search?q={encoded}&{query_range}&limit=10&spss=10");
        let upstream_result =
            get_json_until_non_empty_traces(client, &format!("{oracle}{suffix}"), None).await;
        let candidate_result =
            get_json(client, &format!("{candidate}{suffix}"), Some(TENANT)).await;
        let upstream = upstream_result
            .as_ref()
            .map_or_else(|error| json!({"error":error.to_string()}), Clone::clone);
        let actual = candidate_result
            .as_ref()
            .map_or_else(|error| json!({"error":error.to_string()}), Clone::clone);
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
            json!({"stableid":format!("tempo-live-arithmetic-{stableid}"),"query":query,
            "request":{"range":query_range,"limit":10,"spss":10},
            "independent_expected":{"span_ids_by_trace":expected,"span_count":expected_count},
            "upstream":upstream,"krabka":actual,
            "status":if result.is_ok(){"matched"}else{"mismatch"},
            "error":result.err().map(|error|error.to_string())}),
        );
    }
    if let Some(output) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        std::fs::write(
            std::path::PathBuf::from(output).join("tempo-field-arithmetic-conformance.json"),
            serde_json::to_vec_pretty(&json!({"cases":cases}))?,
        )?;
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

async fn compare_live_singleton_exemplar(
    client: &reqwest::Client,
    oracle: &str,
    candidate: &str,
    query_range: &str,
    trace_start_secs: u64,
) -> TestResult {
    // Tempo uses reservoir sampling within a trace. This selector contains one
    // matching span, so its exact identity and timestamp are independent of RNG.
    let query = r#"{ name = "SELECT cart" } | count_over_time()"#;
    let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
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
    ], "value": 1.0, "timestampMs": (trace_start_secs * 1000 + 100).to_string()});
    let result = metric_series_match(&upstream, &actual).and_then(|()| {
        for response in [&upstream, &actual] {
            let exemplars = response["series"][0]["exemplars"].as_array().ok_or("singleton exemplar missing")?;
            if exemplars.len() != 1 || sorted_metric_labels(&exemplars[0])? != sorted_metric_labels(&expected_exemplar)?
                || metric_timestamp(&exemplars[0])? != i64::try_from(trace_start_secs * 1000 + 100)?
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
            "independent_expected": expected_exemplar, "expected_trace_id": TRACE_ID_HEX, "expected_timestamp_ms": trace_start_secs * 1000 + 100,
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
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, TEMPO_HTTP_PORT).await?;
    let tempo_otlp = mapped_base_url(&tempo, TEMPO_OTLP_PORT).await?;
    wait_for_http_ok(&client, &tempo_query, &["/ready", "/status"]).await?;

    let trace_start_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .saturating_sub(180)
        / 30
        * 30;
    let query_start = trace_start_secs.saturating_sub(60);
    let query_end = trace_start_secs + 120;
    let query_range = format!("start={query_start}&end={query_end}");
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
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, TEMPO_HTTP_PORT).await?;
    let tempo_otlp = mapped_base_url(&tempo, TEMPO_OTLP_PORT).await?;
    wait_for_http_ok(&client, &tempo_query, &["/ready", "/status"]).await?;

    let trace_start_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .saturating_sub(180)
        / 30
        * 30;
    let query_start = trace_start_secs.saturating_sub(60);
    let query_end = trace_start_secs + 120;
    let query_range = format!("start={query_start}&end={query_end}");
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
    compare_live_typed_groups(&client, &tempo_query, &krabka.base_url, &query_range).await?;
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
    let _created: JsonValue = client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth("admin", Some("admin"))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let fetched: JsonValue = client
        .get(format!(
            "{grafana_base}/api/datasources/uid/{GRAFANA_TEMPO_DATASOURCE_UID}"
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

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
    let _created: JsonValue = client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth("admin", Some("admin"))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let fetched: JsonValue = client
        .get(format!("{grafana_base}/api/datasources/uid/{prom_uid}"))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
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
    let client = metrics_span(
        "checkout-frontend",
        [0xA; 8],
        [0; 8],
        MetricsSpanKind::Client,
        MetricsStatusCode::Ok,
        10_000_000,
    );
    let server = metrics_span(
        "cart-backend",
        [0xB; 8],
        [0xA; 8],
        MetricsSpanKind::Server,
        MetricsStatusCode::Ok,
        8_000_000,
    );
    assert2::assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
    assert2::assert!(store.record_span(&server, 1) == RecordOutcome::Completed);
    store.drain(1_000)
}

fn metrics_span(
    service: &str,
    span_id: [u8; 8],
    parent: [u8; 8],
    kind: MetricsSpanKind,
    status: MetricsStatusCode,
    duration_ns: i64,
) -> MetricsSpanRecord {
    MetricsSpanRecord {
        tenant: TENANT.into(),
        trace_id: [0x11; 16],
        span_id,
        parent_span_id: parent,
        name: "op".into(),
        kind,
        start_ns: 0,
        duration_ns,
        status,
        status_message: String::new(),
        service_name: service.into(),
        attributes: vec![],
        resource_attributes: vec![],
        size: ByteSize::from_bytes(0),
    }
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
    let resp = authenticated(distributor::router(distributor_state))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/x-protobuf")
                .header("x-scope-orgid", TENANT)
                .body(Body::from(otlp_body.to_vec()))?,
        )
        .await?;
    assert2::assert!(resp.status() == StatusCode::OK);
    let _ = resp.into_body().collect().await?;

    let records = sink
        .records
        .lock()
        .map_err(|_| "capturing sink lock poisoned")?
        .clone();
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
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });

    Ok(KrabkaPair {
        base_url: format!("http://127.0.0.1:{port}"),
        container_base_url: format!("http://{container_host}:{port}"),
        shutdown: tx,
    })
}

fn span_store_from_records(records: &[SpanRecord]) -> InMemorySpanStore {
    let mut grouped: BTreeMap<(String, [u8; 16]), Vec<Span>> = BTreeMap::new();
    for record in records {
        grouped
            .entry((record.tenant.clone(), record.span.trace_id))
            .or_default()
            .push(record.span.clone());
    }

    let mut store = InMemorySpanStore::new();
    for ((tenant, _), spans) in grouped {
        let root = spans
            .iter()
            .find(|span| span.parent_span_id.is_none())
            .unwrap_or(&spans[0]);
        let root_service = resource_attr(root, "service.name")
            .unwrap_or("unknown")
            .to_string();
        let root_name = root.name.clone();
        store.push_trace(
            &tenant,
            &root_service,
            &root_name,
            spans.into_iter().map(input_span).collect(),
        );
    }
    store
}

fn input_span(span: Span) -> InputSpan {
    let mut attrs = span.resource_attrs;
    attrs.extend(span.span_attrs);
    InputSpan {
        trace_id: span.trace_id,
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name,
        kind: span.kind.as_i32(),
        start_unix_nano: span.start_ns,
        duration: Time::from_nanos(span.duration_ns),
        status_code: span.status.as_i32(),
        status_message: span.status_message,
        instrumentation_name: span.instrumentation_scope,
        instrumentation_version: span.instrumentation_version,
        attrs: attrs
            .into_iter()
            .filter_map(|attr| Some((attr.key, traceql_attr(attr.value)?)))
            .collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

fn traceql_attr(value: AttrValue) -> Option<TraceqlAttrValue> {
    match value {
        AttrValue::Str(value) => Some(TraceqlAttrValue::Str(value)),
        AttrValue::Int(value) => Some(TraceqlAttrValue::Int(value)),
        AttrValue::Double(value) => Some(TraceqlAttrValue::Float(value)),
        AttrValue::Bool(value) => Some(TraceqlAttrValue::Bool(value)),
        AttrValue::Bytes(_) => None,
    }
}

fn resource_attr<'a>(span: &'a Span, key: &str) -> Option<&'a str> {
    span.resource_attrs
        .iter()
        .find_map(|attr| match &attr.value {
            AttrValue::Str(value) if attr.key == key => Some(value.as_str()),
            _ => None,
        })
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
    // Set by //bazel/defs.bzl; see the note above.
    let tag = std::env::var("KRABKA_GRAFANA_IMAGE_TAG").expect(
        "KRABKA_GRAFANA_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under \
         cargo, set it to that image's tag in //bazel/images/images.bzl.",
    );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/grafana".to_string(), tag)
            .with_exposed_port(GRAFANA_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::seconds(5))
            .with_env_var("GF_SECURITY_ADMIN_PASSWORD", "admin")
            .with_host(DOCKER_HOST_ALIAS, Host::HostGateway)
            .start(),
    )
    .await??)
}

async fn mapped_base_url(
    container: &testcontainers::ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
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
    client: &reqwest::Client,
    oracle_base: &str,
    krabka_base: &str,
    query_range: &str,
    expression: String,
    expected: &BTreeMap<String, Vec<String>>,
) -> TestResult<Option<String>> {
    let query = format!("{{ {expression} }}");
    let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
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
    client: &reqwest::Client,
    oracle_base: &str,
    krabka_base: &str,
    query_range: &str,
    expression: String,
    output: &std::path::Path,
    observations: &Mutex<Vec<JsonValue>>,
) -> TestResult<Option<String>> {
    let query = format!("{{ {expression} }}");
    let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
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
        let status = response.status().as_u16();
        let body = response.text().await?;
        let classification = traceql_query_rejection_kind(status, &body);
        rejected &= classification.is_some();
        responses.push(
            json!({"implementation": implementation, "http_status": status,
            "classification": classification, "body": body}),
        );
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
    assert2::assert!(
        status == ReqwestStatusCode::OK,
        "{url}: {status}: {}",
        String::from_utf8_lossy(&body)
    );
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
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = JsonValue::Null;
    while Instant::now() < deadline {
        let json = get_json(client, url, tenant).await?;
        if json["traces"]
            .as_array()
            .is_some_and(|traces| !traces.is_empty())
        {
            return Ok(json);
        }
        last = json;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("timed out waiting for non-empty traces from {url}: {last}").into())
}

async fn get_json_until_positive_metric_total(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
) -> TestResult<JsonValue> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = JsonValue::Null;
    while Instant::now() < deadline {
        let json = get_json(client, url, tenant).await?;
        if metric_points_total(&json) > 0.0 {
            return Ok(json);
        }
        last = json;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("timed out waiting for positive metric total from {url}: {last}").into())
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
    let found = search["traces"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|trace| trace["spanSets"].as_array().into_iter().flatten())
        .flat_map(|span_set| span_set["spans"].as_array().into_iter().flatten())
        .any(|span| span["spanID"].as_str() == Some(span_id));
    assert2::assert!(found);
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
    TracesData {
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
    }
    .encode_to_vec()
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

fn string_kv(key: &str, value: &str) -> OtlpKeyValue {
    OtlpKeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..OtlpKeyValue::default()
    }
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

async fn compare_live_typed_groups(
    client: &reqwest::Client,
    oracle: &str,
    candidate: &str,
    range: &str,
) -> TestResult {
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
        let encoded: String = url::form_urlencoded::byte_serialize(query.as_bytes()).collect();
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
