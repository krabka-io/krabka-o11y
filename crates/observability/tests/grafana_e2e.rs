//! Grafana-mediated differential probe: Krabka and real Loki, read the way
//! Grafana reads them.
//!
//! `loki_differential` asks Krabka and Loki the same question over plain HTTP.
//! This suite asks it through Grafana instead. Both backends are provisioned
//! as Loki datasources on one Grafana, the same corpus runs through the
//! datasource proxy against each, and the two answers are compared. What that
//! adds over the plain differential is the layer between: Grafana rewrites the
//! request, applies the datasource headers, and reads the answer back, so a
//! field Krabka names or types differently is caught here even when the raw
//! JSON compared equal.
//!
//! `krabka-traces` runs the same shape of suite for Tempo
//! (`crates/traces/tests/grafana_e2e.rs`).
//!
//! One test, not three. A Grafana start and a Loki start cost tens of seconds
//! each, and a differential is most useful when it reports every case that
//! disagrees rather than stopping at the first. Each case is compared, the
//! disagreements are collected, and the assertion at the end prints all of
//! them.
//!
//! Cargo ignores this test by default, because it runs
//! `mirror.gcr.io/grafana/grafana` and `mirror.gcr.io/grafana/loki` under
//! Docker. Run with:
//!
//! `cargo test -p krabka-observability --test grafana_e2e -- --ignored --nocapture`

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use assert2::assert;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use krabka_blockstore::{LabelIndex, LogBlockIndex as BlockIndex};
use krabka_observability::{InMemoryWalSink, QuerierState, distributor_router, loki_router};
use serde_json::{Value, json};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::{net::TcpListener, sync::oneshot};
use tower::ServiceExt as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// How long a component gets to report itself ready.
const READY_TIMEOUT: Duration = Duration::from_mins(2);

/// Grafana's default HTTP port.
const GRAFANA_PORT: u16 = 3000;

/// Loki's HTTP port in single-binary mode.
const LOKI_PORT: u16 = 3100;

/// The tenant both datasources send on every request.
const TENANT: &str = "tenant-a";

/// The UID of the datasource that points at Krabka.
const KRABKA_UID: &str = "krabka-loki";

/// The UID of the datasource that points at real Loki.
const LOKI_UID: &str = "real-loki";

/// The step the corpus evaluates every metric query on.
const STEP_SECS: i64 = 15;

/// Cases where Krabka and Loki disagree, each with the reason.
///
/// A listed case still runs, and its disagreement is what is expected of it:
/// the suite fails when a listed case *starts* agreeing, so an entry cannot
/// quietly outlive the divergence it describes.
///
/// Do not loosen `normalize()` to make a case pass. It drops the volatile
/// members of the answer and nothing else.
const KNOWN_DIVERGENCE: &[Divergence] = &[Divergence {
    case: "label_join",
    reason: "Krabka accepts the PromQL function `label_join` in a LogQL query and answers it. \
             Loki rejects it with 400, because `label_join` is not a LogQL function. Krabka is a \
             superset here.",
}];

/// Single-binary Loki, configured for one tenant on the local filesystem.
///
/// The discovery limits (`discover_service_name`, `discover_log_levels`) stay
/// at Loki's defaults on purpose: Krabka implements both, so leaving them on
/// is the stronger test.
const LOKI_CONFIG: &str = r"
auth_enabled: true

server:
  http_listen_port: 3100
  grpc_listen_port: 9096
  log_level: warn

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

limits_config:
  allow_structured_metadata: true
  volume_enabled: true

analytics:
  reporting_enabled: false
";

/// Both datasources, with `{KRABKA_PORT}` and `{LOKI_PORT}` replaced at run
/// time.
///
/// `httpHeaderName1` and `httpHeaderValue1` make Grafana send `X-Scope-OrgID`
/// on every call. Loki runs with `auth_enabled: true` and Krabka keys its
/// storage by that header, so both sides need it and both get the same value.
const DATASOURCES_YAML_TEMPLATE: &str = r"apiVersion: 1
datasources:
  - name: Krabka
    type: loki
    access: proxy
    uid: krabka-loki
    url: http://host.docker.internal:{KRABKA_PORT}
    editable: false
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: tenant-a
  - name: Loki
    type: loki
    access: proxy
    uid: real-loki
    url: http://host.docker.internal:{LOKI_PORT}
    editable: false
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: tenant-a
";

#[derive(Clone, Copy, Debug)]
struct Divergence {
    case: &'static str,
    reason: &'static str,
}

/// One comparison: a request made identically to both datasources.
struct Case {
    name: &'static str,
    /// The path below `/loki/api/v1/`.
    path: String,
    params: Vec<(&'static str, String)>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana and grafana/loki images"]
async fn grafana_reads_the_same_answer_from_krabka_and_from_loki() -> TestResult {
    let client = reqwest::Client::new();
    let timeline = Timeline::new()?;
    let payload = dataset(&timeline);

    let loki = start_loki().await?;
    let loki_port = loki.get_host_port_ipv4(LOKI_PORT.tcp()).await?;
    wait_for_http_ok(&client, &format!("http://127.0.0.1:{loki_port}"), "/ready").await?;
    push_to_loki(&client, loki_port, &payload).await?;

    let krabka = start_krabka(&payload).await?;

    let datasources = DATASOURCES_YAML_TEMPLATE
        .replace("{KRABKA_PORT}", &krabka.host_port.to_string())
        .replace("{LOKI_PORT}", &loki_port.to_string());
    let grafana = start_grafana(&datasources).await?;
    let base = format!(
        "http://127.0.0.1:{}",
        grafana.get_host_port_ipv4(GRAFANA_PORT.tcp()).await?
    );
    wait_for_http_ok(&client, &base, "/api/health").await?;
    for uid in [KRABKA_UID, LOKI_UID] {
        wait_for_datasource(&client, &base, uid).await?;
    }
    // Loki acknowledges a push before the entry is queryable, so the corpus
    // waits for the data rather than for the process.
    wait_for_seeded(&client, &base, &timeline).await?;

    let mut differences = Vec::new();
    let mut healed = Vec::new();
    let cases = corpus(&timeline);
    let total = cases.len();
    for case in cases {
        let krabka_answer = probe(&client, &base, KRABKA_UID, &case).await?;
        let loki_answer = probe(&client, &base, LOKI_UID, &case).await?;
        let agrees = krabka_answer == loki_answer;
        match known_divergence(case.name) {
            Some(divergence) if agrees => healed.push(divergence),
            Some(_) => {}
            None if agrees => {}
            None => differences.push(report(&case, &krabka_answer, &loki_answer)),
        }
    }

    krabka.shutdown();
    println!(
        "grafana_e2e: {total} case(s); {} agreed with Loki, {} are recorded divergences",
        total - KNOWN_DIVERGENCE.len(),
        KNOWN_DIVERGENCE.len(),
    );
    assert!(
        differences.is_empty() && healed.is_empty(),
        "{}",
        summary(&differences, &healed)
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The corpus.
// ---------------------------------------------------------------------------

fn corpus(timeline: &Timeline) -> Vec<Case> {
    let mut cases = Vec::new();
    cases.extend(range_cases(timeline, LOG_QUERIES));
    cases.extend(range_cases(timeline, METRIC_QUERIES));
    cases.extend(metadata_cases(timeline));
    cases
}

/// Log queries: selectors, line filters, parsers, label filters and
/// formatters.
const LOG_QUERIES: &[(&str, &str)] = &[
    ("selector_eq", r#"{app="api"}"#),
    ("selector_neq", r#"{app!="api",env="prod"}"#),
    ("selector_re", r#"{app=~"a.*"}"#),
    ("selector_nre", r#"{app!~"db",env="prod"}"#),
    ("line_contains", r#"{app="api"} |= "error""#),
    ("line_not_contains", r#"{app="api"} != "info""#),
    ("line_regex", r#"{app="api"} |~ "50\\d""#),
    ("line_not_regex", r#"{app="api"} !~ "200""#),
    ("multi_line_filter", r#"{app="api"} |= "error" |~ "50\\d""#),
    ("json_parser", r#"{app="api"} | json"#),
    (
        "json_field_filter_numeric",
        r#"{app="api"} | json | status >= 500"#,
    ),
    (
        "json_field_filter_equal",
        r#"{app="api"} | json | method="POST""#,
    ),
    (
        "logfmt_field_filter_duration",
        r#"{app="web"} | logfmt | duration > 500ms"#,
    ),
    ("logfmt_parser", r#"{app="web"} | logfmt"#),
    (
        "logfmt_field_filter",
        r#"{app="web"} | logfmt | status="502""#,
    ),
    (
        "regexp_parser",
        r#"{app="db"} | regexp "(?P<lvl>[A-Z]+) (?P<rest>.+)""#,
    ),
    ("keep_labels", r#"{app="api"} | json | keep method, status"#),
    ("drop_labels", r#"{app="api"} | json | drop method"#),
    (
        "label_filter_and",
        r#"{app="api"} | json | status>=500 and method="POST""#,
    ),
    (
        "label_filter_or",
        r#"{app="api"} | json | status=404 or status=503"#,
    ),
    (
        "line_format",
        r#"{app="api"} | json | line_format "{{.method}} {{.status}}""#,
    ),
    (
        "label_format_rename",
        r#"{app="api"} | label_format service=app"#,
    ),
    (
        "label_format_template",
        r#"{app="api"} | label_format combo="{{.app}}-{{.env}}""#,
    ),
];

/// Metric queries: range aggregations, vector aggregations, unwrap,
/// binary operators, label functions and `offset`.
const METRIC_QUERIES: &[(&str, &str)] = &[
    ("count_over_time", r#"count_over_time({app="api"}[5m])"#),
    (
        "count_over_time_filtered",
        r#"count_over_time({app="api"} |= "error" [5m])"#,
    ),
    ("rate", r#"rate({app="api"}[5m])"#),
    ("bytes_over_time", r#"bytes_over_time({app="api"}[5m])"#),
    ("bytes_rate", r#"bytes_rate({app="api"}[5m])"#),
    (
        "sum_by",
        r#"sum by (app) (count_over_time({app=~".+"}[5m]))"#,
    ),
    (
        "sum_without",
        r#"sum without (env) (count_over_time({app="api"}[5m]))"#,
    ),
    ("avg", r#"avg(count_over_time({app=~".+"}[5m]))"#),
    ("max", r#"max(count_over_time({app=~".+"}[5m]))"#),
    ("min", r#"min(count_over_time({app=~".+"}[5m]))"#),
    ("count_agg", r#"count(count_over_time({app=~".+"}[5m]))"#),
    ("topk", r#"topk(2, count_over_time({app=~".+"}[5m]))"#),
    ("bottomk", r#"bottomk(1, count_over_time({app=~".+"}[5m]))"#),
    (
        "quantile_over_time",
        r#"quantile_over_time(0.95, {app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "avg_over_time_unwrap",
        r#"avg_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "max_over_time_unwrap",
        r#"max_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "min_over_time_unwrap",
        r#"min_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "first_over_time_unwrap",
        r#"first_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "last_over_time_unwrap",
        r#"last_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "sum_over_time_unwrap",
        r#"sum_over_time({app="api"} | json | unwrap resp_bytes [5m])"#,
    ),
    (
        "stddev_over_time_unwrap",
        r#"stddev_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "absent_over_time_present",
        r#"absent_over_time({app="api"}[5m])"#,
    ),
    (
        "absent_over_time_missing",
        r#"absent_over_time({app="nope",env="prod"}[5m])"#,
    ),
    ("vector_scalar", r"vector(1)"),
    (
        "binary_scalar_div",
        r#"sum(count_over_time({app="api"}[5m])) / 60"#,
    ),
    (
        "binary_scalar_mul",
        r#"100 * sum(count_over_time({app="api"}[5m]))"#,
    ),
    (
        "binary_compare_bool",
        r#"count_over_time({app="api"}[5m]) > bool 1"#,
    ),
    (
        "label_replace",
        r#"label_replace(count_over_time({app="api"}[5m]), "svc", "$1", "app", "(.*)")"#,
    ),
    (
        "label_join",
        r#"label_join(count_over_time({app="api"}[5m]), "combo", "-", "app", "env")"#,
    ),
    ("offset", r#"count_over_time({app="api"}[5m] offset 1m)"#),
];

fn range_cases(timeline: &Timeline, queries: &[(&'static str, &'static str)]) -> Vec<Case> {
    queries
        .iter()
        .copied()
        .map(|(name, logql)| Case {
            name,
            path: "query_range".to_string(),
            params: vec![
                ("query", logql.to_string()),
                ("start", timeline.start_ns.to_string()),
                ("end", timeline.end_ns.to_string()),
                ("step", STEP_SECS.to_string()),
                ("direction", "forward".to_string()),
                ("limit", "5000".to_string()),
            ],
        })
        .collect()
}

/// The label and series endpoints Grafana's log browser drives.
fn metadata_cases(timeline: &Timeline) -> Vec<Case> {
    let window = || {
        vec![
            ("start", timeline.start_ns.to_string()),
            ("end", timeline.end_ns.to_string()),
        ]
    };
    let mut cases = vec![
        Case {
            name: "labels_endpoint",
            path: "labels".to_string(),
            params: window(),
        },
        Case {
            name: "series_endpoint",
            path: "series".to_string(),
            params: {
                let mut params = window();
                params.push(("match[]", r#"{app=~".+"}"#.to_string()));
                params
            },
        },
    ];
    for (name, label) in [
        ("label_values_app", "app"),
        ("label_values_env", "env"),
        ("label_values_service_name", "service_name"),
    ] {
        cases.push(Case {
            name,
            path: format!("label/{label}/values"),
            params: window(),
        });
    }
    cases
}

/// The shared dataset, pushed into Krabka and into Loki unchanged.
///
/// Three streams over about a minute. The lines carry JSON and logfmt bodies
/// with numeric, duration and byte fields, and clear level tokens, so both
/// backends derive the same `detected_level` and `service_name`.
///
/// Every level token is `INFO`, `ERROR` or `WARN`. `DEBUG` is left out on
/// purpose: Loki reads a leading `DEBUG` in a plain-text line as
/// `detected_level: unknown` while Krabka reads it as `debug`, which would
/// split every case on the `db` stream rather than the one case that asks
/// about levels.
fn dataset(timeline: &Timeline) -> Value {
    let at = |offset_secs: i64| timeline.at(offset_secs).to_string();
    json!({
        "streams": [
            {
                "stream": { "app": "api", "env": "prod" },
                "values": [
                    [at(0), r#"{"level":"info","status":200,"method":"GET","latency_ms":12,"resp_bytes":512,"remote_addr":"10.0.0.5"}"#],
                    [at(10), r#"{"level":"error","status":500,"method":"POST","latency_ms":250,"resp_bytes":1048576,"remote_addr":"10.0.0.6"}"#],
                    [at(20), r#"{"level":"warn","status":404,"method":"GET","latency_ms":35,"resp_bytes":256,"remote_addr":"192.168.1.20"}"#],
                    [at(30), r#"{"level":"error","status":503,"method":"POST","latency_ms":480,"resp_bytes":2048,"remote_addr":"10.0.0.7"}"#],
                    [at(40), r#"{"level":"info","status":200,"method":"PUT","latency_ms":18,"resp_bytes":900,"remote_addr":"10.0.0.8"}"#],
                    [at(50), r#"{"level":"info","status":201,"method":"POST","latency_ms":22,"resp_bytes":700,"remote_addr":"10.0.0.9"}"#],
                ],
            },
            {
                "stream": { "app": "web", "env": "prod" },
                "values": [
                    [at(5), r#"level=info msg="served" status=200 duration=8ms"#],
                    [at(15), r#"level=error msg="upstream timeout" status=502 duration=1200ms"#],
                    [at(25), r#"level=warn msg="slow" status=200 duration=900ms"#],
                    [at(45), r#"level=info msg="served" status=200 duration=11ms"#],
                ],
            },
            {
                "stream": { "app": "db", "env": "staging" },
                "values": [
                    [at(8), "INFO connection established"],
                    [at(28), "ERROR deadlock detected on shard 3"],
                    [at(48), "INFO vacuum complete"],
                ],
            },
        ],
    })
}

// ---------------------------------------------------------------------------
// Asking, and comparing.
// ---------------------------------------------------------------------------

/// Runs one case against one datasource, through Grafana's proxy.
///
/// An error answer is normalised into a marker rather than raised, so the
/// suite compares a Krabka error against a Loki error as one more case.
async fn probe(client: &reqwest::Client, base: &str, uid: &str, case: &Case) -> TestResult<Value> {
    let url = format!(
        "{base}/api/datasources/proxy/uid/{uid}/loki/api/v1/{}?{}",
        case.path,
        query_string(&case.params)
    );
    let response = client.get(url).send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        return Ok(json!({ "status": status.as_u16() }));
    }
    Ok(match serde_json::from_str::<Value>(&text) {
        Ok(body) => normalize(&body),
        Err(_) => json!({ "non_json": text }),
    })
}

/// Encodes one query string from its pairs.
///
/// `reqwest` is built here without its `query` feature, which is what
/// `RequestBuilder::query` needs, so the pairs are encoded the way
/// `krabka-metrics-service`'s Grafana suite encodes its own.
fn query_string(pairs: &[(&str, String)]) -> String {
    pairs
        .iter()
        .map(|(name, value)| format!("{}={}", form_encode(name), form_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn form_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// Strips the members of an answer that are not a property of the query.
///
/// `stats` goes: every member of it is a byte count, a duration or a chunk
/// count measured on one side's own storage layout. Result order goes, because
/// neither side promises one. Float text goes to six decimal places, because
/// the two sides format the same value with different precision.
fn normalize(body: &Value) -> Value {
    let data = &body["data"];
    let result_type = data["resultType"].as_str().unwrap_or_default();
    let Some(result) = data.get("result").and_then(Value::as_array) else {
        // The metadata endpoints answer a bare array: label names, label
        // values, or series label sets. `serde_json` orders an object's
        // members, so the text of an item is already canonical.
        let mut items: Vec<String> = data.as_array().map_or_else(Vec::new, |items| {
            items.iter().map(ToString::to_string).collect()
        });
        items.sort();
        return json!({ "data": items });
    };

    let mut entries: Vec<(String, Value)> = result
        .iter()
        .map(|item| {
            let is_stream = result_type == "streams";
            let label_key = if is_stream { "stream" } else { "metric" };
            let labels = canonical_labels(&item[label_key]);
            let mut values: Vec<Value> = item["values"]
                .as_array()
                .cloned()
                .or_else(|| item.get("value").map(|value| vec![value.clone()]))
                .unwrap_or_default();
            values.sort_by_key(ToString::to_string);
            if !is_stream {
                values = values.iter().map(round_sample).collect();
            }
            let mut entry = serde_json::Map::new();
            entry.insert(label_key.to_string(), item[label_key].clone());
            entry.insert("values".to_string(), Value::Array(values));
            (labels, Value::Object(entry))
        })
        .collect();
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    json!({
        "resultType": result_type,
        "result": entries.into_iter().map(|(_, item)| item).collect::<Vec<_>>(),
    })
}

/// A label set as one sorted string, so it can order a result list.
fn canonical_labels(labels: &Value) -> String {
    let ordered: BTreeMap<&str, &str> = labels
        .as_object()
        .map(|members| {
            members
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str().unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default();
    format!("{ordered:?}")
}

/// One `[timestamp, value]` sample, with the value rounded to six places.
fn round_sample(sample: &Value) -> Value {
    let rounded = sample[1]
        .as_str()
        .and_then(|text| text.parse::<f64>().ok())
        .map_or_else(|| sample[1].to_string(), |number| format!("{number:.6}"));
    json!([sample[0], rounded])
}

fn known_divergence(case: &str) -> Option<Divergence> {
    KNOWN_DIVERGENCE
        .iter()
        .find(|divergence| divergence.case == case)
        .copied()
}

fn report(case: &Case, krabka: &Value, loki: &Value) -> String {
    let query = case
        .params
        .iter()
        .find(|(name, _)| *name == "query" || *name == "match[]")
        .map_or_else(|| case.path.clone(), |(_, value)| value.clone());
    format!(
        "\n--- {} ---\n  ask:    {query}\n  krabka: {}\n  loki:   {}\n",
        case.name,
        pretty(krabka),
        pretty(loki),
    )
}

fn pretty(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unprintable>".to_string())
}

fn summary(differences: &[String], healed: &[Divergence]) -> String {
    let mut out = String::new();
    if !differences.is_empty() {
        let _ = write!(
            out,
            "{} case(s) disagree with Loki:\n{}",
            differences.len(),
            differences.concat()
        );
    }
    for divergence in healed {
        let _ = write!(
            out,
            "\n`{}` is listed as a known divergence and now agrees with Loki. Remove its entry \
             from KNOWN_DIVERGENCE. The entry said: {}\n",
            divergence.case, divergence.reason,
        );
    }
    out
}

// ---------------------------------------------------------------------------
// The two containers, and the Krabka process.
// ---------------------------------------------------------------------------

async fn start_loki() -> TestResult<ContainerAsync<GenericImage>> {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagreed
    // testcontainers pulled the image over the network and the suite compared
    // against whatever it got rather than against the pinned bytes.
    let tag = std::env::var("KRABKA_LOKI_IMAGE_TAG").expect(
        "KRABKA_LOKI_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under cargo, set it to \
         that image's tag in //bazel/images/images.bzl.",
    );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/loki".to_string(), tag)
            .with_exposed_port(LOKI_PORT.tcp())
            // No log-line wait condition: this config sets `log_level: warn`,
            // and a healthy single-binary Loki says nothing at that level at
            // all. `/ready` is polled instead.
            .with_copy_to(
                "/etc/loki/local-config.yaml",
                LOKI_CONFIG.as_bytes().to_vec(),
            )
            .start(),
    )
    .await??)
}

async fn start_grafana(datasources_yaml: &str) -> TestResult<ContainerAsync<GenericImage>> {
    let tag = std::env::var("KRABKA_GRAFANA_IMAGE_TAG").expect(
        "KRABKA_GRAFANA_IMAGE_TAG is unset. These suites run under `bazel test --config=docker`, \
         which loads the digest-pinned image and sets this. To run one under cargo, set it to \
         that image's tag in //bazel/images/images.bzl.",
    );
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/grafana".to_string(), tag)
            .with_exposed_port(GRAFANA_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("HTTP Server Listen"))
            .with_copy_to(
                "/etc/grafana/provisioning/datasources/krabka.yaml",
                datasources_yaml.as_bytes().to_vec(),
            )
            // Both backends run on the host: Loki on its mapped port, Krabka
            // in this process.
            .with_host("host.docker.internal", Host::HostGateway)
            .with_env_var("GF_AUTH_ANONYMOUS_ENABLED", "true")
            .with_env_var("GF_AUTH_ANONYMOUS_ORG_ROLE", "Admin")
            .with_env_var("GF_AUTH_BASIC_ENABLED", "false")
            .start(),
    )
    .await??)
}

async fn push_to_loki(client: &reqwest::Client, port: u16, payload: &Value) -> TestResult {
    let response = client
        .post(format!("http://127.0.0.1:{port}/loki/api/v1/push"))
        .header("X-Scope-OrgID", TENANT)
        .json(payload)
        .send()
        .await?;
    assert!(response.status() == reqwest::StatusCode::NO_CONTENT);
    Ok(())
}

struct KrabkaServer {
    /// The host port the Grafana container dials through
    /// `host.docker.internal`.
    host_port: u16,
    shutdown: oneshot::Sender<()>,
}

impl KrabkaServer {
    fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

/// Seeds the querier through the real push door and serves it on the host.
///
/// The push goes through `distributor_router`, not straight into the sink, so
/// Krabka derives `detected_level` and `service_name` the way Loki's own
/// discovery does. The bind address is `0.0.0.0`, not `127.0.0.1`: the
/// container reaches this process over the host gateway, and a loopback-only
/// listener refuses that connection.
async fn start_krabka(payload: &Value) -> TestResult<KrabkaServer> {
    let sink = InMemoryWalSink::default();
    let response = distributor_router(sink.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("content-type", "application/json")
                .header("X-Scope-OrgID", TENANT)
                .body(Body::from(payload.to_string()))?,
        )
        .await?;
    assert!(response.status() == StatusCode::NO_CONTENT);

    // `i64::MIN`: nothing has been compacted, so every record the distributor
    // wrote is in the querier's hot tail.
    let root = tempfile::tempdir()?.keep();
    let state = QuerierState::new(root, LabelIndex::default(), BlockIndex::default())
        .with_hot_tail(sink, i64::MIN);

    let listener = TcpListener::bind(("0.0.0.0", 0)).await?;
    let host_port = listener.local_addr()?.port();
    let (shutdown, stop) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, loki_router(state))
            .with_graceful_shutdown(async move {
                let _ = stop.await;
            })
            .await;
    });
    Ok(KrabkaServer {
        host_port,
        shutdown,
    })
}

// ---------------------------------------------------------------------------
// Waiting.
// ---------------------------------------------------------------------------

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, path: &str) -> TestResult {
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        if client
            .get(format!("{base}{path}"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("{base}{path} did not become ready").into())
}

async fn wait_for_datasource(client: &reqwest::Client, base: &str, uid: &str) -> TestResult {
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        if client
            .get(format!("{base}/api/datasources/uid/{uid}"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("datasource {uid} was not provisioned on {base}").into())
}

/// Waits until both datasources answer the whole corpus window with data.
async fn wait_for_seeded(client: &reqwest::Client, base: &str, timeline: &Timeline) -> TestResult {
    let case = Case {
        name: "seed probe",
        path: "query_range".to_string(),
        params: vec![
            ("query", r#"{app=~".+"}"#.to_string()),
            ("start", timeline.start_ns.to_string()),
            ("end", timeline.end_ns.to_string()),
            ("step", STEP_SECS.to_string()),
            ("direction", "forward".to_string()),
        ],
    };
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        let mut seeded = true;
        for uid in [KRABKA_UID, LOKI_UID] {
            let answer = probe(client, base, uid, &case).await?;
            // Three pushed streams, and no fewer than three in the answer.
            // Not exactly three: the default encoding folds `detected_level`
            // into the stream labels, so a pushed stream whose lines carry
            // two levels comes back as two streams.
            seeded &= answer["result"]
                .as_array()
                .is_some_and(|result| result.len() >= 3);
        }
        if seeded {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err("the corpus did not become queryable on both datasources".into())
}

// ---------------------------------------------------------------------------
// Time.
// ---------------------------------------------------------------------------

/// The corpus clock.
///
/// The window is aligned to a multiple of the step. Loki rounds an unaligned
/// window up to the next multiple of the step and evaluates on that grid;
/// Krabka steps from the raw `start`. With an aligned window the two grids
/// coincide, so the suite compares absolute timestamps and still catches a
/// genuine bucketing fault.
struct Timeline {
    base_ns: i64,
    start_ns: i64,
    end_ns: i64,
}

impl Timeline {
    fn new() -> TestResult<Self> {
        let now_secs = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())?;
        // Three minutes back, so the whole window is inside Loki's ingestion
        // window and none of it is in the future.
        let base_secs = now_secs - 180;
        let start_secs = base_secs - 60;
        let end_secs = base_secs + 180;
        Ok(Self {
            base_ns: base_secs * 1_000_000_000,
            start_ns: (start_secs - start_secs.rem_euclid(STEP_SECS)) * 1_000_000_000,
            end_ns: (end_secs + (STEP_SECS - end_secs.rem_euclid(STEP_SECS)) % STEP_SECS)
                * 1_000_000_000,
        })
    }

    /// The epoch nanosecond `offset_secs` after the base time.
    fn at(&self, offset_secs: i64) -> i64 {
        self.base_ns + offset_secs * 1_000_000_000
    }
}
