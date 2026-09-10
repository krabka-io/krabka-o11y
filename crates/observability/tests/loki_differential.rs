//! Docker-backed differential probe against real Grafana Loki.
//!
//! Metrics, traces and profiles each check themselves against the upstream
//! component in a container -- `diff_mimir`, `diff_prometheus`,
//! `tempo_differential`, `pyroscope_differential`. The logs path had no such
//! check, even though it is the largest of the four: a Loki push receiver that
//! speaks JSON, snappy-protobuf and OTLP, structured metadata, and a `LogQL`
//! engine. Everything about it was checked only against this project's own
//! expectations.
//!
//! The structure follows `diff_prometheus.rs` and `diff_mimir.rs`: one corpus
//! written to both a Loki container and this crate's own router, then the same
//! queries run against both, then a comparison that removes only the fields
//! that are legitimately volatile.
//!
//! Cargo ignores this test by default, because it runs
//! `mirror.gcr.io/grafana/loki` under Docker. Run with:
//!
//! `cargo test -p krabka-observability --test loki_differential -- --ignored --nocapture`
//!
//! One test, not many: a container start costs tens of seconds, and a
//! differential is most useful when it reports *every* case that disagrees
//! rather than stopping at the first. Each case is compared, the disagreements
//! are collected, and the assertion at the end prints all of them.

use std::{
    fmt::Write as _,
    io::Write as _,
    time::{Duration, Instant},
};

use assert2::assert;
use krabka_blockstore::{LabelIndex, LogBlockIndex as BlockIndex};
use krabka_observability::{InMemoryWalSink, QuerierState, distributor_router, loki_router};
use prost::Message as _;
use serde_json::{Value, json};
use support::{
    LokiProtoEntry, LokiProtoLabelPair, LokiProtoPushRequest, LokiProtoStream, LokiProtoTimestamp,
};
use testcontainers::{GenericImage, ImageExt, core::IntoContainerPort, runners::AsyncRunner};
use tokio::{net::TcpListener, sync::oneshot};

mod support;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

/// Fixed tenant header used on both sides.
///
/// `auth_enabled: true` below makes Loki require `X-Scope-OrgID` on every push
/// and query, which is the header Krabka's distributor and querier key on too.
/// Leaving Loki's default of `auth_enabled: false` would have let the two sides
/// disagree about tenancy without the corpus noticing.
const TENANT: &str = "compliance";

/// Loki's HTTP port in single-binary mode.
const LOKI_PORT: u16 = 3100;

/// Cases where Krabka and Loki disagree, each with the reason.
///
/// A listed case still runs, and its disagreement is what is expected of it:
/// the suite fails when a listed case *starts* agreeing, so an entry cannot
/// quietly outlive the divergence it describes. Each reason names the
/// behaviour, not the symptom, so that removing the entry is the same work as
/// reading it.
///
/// Do not loosen `normalize()` to make a case pass. The only field it drops is
/// `stats`, whose every member is a byte count, a duration or a chunk count
/// measured on one side's own storage layout.
const LOKI_KNOWN_DIVERGENCE: &[Divergence] = &[
    Divergence {
        case: "instant_selector_api_stream",
        reason: "Loki refuses a log selector on `/query` outright -- 400, \"log queries are not \
                 supported as an instant query type\". Krabka answers it, with an empty stream \
                 list.",
    },
    Divergence {
        case: "instant_selector_worker_stream",
        reason: "Loki refuses a log selector on `/query`, as `instant_selector_api_stream`.",
    },
    Divergence {
        case: "topk_over_vector_aggregation",
        reason: "Krabka's LogQL parser rejects a vector aggregation nested inside `topk` -- \
                 `topk(1, sum by (app) (...))` is a parse error, while `topk` over a bare range \
                 aggregation parses. Loki accepts both.",
    },
    Divergence {
        case: "instant_topk_count_over_time",
        reason: "the nested-`topk` parse gap, as `topk_over_vector_aggregation`.",
    },
    Divergence {
        case: "unwrap_label_named_after_a_conversion",
        reason: "Krabka's parser reads `unwrap duration` as the start of the `duration(...)` \
                 conversion and rejects the query when no `(` follows. Loki reads a bare \
                 identifier as the label of that name, so `unwrap duration` unwraps the label \
                 called `duration`.",
    },
    Divergence {
        case: "format_query_unwrap",
        reason: "the `unwrap duration` parse gap, as \
                 `unwrap_label_named_after_a_conversion`; `/format_query` reaches the same \
                 parser.",
    },
    Divergence {
        case: "format_query_selector",
        reason: "matcher separator. Loki's formatter writes `{app=\"api\", env=\"prod\"}` with a \
                 space after the comma; Krabka writes it without one. Both re-parse; only the \
                 text differs.",
    },
    Divergence {
        case: "count_over_time_unaligned_window",
        reason: "step alignment. Given a window whose bounds are not multiples of the step, Loki \
                 rounds both bounds up to the next multiple and evaluates on that grid; Krabka \
                 steps from the raw `start`. Every point of the answer is therefore at a \
                 different timestamp.",
    },
];

/// The request header that asks for the `categorize-labels` encoding.
///
/// Loki has two JSON encodings for a `streams` answer, and the corpus asks for
/// both. The default folds an entry's structured metadata and its parsed labels
/// into the stream's label map and leaves the entry two elements long, so one
/// pushed stream comes back as one stream per distinct metadata value. This
/// header switches to the other: the stream keeps only its own labels, and each
/// entry grows a third element -- `{"structuredMetadata": {...}, "parsed":
/// {...}}`, an envelope rather than a bare map -- which is also where
/// `detected_level` shows up, since Loki's level discovery writes it as
/// structured metadata.
const CATEGORIZE_LABELS: &str = "categorize-labels";

/// Single-binary Loki, configured for one tenant on the local filesystem.
///
/// The discovery limits (`discover_service_name`, `discover_log_levels`) stay
/// at Loki's defaults on purpose: Krabka implements both, so leaving them on is
/// the stronger test.
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
  # Structured metadata is half of what this suite is here to compare, and Loki
  # refuses a push carrying it unless this is on.
  allow_structured_metadata: true
  volume_enabled: true

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
    let client = reqwest::Client::new();
    let loki = start_loki().await?;
    let loki_base = mapped_base_url(&loki, LOKI_PORT).await?;
    wait_for_ready(&client, &loki_base).await?;

    let krabka = start_krabka().await?;
    let timeline = Timeline::new()?;

    let json_body = json_streams(&timeline, JSON_STREAMS);
    let gzip_body = json_streams(&timeline, GZIP_STREAMS);
    let proto_body = proto_push_body(&timeline)?;
    for base in [loki_base.as_str(), krabka.push_url.as_str()] {
        push_json(&client, base, &json_body).await?;
        push_gzip_json(&client, base, &gzip_body).await?;
        push_proto(&client, base, &proto_body).await?;
    }
    wait_for_seeded(&client, &loki_base, &timeline).await?;
    wait_for_seeded(&client, &krabka.query_url, &timeline).await?;

    let mut differences = Vec::new();
    let mut healed = Vec::new();
    let cases = corpus(&timeline);
    let total = cases.len();
    for case in cases {
        let krabka_response = probe(&client, &krabka.query_url, &case).await?;
        let loki_response = probe(&client, &loki_base, &case).await?;
        let agrees = krabka_response == loki_response;
        match known_divergence(case.name) {
            Some(divergence) if agrees => healed.push(divergence),
            Some(_) => {}
            None if agrees => {}
            None => differences.push(report(&case, &krabka_response, &loki_response)),
        }
    }

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
// The corpus.
// ---------------------------------------------------------------------------

/// A seeded stream: its labels, and the entries pushed into it.
struct SeedStream {
    labels: &'static [(&'static str, &'static str)],
    entries: &'static [SeedEntry],
}

/// One seeded entry, at `offset_secs` after the corpus base time.
struct SeedEntry {
    offset_secs: i64,
    line: &'static str,
    metadata: &'static [(&'static str, &'static str)],
}

/// Streams pushed as `application/json`, the encoding Promtail and Alloy use.
///
/// `api` carries structured metadata on every entry and JSON lines, so it feeds
/// both the `| json` cases and the metadata cases. `worker` carries neither, so
/// a case that fails on it is not failing on metadata.
const JSON_STREAMS: &[SeedStream] = &[
    SeedStream {
        labels: &[("app", "api"), ("env", "prod")],
        entries: &[
            SeedEntry {
                offset_secs: 0,
                line: r#"{"level":"info","status":200,"msg":"api ok","duration":0.25}"#,
                metadata: &[("pod", "api-1"), ("trace_id", "abc")],
            },
            SeedEntry {
                offset_secs: 10,
                line: r#"{"level":"error","status":500,"msg":"api error","duration":1.5}"#,
                metadata: &[("pod", "api-2"), ("trace_id", "def")],
            },
            SeedEntry {
                offset_secs: 20,
                line: r#"{"level":"warn","status":404,"msg":"api missing","duration":0.5}"#,
                metadata: &[("pod", "api-1"), ("trace_id", "ghi")],
            },
        ],
    },
    SeedStream {
        labels: &[("app", "worker"), ("env", "prod")],
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
        ],
    },
];

/// The stream pushed as gzipped JSON, covering the `Content-Encoding` path.
const GZIP_STREAMS: &[SeedStream] = &[SeedStream {
    labels: &[("app", "gz"), ("env", "prod")],
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
        labels: &[("app", "fmt"), ("env", "stage")],
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
        ],
    },
    SeedStream {
        labels: &[("app", "meta"), ("env", "stage")],
        entries: &[
            SeedEntry {
                offset_secs: 4,
                line: "meta first",
                metadata: &[("shard", "a")],
            },
            SeedEntry {
                offset_secs: 14,
                line: "meta second",
                metadata: &[("shard", "b")],
            },
        ],
    },
];

/// One comparison: a request made identically to both sides.
struct Case {
    name: &'static str,
    path: String,
    params: Vec<(&'static str, String)>,
    /// The `X-Loki-Response-Encoding-Flags` value the request carries, when it
    /// carries one. A case that names no flags asks the question a default
    /// client asks.
    encoding_flags: Option<&'static str>,
}

/// Every case the suite compares.
fn corpus(timeline: &Timeline) -> Vec<Case> {
    let mut cases = Vec::new();
    cases.extend(stream_cases(timeline));
    cases.extend(categorized_stream_cases(timeline));
    cases.extend(metric_cases(timeline));
    cases.extend(instant_cases(timeline));
    cases.extend(metadata_cases(timeline));
    cases.extend(format_query_cases());
    cases
}

/// `/query_range` over log selectors, line filters, parsers and formatters.
fn stream_cases(timeline: &Timeline) -> Vec<Case> {
    [
        ("selector_api_stream", r#"{app="api"}"#),
        ("selector_meta_stream", r#"{app="meta"}"#),
        ("selector_every_stream", r#"{env=~".+"}"#),
        ("selector_regex_match", r#"{app=~"worker|gz"}"#),
        ("selector_not_equal", r#"{env="prod", app!="api"}"#),
        ("selector_regex_not_match", r#"{env="prod", app!~"api|gz"}"#),
        ("line_filter_contains", r#"{app="worker"} |= "error""#),
        ("line_filter_not_contains", r#"{app="worker"} != "error""#),
        ("line_filter_regex", r#"{app="worker"} |~ "job=[0-9]""#),
        (
            "line_filter_not_regex",
            r#"{app="worker"} !~ "^worker started""#,
        ),
        ("line_filter_chained", r#"{app="gz"} |= "gz" != "second""#),
        ("json_parser", r#"{app="api"} | json"#),
        (
            "json_parser_label_filter",
            r#"{app="api"} | json | status = "500""#,
        ),
        (
            "json_parser_named_expression",
            r#"{app="api"} | json severity="level""#,
        ),
        ("logfmt_parser", r#"{app="fmt"} | logfmt"#),
        (
            "logfmt_label_filter",
            r#"{app="fmt"} | logfmt | status = "ok""#,
        ),
        (
            "logfmt_numeric_label_filter",
            r#"{app="fmt"} | logfmt | latency > 10"#,
        ),
        (
            "label_format_rename",
            r#"{app="fmt"} | logfmt | label_format svc=service"#,
        ),
        (
            "label_format_template",
            r#"{app="fmt"} | logfmt | label_format svc=`{{.service}}-{{.status}}`"#,
        ),
        (
            "line_format_template",
            r#"{app="fmt"} | logfmt | line_format `{{.service}}/{{.status}}`"#,
        ),
        (
            "structured_metadata_filter",
            r#"{app="meta"} | shard = "b""#,
        ),
    ]
    .into_iter()
    .map(|(name, logql)| Case {
        name,
        path: "/loki/api/v1/query_range".to_string(),
        params: range_params(timeline, logql, 15),
        encoding_flags: None,
    })
    .collect()
}

/// The same reads, asked for under the `categorize-labels` encoding.
///
/// Every query here reaches a stream that carries something to categorise:
/// structured metadata pushed as JSON (`api`) or as protobuf (`meta`), labels a
/// parser stage produced (`| json`, `| logfmt`), a label `label_format` wrote,
/// and -- in `worker` -- a stream whose only categorised label is the
/// `detected_level` that Loki's own discovery added.
fn categorized_stream_cases(timeline: &Timeline) -> Vec<Case> {
    [
        ("categorized_selector_api_stream", r#"{app="api"}"#),
        ("categorized_selector_meta_stream", r#"{app="meta"}"#),
        ("categorized_selector_worker_stream", r#"{app="worker"}"#),
        ("categorized_json_parser", r#"{app="api"} | json"#),
        ("categorized_logfmt_parser", r#"{app="fmt"} | logfmt"#),
        (
            "categorized_label_format_rename",
            r#"{app="fmt"} | logfmt | label_format svc=service"#,
        ),
        (
            "categorized_structured_metadata_filter",
            r#"{app="meta"} | shard = "b""#,
        ),
    ]
    .into_iter()
    .map(|(name, logql)| Case {
        name,
        path: "/loki/api/v1/query_range".to_string(),
        params: range_params(timeline, logql, 15),
        encoding_flags: Some(CATEGORIZE_LABELS),
    })
    .collect()
}

/// `/query_range` over metric queries: range aggregations, vector
/// aggregations, unwrap, and one deliberately step-unaligned window.
fn metric_cases(timeline: &Timeline) -> Vec<Case> {
    let mut cases: Vec<Case> = [
        ("count_over_time", r#"count_over_time({app="api"}[1m])"#),
        (
            "count_over_time_line_filter",
            r#"count_over_time({app="worker"} |= "error" [1m])"#,
        ),
        ("rate", r#"rate({app=~"api|worker"}[1m])"#),
        ("bytes_over_time", r#"bytes_over_time({app="worker"}[1m])"#),
        ("bytes_rate", r#"bytes_rate({app="worker"}[1m])"#),
        (
            "sum_by_count_over_time",
            r#"sum by (app) (count_over_time({app=~".+"}[1m]))"#,
        ),
        (
            "sum_without_count_over_time",
            r#"sum without (env, detected_level, service_name) (count_over_time({app=~"worker|gz"}[1m]))"#,
        ),
        ("max_by_rate", r#"max by (env) (rate({app=~".+"}[1m]))"#),
        (
            "avg_by_count_over_time",
            r#"avg by (app) (count_over_time({app=~".+"}[1m]))"#,
        ),
        (
            "topk_over_range_aggregation",
            r#"topk(1, count_over_time({app=~"worker|gz"}[1m]))"#,
        ),
        (
            "topk_over_vector_aggregation",
            r#"topk(1, sum by (app) (count_over_time({app=~".+"}[1m])))"#,
        ),
        (
            "scalar_multiply",
            r#"sum(count_over_time({app=~".+"}[1m])) * 2"#,
        ),
        (
            "binary_vector_division",
            r#"sum by (app) (count_over_time({app=~".+"}[1m])) / sum by (app) (count_over_time({app=~".+"}[2m]))"#,
        ),
        (
            "unwrap_sum_over_time",
            r#"sum_over_time({app="fmt"} | logfmt | unwrap latency [1m])"#,
        ),
        (
            "unwrap_avg_over_time_by_service",
            r#"avg_over_time({app="fmt"} | logfmt | unwrap latency [1m]) by (service)"#,
        ),
        (
            "unwrap_max_over_time",
            r#"max_over_time({app="fmt"} | logfmt | unwrap latency [1m])"#,
        ),
        (
            "unwrap_quantile_over_time",
            r#"quantile_over_time(0.9, {app="fmt"} | logfmt | unwrap latency [1m]) by (service)"#,
        ),
        (
            "unwrap_rate",
            r#"rate({app="fmt"} | logfmt | unwrap latency [1m])"#,
        ),
        (
            "unwrap_duration_conversion",
            r#"sum_over_time({app="fmt"} | logfmt | unwrap duration(took) [1m])"#,
        ),
        (
            "unwrap_label_named_after_a_conversion",
            r#"sum_over_time({app="fmt"} | logfmt | unwrap duration [1m])"#,
        ),
    ]
    .into_iter()
    .map(|(name, logql)| Case {
        name,
        path: "/loki/api/v1/query_range".to_string(),
        params: range_params(timeline, logql, 15),
        encoding_flags: None,
    })
    .collect();

    // A window whose bounds are not multiples of the step. Loki rounds both
    // bounds up to the next multiple; nothing in this repo said what Krabka
    // does, so the corpus asks.
    cases.push(Case {
        name: "count_over_time_unaligned_window",
        path: "/loki/api/v1/query_range".to_string(),
        params: vec![
            ("query", r#"count_over_time({app="api"}[1m])"#.to_string()),
            ("start", timeline.at(-7).to_string()),
            ("end", timeline.at(53).to_string()),
            ("step", "15".to_string()),
            ("direction", "forward".to_string()),
        ],
        encoding_flags: None,
    });
    cases
}

/// `/query`, which answers at a single instant rather than over a window.
fn instant_cases(timeline: &Timeline) -> Vec<Case> {
    [
        ("instant_selector_api_stream", r#"{app="api"}"#),
        ("instant_selector_worker_stream", r#"{app="worker"}"#),
        (
            "instant_sum_count_over_time",
            r#"sum(count_over_time({app=~".+"}[5m]))"#,
        ),
        (
            "instant_sum_by_count_over_time",
            r#"sum by (app) (count_over_time({app=~".+"}[5m]))"#,
        ),
        (
            "instant_topk_count_over_time",
            r#"topk(2, sum by (app) (count_over_time({app=~".+"}[5m])))"#,
        ),
        (
            "instant_unwrap_sum_over_time",
            r#"sum_over_time({app="fmt"} | logfmt | unwrap latency [5m])"#,
        ),
    ]
    .into_iter()
    .map(|(name, logql)| Case {
        name,
        path: "/loki/api/v1/query".to_string(),
        params: vec![
            ("query", logql.to_string()),
            ("time", timeline.at(60).to_string()),
            ("direction", "forward".to_string()),
        ],
        encoding_flags: None,
    })
    .collect()
}

/// The label and series endpoints Grafana's log browser drives.
fn metadata_cases(timeline: &Timeline) -> Vec<Case> {
    let window = || {
        vec![
            ("start", timeline.at(-60).to_string()),
            ("end", timeline.at(120).to_string()),
        ]
    };
    let mut cases = vec![
        Case {
            name: "labels_endpoint",
            path: "/loki/api/v1/labels".to_string(),
            params: window(),
            encoding_flags: None,
        },
        Case {
            name: "series_endpoint",
            path: "/loki/api/v1/series".to_string(),
            params: {
                let mut params = window();
                params.push(("match[]", r#"{app=~".+"}"#.to_string()));
                params
            },
            encoding_flags: None,
        },
    ];
    for (name, label) in [
        ("label_values_app", "app"),
        ("label_values_env", "env"),
        ("label_values_service_name", "service_name"),
    ] {
        cases.push(Case {
            name,
            path: format!("/loki/api/v1/label/{label}/values"),
            params: window(),
            encoding_flags: None,
        });
    }
    cases
}

/// `/format_query`, which is pure: no data, no clock, just the pretty-printer.
fn format_query_cases() -> Vec<Case> {
    [
        ("format_query_selector", r#"{app="api",env="prod"}"#),
        (
            "format_query_pipeline",
            r#"{app="api"}|="error"|json|status="500"|line_format "{{.msg}}""#,
        ),
        (
            "format_query_metric",
            r#"sum by(app)(rate({app=~"api|worker"}|="error"[5m]))"#,
        ),
        (
            "format_query_unwrap",
            r#"quantile_over_time(0.99,{app="fmt"}|logfmt|unwrap duration[5m])by(service)"#,
        ),
        (
            "format_query_binary",
            r#"sum(count_over_time({app="api"}[1m]))/sum(count_over_time({app="worker"}[1m]))"#,
        ),
    ]
    .into_iter()
    .map(|(name, logql)| Case {
        name,
        path: "/loki/api/v1/format_query".to_string(),
        params: vec![("query", logql.to_string())],
        encoding_flags: None,
    })
    .collect()
}

fn range_params(timeline: &Timeline, logql: &str, step_secs: i64) -> Vec<(&'static str, String)> {
    vec![
        ("query", logql.to_string()),
        ("start", timeline.at(0).to_string()),
        ("end", timeline.at(60).to_string()),
        ("step", step_secs.to_string()),
        ("direction", "forward".to_string()),
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
// Pushing.
// ---------------------------------------------------------------------------

fn json_streams(timeline: &Timeline, streams: &[SeedStream]) -> Value {
    json!({
        "streams": streams
            .iter()
            .map(|stream| json!({
                "stream": stream
                    .labels
                    .iter()
                    .map(|(name, value)| ((*name).to_string(), json!(value)))
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
                                    .map(|(name, value)| ((*name).to_string(), json!(value)))
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
                                .map(|(name, value)| LokiProtoLabelPair {
                                    name: (*name).to_string(),
                                    value: (*value).to_string(),
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
fn proto_label_set(labels: &[(&str, &str)]) -> String {
    let pairs = labels
        .iter()
        .map(|(name, value)| format!("{name}=\"{value}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{pairs}}}")
}

async fn push_json(client: &reqwest::Client, base: &str, body: &Value) -> TestResult {
    accept_push(
        client
            .post(format!("{base}/loki/api/v1/push"))
            .header("Content-Type", "application/json")
            .header("X-Scope-OrgID", TENANT)
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
// Probing and comparing.
// ---------------------------------------------------------------------------

/// One side's answer to one case: the status, and the body normalized.
///
/// `message` is deliberately outside the comparison -- see `probe` -- so
/// `PartialEq` is written rather than derived.
#[derive(Debug)]
struct Probe {
    status: u16,
    body: Value,
    message: String,
}

impl PartialEq for Probe {
    fn eq(&self, other: &Self) -> bool {
        self.status == other.status && self.body == other.body
    }
}

async fn probe(client: &reqwest::Client, base: &str, case: &Case) -> TestResult<Probe> {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(
            case.params
                .iter()
                .map(|(name, value)| (*name, value.as_str())),
        )
        .finish();
    let mut request = client
        .get(format!("{base}{}?{query}", case.path))
        .header("X-Scope-OrgID", TENANT);
    if let Some(encoding_flags) = case.encoding_flags {
        request = request.header("X-Loki-Response-Encoding-Flags", encoding_flags);
    }
    let response = request.send().await?;
    let status = response.status().as_u16();
    let text = response.text().await?;
    if status == 200 {
        return Ok(Probe {
            status,
            body: normalize(&serde_json::from_str::<Value>(&text)?),
            message: String::new(),
        });
    }
    // A non-200 body is shaped by whichever server wrote it -- Loki answers in
    // plain text, Krabka in a JSON envelope -- so it is not part of the
    // comparison. It is carried anyway, because a status divergence is
    // unreadable without the reason the losing side gave.
    Ok(Probe {
        status,
        body: Value::Null,
        message: text,
    })
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
/// labels unordered -- `{"env":"stage","service_name":"meta","app":"meta"}` for
/// a label set Krabka writes sorted. That is not a difference in the answer,
/// so object keys are sorted on both sides before anything is compared. The
/// arrays that carry an *unordered set* -- `result`, and the bare array
/// `/labels` and `/series` answer with -- are sorted for the same reason. The
/// arrays that carry an ordered sequence, an entry's `[timestamp, line]` and a
/// series' `values`, are left exactly as each side wrote them.
fn normalize(response: &Value) -> Value {
    let mut response = sort_object_keys(response);
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
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), sort_object_keys(value)))
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
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

fn known_divergence(case: &'static str) -> Option<&'static Divergence> {
    LOKI_KNOWN_DIVERGENCE
        .iter()
        .find(|divergence| divergence.case == case)
}

fn report(case: &Case, krabka: &Probe, loki: &Probe) -> String {
    let params = case
        .params
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let encoding_flags = case.encoding_flags.map_or(String::new(), |flags| {
        format!("\n  X-Loki-Response-Encoding-Flags: {flags}")
    });
    format!(
        "case `{}` differed\n  request: {} {params}{encoding_flags}\n  krabka  ({}): {}\n  \
         loki    ({}): {}",
        case.name,
        case.path,
        krabka.status,
        answer(krabka),
        loki.status,
        answer(loki),
    )
}

/// What one side said: the normalized body when it answered, the message it
/// wrote when it refused.
fn answer(probe: &Probe) -> String {
    if probe.status == 200 {
        pretty(&probe.body)
    } else {
        probe.message.trim().to_string()
    }
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
            // No log-line wait condition: this config sets `log_level: warn`,
            // and a healthy single-binary Loki says nothing at that level at
            // all. `wait_for_ready` polls /ready instead, which is the answer
            // the question actually wants.
            // Copied into the image rather than bind-mounted, so the test has
            // no host-filesystem prerequisites. The path is the one the image's
            // own default command already points at.
            .with_copy_to(
                "/etc/loki/local-config.yaml",
                LOKI_CONFIG.as_bytes().to_vec(),
            )
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

/// The Krabka side: a distributor and a querier over one shared WAL.
///
/// Two listeners rather than one merged router, because the two roles are two
/// routers: both register `/loki/api/v1/format_query` and both register the
/// role's ops endpoints, so merging them is an overlapping-route panic. A real
/// deployment runs them as separate roles too.
struct KrabkaStack {
    push_url: String,
    query_url: String,
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
    let sink = InMemoryWalSink::default();
    let root = tempfile::tempdir()?.keep();
    // `i64::MIN`: nothing has been compacted, so every record the distributor
    // wrote is in the querier's hot tail. This is the same wiring
    // `build_service_router` uses for a querier with no compaction frontier.
    let state = QuerierState::new(root, LabelIndex::default(), BlockIndex::default())
        .with_hot_tail(sink.clone(), i64::MIN);
    let (push_url, push_shutdown) = serve(distributor_router(sink)).await?;
    let (query_url, query_shutdown) = serve(loki_router(state)).await?;
    Ok(KrabkaStack {
        push_url,
        query_url,
        shutdown: vec![push_shutdown, query_shutdown],
    })
}

async fn serve(router: axum::Router) -> TestResult<(String, oneshot::Sender<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    Ok((format!("http://{addr}"), tx))
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

/// Waits until every seeded stream is queryable on `base`.
///
/// Loki acknowledges a push before the entry is visible to a query, and a
/// differential that queries too early compares a full corpus against a partial
/// one and blames the engine.
async fn wait_for_seeded(client: &reqwest::Client, base: &str, timeline: &Timeline) -> TestResult {
    let expected = JSON_STREAMS.len() + GZIP_STREAMS.len() + PROTO_STREAMS.len();
    let case = Case {
        name: "seeded",
        path: "/loki/api/v1/label/app/values".to_string(),
        params: vec![
            ("start", timeline.at(-60).to_string()),
            ("end", timeline.at(120).to_string()),
        ],
        encoding_flags: None,
    };
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        if let Ok(probe) = probe(client, base, &case).await
            && probe.body["data"].as_array().map_or(0, Vec::len) >= expected
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("{base} never returned all {expected} seeded streams").into())
}
