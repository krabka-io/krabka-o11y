#![recursion_limit = "512"]

//! Docker-backed Grafana end-to-end coverage.
//!
//! The test runs a real Grafana with a provisioned Prometheus-type datasource
//! that points at the in-process Krabka query API. The test drives the full
//! `PromQL` query-shape matrix through Grafana's dashboard query path:
//! `POST /api/ds/query`, instant and range. The test also drives the metadata,
//! label, series, exemplar, and build-info surfaces through Grafana's
//! datasource resource proxy at `/api/datasources/uid/<uid>/resources/...`.
//! Every assertion exercises the full Grafana -> Prometheus-datasource ->
//! Krabka path.
//!
//! Cargo ignores this test by default, because it pulls and runs
//! `mirror.gcr.io/grafana/grafana` under Docker.
//! Run with:
//!
//! `cargo test -p krabka-metrics-service --test grafana_integration -- --ignored --nocapture`
//!
//! Host reachability is platform-specific. Grafana runs in a container and must
//! reach the Krabka server on the host. The test binds Krabka to `0.0.0.0:0`
//! and gives the mapped port to Grafana in a provisioned datasource URL of
//! `http://host.docker.internal:<port>`. The test also adds
//! `host.docker.internal -> host-gateway` to the container with
//! `with_host(.., Host::HostGateway)`. Docker exposes the host to a container
//! this way on Linux, macOS, and Windows.

use std::{collections::BTreeMap, time::Duration};

use krabka_promql::WalHead;
use serde_json::{Value, json};
use testcontainers::{
    GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

// The shared corpus/differ module is path-included exactly as `diff_prometheus.rs`
// does it, so all metrics differential suites share one corpus definition. This
// integration only needs `seed_dataset`; the differ/corpus helpers are unused
// here, so allow dead code on the included module.
#[path = "support/pinned_grafana_image.rs"]
mod pinned_grafana_image;
#[path = "support/seed_remote_write.rs"]
mod seed_remote_write;
#[path = "support/upstream_http.rs"]
mod upstream_http;

use self::{
    pinned_grafana_image::pinned_grafana_image,
    seed_remote_write::remote_write_body,
    upstream_http::{
        KrabkaServer, RemoteWrite, TestResult, mapped_base_url, post_remote_write,
        wait_for_http_ok, wait_for_non_empty_result,
    },
};

#[allow(dead_code)]
#[path = "../../metrics/tests/support/diff_corpus.rs"]
mod diff_corpus;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// Tenant header that Grafana forwards to Krabka.
///
/// The provisioned datasource carries it as a static `X-Scope-OrgID` header.
const TENANT: &str = "grafana";

/// Grafana's default HTTP port.
const GRAFANA_PORT: u16 = 3000;

/// Stable datasource UID that the `/api/ds/query` payload references.
const DATASOURCE_UID: &str = "krabka-prom";

/// The seed data spans t=0..45s.
///
/// Every instant query evaluates at this instant.
const EVAL_MS: i64 = 45_000;

/// Provisioned datasource that points Grafana at Krabka through the host
/// gateway.
///
/// The test replaces the `{PORT}` placeholder with the mapped Krabka port at
/// runtime. Grafana sends `X-Scope-OrgID` on every datasource request, both
/// queries and resource calls, so Krabka keys storage by the test tenant.
const DATASOURCE_YAML_TEMPLATE: &str = r"apiVersion: 1
datasources:
  - name: Krabka
    type: prometheus
    access: proxy
    uid: krabka-prom
    url: http://host.docker.internal:{PORT}
    isDefault: true
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: grafana
";

#[tokio::test]
#[ignore = "requires Docker"]
async fn grafana_e2e_covers_all_api_surfaces_and_query_shapes() -> TestResult {
    let client = reqwest::Client::new();

    // In-process Krabka write+query path, bound to a host-reachable address so the
    // Grafana container can dial back via host.docker.internal.
    let krabka = start_krabka_query_server().await?;
    post_remote_write(
        &client,
        RemoteWrite {
            base: &krabka.base_url,
            path: "/api/v1/write",
            tenant: Some(TENANT),
            body: &remote_write_body(),
        },
    )
    .await?;
    wait_for_query_ready(&client, &krabka.base_url, TENANT, "up").await?;

    // Real Grafana with the provisioned datasource.
    let datasource_yaml = DATASOURCE_YAML_TEMPLATE.replace(
        "{PORT}",
        krabka
            .base_url
            .rsplit_once(':')
            .map_or("", |(_, port)| port),
    );
    let grafana = start_grafana(&datasource_yaml).await?;
    let base = mapped_base_url(&grafana, GRAFANA_PORT).await?;
    wait_for_http_ok(
        &client,
        &format!("{base}/api/health"),
        Duration::from_mins(1),
    )
    .await?;
    // /api/health ("database: ok") can race ahead of datasource provisioning; a
    // query before the datasource UID resolves returns 404. Poll until present.
    wait_for_datasource(&client, &base, DATASOURCE_UID).await?;

    // Collect every mismatch so one run reports all problems rather than aborting
    // on the first.
    let mut fails: Vec<String> = Vec::new();

    check_instant_query_shapes(&client, &base, &mut fails).await?;
    check_range_query_shapes(&client, &base, &mut fails).await?;
    check_resource_surfaces(&client, &base, &mut fails).await?;

    krabka.shutdown();

    if fails.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} Grafana e2e check(s) failed:\n  - {}",
            fails.len(),
            fails.join("\n  - ")
        )
        .into())
    }
}

// ---------------------------------------------------------------------------
// Instant query-shape matrix (Grafana `/api/ds/query`, queryType=instant)
// ---------------------------------------------------------------------------

async fn check_instant_query_shapes(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    check_instant_selectors(client, base, fails).await?;
    check_instant_aggregations(client, base, fails).await?;
    check_instant_binary_operators(client, base, fails).await?;
    check_instant_range_functions(client, base, fails).await?;
    check_instant_over_time(client, base, fails).await?;
    check_instant_histograms(client, base, fails).await?;
    check_instant_label_manipulation(client, base, fails).await?;
    check_instant_scalar_math(client, base, fails).await?;
    check_instant_time_and_sort(client, base, fails).await
}

async fn check_instant_selectors(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    // (name, expr, expectation) — driven through Grafana's dashboard query path.
    // Values are computed against `seed_dataset()` evaluated at t=45s:
    //   up{job=api,instance=a}=1
    //   http_requests_total{method=GET,code=200}=120  {method=POST,code=500}=8
    //   cpu_temperature_celsius{job=node,instance=a}=43
    //   http_request_duration_seconds_bucket{le=0.5}=40 {le=1}=70 {le=+Inf}=90
    //   http_request_duration_seconds_sum=60  _count=90
    //   native_histogram_marker=1
    let exact = Expect::exact;

    // -- selectors & label matchers --------------------------------------------
    instant(
        client,
        base,
        fails,
        "selector gauge",
        "up",
        &[(&[("job", "api")], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "selector eq matcher",
        "http_requests_total{method=\"GET\"}",
        &[(&[("method", "GET")], exact(120.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "selector regex matcher",
        "http_requests_total{code=~\"5..\"}",
        &[(&[("method", "POST")], exact(8.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "selector negative matcher",
        "http_requests_total{method!=\"GET\"}",
        &[(&[("code", "500")], exact(8.0))],
    )
    .await;
    instant_empty(
        client,
        base,
        fails,
        "selector matches nothing",
        "http_requests_total{job=\"nope\"}",
    )
    .await;

    Ok(())
}

async fn check_instant_aggregations(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    let exact = Expect::exact;

    instant(
        client,
        base,
        fails,
        "sum",
        "sum(http_requests_total)",
        &[(&[], exact(128.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "sum by",
        "sum by (method) (http_requests_total)",
        &[
            (&[("method", "GET")], exact(120.0)),
            (&[("method", "POST")], exact(8.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "sum without",
        "sum without (method, code, instance) (http_requests_total)",
        &[(&[("job", "api")], exact(128.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "avg",
        "avg(http_requests_total)",
        &[(&[], exact(64.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "min",
        "min(http_requests_total)",
        &[(&[], exact(8.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "max",
        "max(http_requests_total)",
        &[(&[], exact(120.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "count",
        "count(http_requests_total)",
        &[(&[], exact(2.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "group",
        "group(http_requests_total)",
        &[(&[], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "topk",
        "topk(1, http_requests_total)",
        &[(&[("method", "GET")], exact(120.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "bottomk",
        "bottomk(1, http_requests_total)",
        &[(&[("method", "POST")], exact(8.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "quantile",
        "quantile(0.5, http_requests_total)",
        &[(&[], exact(64.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "stddev",
        "stddev(http_requests_total)",
        &[(&[], exact(56.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "stdvar",
        "stdvar(http_requests_total)",
        &[(&[], exact(3136.0))],
    )
    .await;
    instant_count(
        client,
        base,
        fails,
        "count_values",
        "count_values(\"v\", http_requests_total)",
        2,
    )
    .await;

    Ok(())
}

async fn check_instant_binary_operators(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    let exact = Expect::exact;
    let approx = Expect::approx;

    instant(
        client,
        base,
        fails,
        "scalar arithmetic",
        "http_requests_total * 2",
        &[
            (&[("method", "GET")], exact(240.0)),
            (&[("method", "POST")], exact(16.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "comparison filter",
        "http_requests_total > 100",
        &[(&[("method", "GET")], exact(120.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "comparison bool",
        "http_requests_total >= bool 100",
        &[
            (&[("method", "GET")], exact(1.0)),
            (&[("method", "POST")], exact(0.0)),
        ],
    )
    .await;
    instant_count(
        client,
        base,
        fails,
        "or set op",
        "up or http_requests_total",
        3,
    )
    .await;
    instant(
        client,
        base,
        fails,
        "and set op on label",
        "http_requests_total and on(job) up",
        &[
            (&[("method", "GET")], exact(120.0)),
            (&[("method", "POST")], exact(8.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "unless set op",
        "http_requests_total unless on(method) http_requests_total{method=\"GET\"}",
        &[(&[("method", "POST")], exact(8.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "group_left vector match",
        "http_requests_total / on(job) group_left sum by (job) (http_requests_total)",
        &[
            (&[("method", "GET")], approx(120.0 / 128.0)),
            (&[("method", "POST")], approx(8.0 / 128.0)),
        ],
    )
    .await;

    Ok(())
}

async fn check_instant_range_functions(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    let approx = Expect::approx;

    // rate over [30s] at 45s: GET samples 30s=75,45s=120 -> ~3/s; POST 30s=5,45s=8 -> ~0.2/s.
    instant(
        client,
        base,
        fails,
        "rate",
        "rate(http_requests_total[30s])",
        &[
            (&[("method", "GET")], approx(3.0)),
            (&[("method", "POST")], approx(0.2)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "increase",
        "increase(http_requests_total[30s])",
        &[
            (&[("method", "GET")], approx(90.0)),
            (&[("method", "POST")], approx(6.0)),
        ],
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "irate",
        "irate(http_requests_total[1m])",
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "delta",
        "delta(cpu_temperature_celsius[1m])",
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "idelta",
        "idelta(cpu_temperature_celsius[1m])",
    )
    .await;
    instant(
        client,
        base,
        fails,
        "changes",
        "changes(cpu_temperature_celsius[1m])",
        &[(&[("instance", "a")], approx(3.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "resets",
        "resets(cpu_temperature_celsius[1m])",
        &[(&[("instance", "a")], approx(1.0))],
    )
    .await;

    Ok(())
}

async fn check_instant_over_time(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    let exact = Expect::exact;
    let approx = Expect::approx;

    let temps = "cpu_temperature_celsius[1m]";
    instant(
        client,
        base,
        fails,
        "max_over_time",
        &format!("max_over_time({temps})"),
        &[(&[("instance", "a")], exact(43.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "min_over_time",
        &format!("min_over_time({temps})"),
        &[(&[("instance", "a")], exact(40.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "avg_over_time",
        &format!("avg_over_time({temps})"),
        &[(&[("instance", "a")], approx(41.625))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "sum_over_time",
        &format!("sum_over_time({temps})"),
        &[(&[("instance", "a")], exact(166.5))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "count_over_time",
        &format!("count_over_time({temps})"),
        &[(&[("instance", "a")], exact(4.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "last_over_time",
        &format!("last_over_time({temps})"),
        &[(&[("instance", "a")], exact(43.0))],
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "stddev_over_time",
        &format!("stddev_over_time({temps})"),
    )
    .await;
    instant(
        client,
        base,
        fails,
        "quantile_over_time",
        &format!("quantile_over_time(0.5, {temps})"),
        &[(&[("instance", "a")], approx(41.75))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "present_over_time",
        &format!("present_over_time({temps})"),
        &[(&[("instance", "a")], exact(1.0))],
    )
    .await;

    Ok(())
}

async fn check_instant_histograms(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    let approx = Expect::approx;

    // bucket counts at 45s: le0.5=40, le1=70, le+Inf=90; p50 -> ~0.583, p90 -> in (1,+Inf].
    instant(
        client,
        base,
        fails,
        "histogram_quantile p50",
        "histogram_quantile(0.5, http_request_duration_seconds_bucket)",
        &[(&[("job", "api")], approx(0.5833))],
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "histogram_quantile p90",
        "histogram_quantile(0.9, http_request_duration_seconds_bucket)",
    )
    .await;

    Ok(())
}

async fn check_instant_label_manipulation(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    let exact = Expect::exact;

    instant(
        client,
        base,
        fails,
        "label_replace adds label",
        "label_replace(up, \"datacenter\", \"east\", \"job\", \"api\")",
        &[(&[("datacenter", "east"), ("job", "api")], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "label_join concatenates",
        "label_join(up, \"id\", \"-\", \"job\", \"instance\")",
        &[(&[("id", "api-a")], exact(1.0))],
    )
    .await;

    Ok(())
}

async fn check_instant_scalar_math(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    let exact = Expect::exact;
    let approx = Expect::approx;

    instant(
        client,
        base,
        fails,
        "scalar",
        "scalar(up)",
        &[(&[], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "vector",
        "vector(42)",
        &[(&[], exact(42.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "abs",
        "abs(cpu_temperature_celsius - 100)",
        &[(&[("instance", "a")], exact(57.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "ceil",
        "ceil(http_request_duration_seconds_sum + 0.4)",
        &[(&[("job", "api")], exact(61.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "floor",
        "floor(http_request_duration_seconds_sum + 0.9)",
        &[(&[("job", "api")], exact(60.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "round",
        "round(cpu_temperature_celsius)",
        &[(&[("instance", "a")], exact(43.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "clamp_max",
        "clamp_max(http_requests_total, 100)",
        &[
            (&[("method", "GET")], exact(100.0)),
            (&[("method", "POST")], exact(8.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "clamp_min",
        "clamp_min(http_requests_total, 50)",
        &[
            (&[("method", "GET")], exact(120.0)),
            (&[("method", "POST")], exact(50.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "sqrt",
        "sqrt(http_request_duration_seconds_count)",
        &[(&[("job", "api")], approx(9.4868))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "ln",
        "ln(native_histogram_marker)",
        &[(&[("job", "api")], exact(0.0))],
    )
    .await;
    instant_present(client, base, fails, "exp", "exp(native_histogram_marker)").await;
    instant_present(client, base, fails, "trig", "sin(native_histogram_marker)").await;

    Ok(())
}

async fn check_instant_time_and_sort(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    let exact = Expect::exact;
    let approx = Expect::approx;

    instant(
        client,
        base,
        fails,
        "timestamp",
        "timestamp(up)",
        &[(&[("job", "api")], approx(45.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "absent present-metric",
        "absent(http_requests_total)",
        &[],
    )
    .await; // present -> empty
    instant(
        client,
        base,
        fails,
        "absent missing-metric",
        "absent(nonexistent_metric)",
        &[(&[], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "absent_over_time missing",
        "absent_over_time(nonexistent_metric[1m])",
        &[(&[], exact(1.0))],
    )
    .await;

    // -- sort / subquery / modifiers -------------------------------------------
    instant_count(client, base, fails, "sort", "sort(http_requests_total)", 2).await;
    instant_count(
        client,
        base,
        fails,
        "sort_desc",
        "sort_desc(http_requests_total)",
        2,
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "subquery",
        "max_over_time(rate(http_requests_total[30s])[1m:15s])",
    )
    .await;
    instant(
        client,
        base,
        fails,
        "at modifier",
        "up @ 30.000",
        &[(&[("job", "api")], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "offset modifier",
        "http_requests_total offset 15s",
        &[
            (&[("method", "GET")], exact(75.0)),
            (&[("method", "POST")], exact(5.0)),
        ],
    )
    .await;

    Ok(())
}

// ---------------------------------------------------------------------------
// Range query-shape coverage (Grafana `/api/ds/query`, queryType=range)
// ---------------------------------------------------------------------------

async fn check_range_query_shapes(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    // A representative subset over [0, 45s] step 15s -> >= 2 points per series.
    for (name, expr, min_series) in [
        ("range gauge", "up", 1usize),
        ("range counter rate", "rate(http_requests_total[30s])", 2),
        (
            "range aggregation",
            "sum by (method) (http_requests_total)",
            2,
        ),
        (
            "range histogram_quantile",
            "histogram_quantile(0.5, http_request_duration_seconds_bucket)",
            1,
        ),
        ("range scalar math", "cpu_temperature_celsius * 2", 1),
        (
            "range subquery",
            "max_over_time(rate(http_requests_total[30s])[1m:15s])",
            2,
        ),
    ] {
        let raw = ds_query(client, base, expr, Some((0, EVAL_MS, 15))).await?;
        let series = parse_range_series(&raw);
        if series.len() < min_series {
            fails.push(format!(
                "range `{name}` ({expr}): expected >= {min_series} series, got {} ({raw})",
                series.len()
            ));
            continue;
        }
        let has_multi_point = series.iter().any(|points| points.len() >= 2);
        if !has_multi_point {
            fails.push(format!(
                "range `{name}` ({expr}): no series carried >= 2 data points"
            ));
        }
        // The render path must surface at least one real value. NaN at some steps
        // is legitimate (e.g. histogram_quantile over the all-zero start buckets),
        // so we require some-finite rather than all-finite.
        if !series.iter().flatten().any(|v| v.is_finite()) {
            fails.push(format!(
                "range `{name}` ({expr}): rendered no finite values"
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Resource-proxy surfaces (labels / values / series / metadata / exemplars /
// build-info) via Grafana's datasource resource API.
// ---------------------------------------------------------------------------

async fn check_resource_surfaces(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    // /api/v1/labels -> includes the seeded label names.
    let labels = resource_json(client, base, "api/v1/labels").await?;
    let names = string_array(&labels["data"]);
    for want in ["__name__", "job", "method", "code", "instance", "le"] {
        if !names.iter().any(|n| n == want) {
            fails.push(format!("resource labels: missing `{want}` in {names:?}"));
        }
    }

    // /api/v1/label/job/values -> {api, node}.
    let job_values = resource_json(client, base, "api/v1/label/job/values").await?;
    let jobs = string_array(&job_values["data"]);
    for want in ["api", "node"] {
        if !jobs.iter().any(|j| j == want) {
            fails.push(format!(
                "resource label values(job): missing `{want}` in {jobs:?}"
            ));
        }
    }

    // /api/v1/label/__name__/values -> includes the seeded metric names.
    let metric_values = resource_json(client, base, "api/v1/label/__name__/values").await?;
    let metrics = string_array(&metric_values["data"]);
    for want in ["up", "http_requests_total", "cpu_temperature_celsius"] {
        if !metrics.iter().any(|m| m == want) {
            fails.push(format!(
                "resource __name__ values: missing `{want}` in {metrics:?}"
            ));
        }
    }

    // /api/v1/series?match[]=http_requests_total -> 2 series.
    let series = resource_json(
        client,
        base,
        "api/v1/series?match%5B%5D=http_requests_total",
    )
    .await?;
    let series_count = series["data"].as_array().map_or(0, Vec::len);
    if series_count != 2 {
        fails.push(format!(
            "resource series(http_requests_total): expected 2, got {series_count} ({series})"
        ));
    }

    // /api/v1/metadata -> reachable, success status (seed carries no metadata, so
    // the payload may be empty; we assert the surface responds correctly).
    let metadata = resource_json(client, base, "api/v1/metadata").await?;
    if metadata["status"] != "success" {
        fails.push(format!(
            "resource metadata: status not success ({metadata})"
        ));
    }

    // /api/v1/query_exemplars -> reachable (seed carries no exemplars).
    let exemplars = resource_json(
        client,
        base,
        "api/v1/query_exemplars?query=http_requests_total&start=0&end=45",
    )
    .await
    .ok();
    if let Some(exemplars) = exemplars
        && exemplars["status"] != "success"
    {
        fails.push(format!(
            "resource query_exemplars: status not success ({exemplars})"
        ));
    }

    // /api/v1/status/buildinfo -> Grafana feature detection; must report success.
    let buildinfo = resource_json(client, base, "api/v1/status/buildinfo").await?;
    if buildinfo["status"] != "success" {
        fails.push(format!(
            "resource buildinfo: status not success ({buildinfo})"
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Query helpers + frame parsing
// ---------------------------------------------------------------------------

/// Expected value for a single series.
#[derive(Clone, Copy)]
enum Expect {
    Exact(f64),
    Approx(f64),
}

impl Expect {
    fn exact(value: f64) -> Self {
        Self::Exact(value)
    }
    fn approx(value: f64) -> Self {
        Self::Approx(value)
    }
    fn matches(self, got: f64) -> bool {
        match self {
            Self::Exact(want) => (got - want).abs() <= 1e-6 * want.abs().max(1.0) + 1e-9,
            Self::Approx(want) => (got - want).abs() <= 0.05 * want.abs().max(1.0) + 1e-6,
        }
    }
    fn want(self) -> f64 {
        match self {
            Self::Exact(v) | Self::Approx(v) => v,
        }
    }
}

/// Run an instant query through Grafana and assert the parsed series.
///
/// The series must match the expected `(label-subset, value)` set exactly, in
/// any order.
async fn instant(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
    name: &str,
    expr: &str,
    expected: &[(&[(&str, &str)], Expect)],
) {
    let raw = match ds_query(client, base, expr, None).await {
        Ok(raw) => raw,
        Err(error) => {
            fails.push(format!(
                "instant `{name}` ({expr}): request failed: {error}"
            ));
            return;
        }
    };
    let series = parse_instant_series(&raw);
    if series.len() != expected.len() {
        fails.push(format!(
            "instant `{name}` ({expr}): expected {} series, got {} ({})",
            expected.len(),
            series.len(),
            serde_json::to_string(&raw).unwrap_or_default()
        ));
        return;
    }
    for (want_labels, want_value) in expected {
        match series_value(&series, want_labels) {
            Some(got) if want_value.matches(got) => {}
            Some(got) => fails.push(format!(
                "instant `{name}` ({expr}): series {want_labels:?} = {got}, expected ~{}",
                want_value.want()
            )),
            None => fails.push(format!(
                "instant `{name}` ({expr}): no series matched {want_labels:?} in {series:?}"
            )),
        }
    }
}

/// Assert an instant query renders exactly `count` series.
///
/// This helper does not check the values. It checks only that the path rendered
/// the right cardinality.
async fn instant_count(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
    name: &str,
    expr: &str,
    count: usize,
) {
    match ds_query(client, base, expr, None).await {
        Ok(raw) => {
            let series = parse_instant_series(&raw);
            if series.len() != count {
                fails.push(format!(
                    "instant `{name}` ({expr}): expected {count} series, got {} ({series:?})",
                    series.len()
                ));
            }
        }
        Err(error) => fails.push(format!(
            "instant `{name}` ({expr}): request failed: {error}"
        )),
    }
}

/// Assert an instant query renders at least one finite-valued series.
async fn instant_present(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
    name: &str,
    expr: &str,
) {
    match ds_query(client, base, expr, None).await {
        Ok(raw) => {
            let series = parse_instant_series(&raw);
            if series.is_empty() || series.iter().any(|(_, v)| !v.is_finite()) {
                fails.push(format!(
                    "instant `{name}` ({expr}): expected >= 1 finite series, got {series:?}"
                ));
            }
        }
        Err(error) => fails.push(format!(
            "instant `{name}` ({expr}): request failed: {error}"
        )),
    }
}

/// Assert an instant query renders no series: an empty vector.
async fn instant_empty(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
    name: &str,
    expr: &str,
) {
    match ds_query(client, base, expr, None).await {
        Ok(raw) => {
            let series = parse_instant_series(&raw);
            if !series.is_empty() {
                fails.push(format!(
                    "instant `{name}` ({expr}): expected empty, got {series:?}"
                ));
            }
        }
        Err(error) => fails.push(format!(
            "instant `{name}` ({expr}): request failed: {error}"
        )),
    }
}

/// Find the value of the parsed series whose labels include every `(k, v)` in
/// `want`.
///
/// This is a label-subset match. It ignores `__name__`.
fn series_value(series: &[(BTreeMap<String, String>, f64)], want: &[(&str, &str)]) -> Option<f64> {
    series
        .iter()
        .find(|(labels, _)| {
            want.iter()
                .all(|(k, v)| labels.get(*k).map(String::as_str) == Some(*v))
        })
        .map(|(_, value)| *value)
}

/// Parse a Grafana `/api/ds/query` instant response into `(labels, value)` per
/// series.
///
/// Grafana renders each series as a numeric field that carries the series
/// labels. The instant value is the last datum of the field, and the field has
/// only one datum.
fn parse_instant_series(resp: &Value) -> Vec<(BTreeMap<String, String>, f64)> {
    number_fields(resp)
        .into_iter()
        .filter_map(|(field, column)| {
            let labels = field["labels"]
                .as_object()
                .map(|map| {
                    map.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default();
            column
                .last()
                .and_then(Value::as_f64)
                .or((!column.is_empty()).then_some(f64::NAN))
                .map(|value| (labels, value))
        })
        .collect()
}

/// Parse a Grafana `/api/ds/query` range response into one value-column per
/// series.
fn parse_range_series(resp: &Value) -> Vec<Vec<f64>> {
    number_fields(resp)
        .into_iter()
        .map(|(_, column)| {
            column
                .iter()
                .map(|v| v.as_f64().unwrap_or(f64::NAN))
                .collect()
        })
        .collect()
}

/// Every numeric field of a Grafana `/api/ds/query` response, with its value
/// column. The Time field is skipped.
fn number_fields(resp: &Value) -> Vec<(&Value, &Vec<Value>)> {
    let Some(frames) = resp["results"]["A"]["frames"].as_array() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for frame in frames {
        let (Some(fields), Some(columns)) = (
            frame["schema"]["fields"].as_array(),
            frame["data"]["values"].as_array(),
        ) else {
            continue;
        };
        for (index, field) in fields.iter().enumerate() {
            if field["type"].as_str() != Some("number") {
                continue;
            }
            if let Some(column) = columns.get(index).and_then(Value::as_array) {
                out.push((field, column));
            }
        }
    }
    out
}

/// Issue a Grafana datasource query with `POST /api/ds/query`.
///
/// The `range` parameter is `(start_ms, end_ms, step_secs)`. A `None` range
/// makes an instant query at `EVAL_MS`.
async fn ds_query(
    client: &reqwest::Client,
    base: &str,
    expr: &str,
    range: Option<(i64, i64, i64)>,
) -> TestResult<Value> {
    let mut target = json!({
        "refId": "A",
        "datasource": { "type": "prometheus", "uid": DATASOURCE_UID },
        "expr": expr,
    });
    let (from, to) = if let Some((start, end, step)) = range {
        target["queryType"] = json!("range");
        target["range"] = json!(true);
        target["intervalMs"] = json!(step * 1000);
        target["maxDataPoints"] = json!(1000);
        (start, end)
    } else {
        target["queryType"] = json!("instant");
        target["instant"] = json!(true);
        (EVAL_MS, EVAL_MS)
    };
    let body = json!({ "from": from.to_string(), "to": to.to_string(), "queries": [target] });

    let response = client
        .post(format!("{base}/api/ds/query"))
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json().await?)
}

/// GET a datasource resource path through Grafana's resource proxy.
///
/// This function returns the raw Prometheus JSON that the datasource produced.
/// The `path` parameter is the datasource-relative path, for example
/// `api/v1/labels`. The `path` parameter is already URL-encoded where needed.
async fn resource_json(client: &reqwest::Client, base: &str, path: &str) -> TestResult<Value> {
    let url = format!("{base}/api/datasources/uid/{DATASOURCE_UID}/resources/{path}");
    let response = client.get(url).send().await?.error_for_status()?;
    Ok(response.json().await?)
}

/// Extract a JSON string array: the Prometheus `data` payloads for labels and
/// values.
fn string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Container + Krabka harness
// ---------------------------------------------------------------------------

async fn start_grafana(
    datasource_yaml: &str,
) -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        pinned_grafana_image()
            .with_exposed_port(GRAFANA_PORT.tcp())
            // Grafana writes its go logger to STDOUT (verified: the "HTTP Server
            // Listen" line appears on stdout, not stderr). /api/health is polled for
            // real readiness below.
            .with_wait_for(WaitFor::message_on_stdout("HTTP Server Listen"))
            // Provision the datasource so no UI/API setup is needed.
            .with_copy_to(
                "/etc/grafana/provisioning/datasources/krabka.yaml",
                datasource_yaml.as_bytes().to_vec(),
            )
            // host.docker.internal -> host gateway lets the container reach the Krabka
            // server running on the host.
            .with_host("host.docker.internal", Host::HostGateway)
            // Keep the bundled plugins from the pinned image.
            .with_env_var("GF_PLUGINS_PREINSTALL_DISABLED", "true")
            // Anonymous admin so the test drives the API without a login.
            .with_env_var("GF_AUTH_ANONYMOUS_ENABLED", "true")
            .with_env_var("GF_AUTH_ANONYMOUS_ORG_ROLE", "Admin")
            .with_env_var("GF_AUTH_BASIC_ENABLED", "false")
            .start(),
    )
    .await??)
}

async fn start_krabka_query_server() -> TestResult<KrabkaServer> {
    let head = WalHead::new();
    let query_router = krabka_metrics_service::prometheus_router_for_store(head.clone());
    // Bind 0.0.0.0 so the Grafana container can reach the server through the
    // Docker host gateway; the OS picks the port.
    KrabkaServer::start(query_router, head, "0.0.0.0:0".parse()?).await
}

async fn wait_for_datasource(client: &reqwest::Client, base: &str, uid: &str) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
    while std::time::Instant::now() < deadline {
        if client
            .get(format!("{base}/api/datasources/uid/{uid}"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(format!("datasource {uid} was not provisioned on {base}").into())
}

async fn wait_for_query_ready(
    client: &reqwest::Client,
    base: &str,
    tenant: &str,
    query: &str,
) -> TestResult {
    let url = format!(
        "{base}/api/v1/query?query={}&time=45.000",
        url::form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    );
    let url = &url;
    let ready = wait_for_non_empty_result(std::time::Duration::from_secs(15), || async move {
        let response: Value = client
            .get(url)
            .header("X-Scope-OrgID", tenant)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(response)
    })
    .await?;
    if ready {
        return Ok(());
    }
    Err(format!("query `{query}` did not become non-empty on {base}").into())
}
