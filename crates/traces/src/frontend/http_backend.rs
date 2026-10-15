//! The real querier fan-out backend.
//!
//! It is a reqwest client over a configurable set of querier addresses. It
//! speaks the Tempo HTTP API at the per-job grain, one HTTP call per planned
//! shard.
//!
//! The shard restriction uses the querier's real `scan_options` contract in
//! `querier/http::scan_options_param`. A cold block with a row-group range is
//! `block=<object_key>&rowGroupStart=<n>&rowGroupEnd=<m>`. The live shard sends
//! no scan params, which gives the querier's hot/cold union scan. There is no
//! `shard=live` param.
//!
//! By-id has no block scoping. It targets one querier by address and unions
//! across the ready pool. `start` and `end` are epoch **seconds** on every
//! endpoint.
//!
//! The transport picks no targets. Every request carries the `host:port` the
//! frontend assigned it to; see [`crate::frontend::membership`] and
//! [`crate::frontend::assignment`].

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use krabka_blockstore::TENANT_HEADER;
use krabka_observability::{
    RoleReadiness,
    server_security::{InternalClient, ServerListener, ServerSecurity, serve_router},
};
use krabka_units::convert::TimeExt as _;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::frontend::{
    QuerierScheme,
    backend::{
        BackendError, MetricsJobRequest, MetricsPartial, QuerierBackend, SearchJobRequest,
        SearchPartial, TagNamesJobRequest, TagNamesPartial, TagValuesJobRequest, TagValuesPartial,
        TraceByIdJobRequest, TracePartial,
    },
    config::FrontendConfig,
    job::{JobShard, TraceIndexCatalog},
    membership::{
        HttpReadinessProbe, MembershipView, QUERIER_MEMBERSHIP_GATE, mark_querier_membership_gate,
        refresh_membership, run_membership_refresh,
    },
    metrics_merge::{KeyValue, MetricSample, MetricSeries, MetricsResponseJson},
    wire::{Metrics, SearchResponseJson, TraceByIdResponseJson},
};

struct QueryParam {
    key: String,
    value: String,
}

impl QueryParam {
    fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

// --- Tag body parsing (the querier's v2 tag shapes) -------------------------

use std::future::IntoFuture as _;

use krabka_observability::{CriticalTaskError, SupervisedTasks};

/// Refuses a non-success querier response, then decodes its JSON body.
///
/// `body_kind` names the body in the transport error, as in
/// `decode <body_kind> body: <cause>`.
pub(crate) async fn decode_json_body<T: DeserializeOwned>(
    resp: reqwest::Response,
    body_kind: &str,
) -> Result<T, BackendError> {
    error_for_status(resp)
        .await?
        .json()
        .await
        .map_err(|e| BackendError::Transport(format!("decode {body_kind} body: {e}")))
}

/// Boot the query-frontend role.
///
/// This builds the querier transport, resolves and probes the querier pool
/// once so the first query sees real membership rather than an empty one, then
/// serves the router on `cfg.listen_addr` until `shutdown` fires.
///
/// `security` decides how the listener serves, and it gives the internal
/// client that the transport and the probe dial the queriers with. Every
/// route checks the request's principal against the tenant before it plans a
/// job.
///
/// The membership refresh runs as a supervised task, not a bare `tokio::spawn`.
/// If it stopped unnoticed the frontend would keep fanning out over whichever
/// pool it last saw. That is stale, and silently so, which is the failure the
/// loop exists to prevent.
///
/// `catalog` is the production [`TraceIndexCatalog`], or any compatible block
/// catalog.
///
/// `readiness` already carries the gates the caller cleared before this point.
/// This function adds `querier-membership`, which the probe loop moves up and
/// down for the life of the role. It is the one gate of a query-frontend that
/// no amount of startup can settle, because it reports other processes.
///
/// # Errors
/// Propagates bind and serve `std::io` errors, and backend-construction
/// failures.
pub async fn run_query_frontend(
    cfg: FrontendConfig,
    catalog: TraceIndexCatalog,
    readiness: RoleReadiness,
    security: &ServerSecurity,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let internal_client = security.internal_client();
    let backend = HttpQuerier::new(
        cfg.request_timeout.to_std(),
        cfg.querier_scheme,
        internal_client,
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    let probe: Arc<dyn crate::frontend::membership::ReadinessProbe> = Arc::new(
        HttpReadinessProbe::new(
            cfg.readiness_timeout.to_std(),
            cfg.querier_scheme,
            internal_client,
        )
        .map_err(|e| std::io::Error::other(e.to_string()))?,
    );
    let membership_gate = readiness.gate(QUERIER_MEMBERSHIP_GATE);
    let membership = MembershipView::empty();
    let members = refresh_membership(&cfg.querier_addrs, probe.as_ref()).await;
    mark_querier_membership_gate(&membership_gate, &members);
    membership.publish(members);

    let listen_addr = cfg.listen_addr;
    let endpoints = cfg.querier_addrs.clone();
    let refresh_interval = cfg.membership_refresh_interval.to_std();
    let qf = Arc::new(crate::frontend::QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
        membership.clone(),
    ));
    let app = crate::frontend::server::router_with_backend(qf, readiness);
    let tcp = tokio::net::TcpListener::bind(listen_addr).await?;
    let listener = ServerListener::bind(tcp, security).map_err(std::io::Error::other)?;

    let mut tasks = SupervisedTasks::new(shutdown.clone());
    let refresh_shutdown = shutdown.clone();
    tasks.spawn(
        "traces query-frontend membership refresh",
        run_membership_refresh(
            membership,
            endpoints,
            probe,
            refresh_interval,
            membership_gate,
            refresh_shutdown,
        ),
    );

    let server = serve_router(listener, app, security)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    let outcome = tokio::select! {
        result = server => result,
        name = tasks.first_unexpected_exit() => Err(std::io::Error::other(CriticalTaskError(name))),
    };
    tasks.shutdown().await;
    outcome
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct ScopeTagsJson {
    #[serde(default)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) tags: Vec<String>,
}

/// The `/api/v2/search/tag/{tag}/values` body:
/// `{ tagValues: [{ type, value }], metrics }`.
#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct TagValuesBody {
    #[serde(rename = "tagValues", default)]
    pub(crate) tag_values: Vec<TypedValueJson>,
    #[serde(default)]
    pub(crate) metrics: crate::frontend::wire::Metrics,
}

impl TagValuesBody {
    pub(crate) fn into_typed_values(self) -> Vec<krabka_traceql::TypedValue> {
        self.tag_values
            .into_iter()
            .map(|v| krabka_traceql::TypedValue {
                type_: v.type_,
                value: v.value,
            })
            .collect()
    }
}

/// The `/api/v2/search/tags` body: `{ scopes: [{ name, tags }], metrics }`.
#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct TagsBody {
    #[serde(default)]
    pub(crate) scopes: Vec<ScopeTagsJson>,
    #[serde(default)]
    pub(crate) metrics: crate::frontend::wire::Metrics,
}

impl TagsBody {
    pub(crate) fn scoped_tags(&self) -> Vec<krabka_traceql::ScopedTag> {
        self.scopes
            .iter()
            .map(|s| krabka_traceql::ScopedTag {
                scope: parse_scope(&s.name),
                tags: s.tags.clone(),
            })
            .collect()
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct TypedValueJson {
    #[serde(rename = "type", default)]
    pub(crate) type_: String,
    #[serde(default)]
    pub(crate) value: String,
}

#[cfg(test)]
mod parse_scope_tests {

    /// `parse_scope` defaults to the span scope rather than refusing, so
    /// "span" and an unknown name reach the same answer by different routes.
    /// Every named scope is checked so none of them can quietly fall through
    /// to that default instead of being recognised.
    #[test]
    fn a_scope_name_defaults_to_span_only_when_unrecognised() {
        use krabka_traceql::TagScope;
        let scope = super::parse_scope;

        check!(scope("resource") == TagScope::Resource);
        check!(scope("intrinsic") == TagScope::Intrinsic);
        check!(scope("event") == TagScope::Event);
        check!(scope("link") == TagScope::Link);
        check!(scope("instrumentation") == TagScope::Instrumentation);

        // Both routes to Span: named, and by falling through.
        check!(scope("span") == TagScope::Span, "named explicitly");
        check!(scope("") == TagScope::Span, "the default");
        check!(scope("unknown") == TagScope::Span);
        check!(
            scope("Resource") == TagScope::Span,
            "case-sensitive, so this defaults"
        );
    }
    use assert2::check;

    use super::*;

    #[yare::parameterized(
        zero = { 0, "0" },
        one_second = { 1_000_000_000, "1" },
        fractional = { 1_400_000_000, "1.4" },
        negative_fractional = { -500_000_000, "-0.5" },
    )]
    fn ns_to_seconds_round_trips_whole_and_fractional(ns: i64, expected: &str) {
        check!(ns_to_seconds(ns) == expected);
    }

    #[test]
    fn tag_values_body_projects_typed_values() {
        let body = TagValuesBody {
            tag_values: vec![TypedValueJson {
                type_: "string".into(),
                value: "GET".into(),
            }],
            metrics: crate::frontend::wire::Metrics::default(),
        };
        let values = body.into_typed_values();
        assert2::assert!(values.len() == 1);
        assert2::assert!(values[0].value.as_str() == "GET");
    }
}

use crate::querier::http::tag_scope_name;

pub(crate) fn parse_scope(name: &str) -> krabka_traceql::TagScope {
    match name {
        "resource" => krabka_traceql::TagScope::Resource,
        "intrinsic" => krabka_traceql::TagScope::Intrinsic,
        "event" => krabka_traceql::TagScope::Event,
        "link" => krabka_traceql::TagScope::Link,
        "instrumentation" => krabka_traceql::TagScope::Instrumentation,
        _ => krabka_traceql::TagScope::Span,
    }
}

/// Build a `host/path?query` URL with the given params.
///
/// The crate builds `reqwest` without the `query` feature, so `url` encodes the
/// query strings. The legacy query-frontend uses the same approach.
pub(crate) fn build_url(base: &str, params: &[QueryParam]) -> Result<reqwest::Url, BackendError> {
    let mut url = reqwest::Url::parse(base)
        .map_err(|e| BackendError::Transport(format!("invalid url {base}: {e}")))?;
    {
        let mut pairs = url.query_pairs_mut();
        for param in params {
            pairs.append_pair(&param.key, &param.value);
        }
    }
    Ok(url)
}

pub(crate) async fn error_for_status(
    resp: reqwest::Response,
) -> Result<reqwest::Response, BackendError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let message = resp.text().await.unwrap_or_default();
    Err(BackendError::Backend {
        status: status.as_u16().to_string(),
        message,
    })
}

/// Builds a reqwest client for frontend-to-querier calls.
///
/// `timeout` bounds one request. `internal_client` supplies the token, TLS
/// identity, and CA bundle of the frontend's internal principal.
///
/// # Errors
/// Returns `BackendError::Transport` if the client cannot be built.
pub(crate) fn internal_http_client(
    timeout: Duration,
    internal_client: &InternalClient,
) -> Result<reqwest::Client, BackendError> {
    internal_client
        .apply(reqwest::Client::builder().timeout(timeout))
        .build()
        .map_err(|e| BackendError::Transport(e.to_string()))
}

/// Epoch nanos -> epoch seconds string. The querier parses `start` and `end` as
/// seconds, and allows a fractional part.
pub(crate) fn ns_to_seconds(ns: i64) -> String {
    let negative = ns < 0;
    let ns = ns.unsigned_abs();
    let seconds = ns / 1_000_000_000;
    let nanos = ns % 1_000_000_000;
    let s = if nanos == 0 {
        seconds.to_string()
    } else {
        let mut frac = format!("{nanos:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        format!("{seconds}.{frac}")
    };
    if negative { format!("-{s}") } else { s }
}

/// Push the querier's scan-job params for a cold-block shard. The live shard
/// sends no such params.
pub(crate) fn push_shard_params(params: &mut Vec<QueryParam>, shard: &JobShard) {
    if let JobShard::Block {
        block_id,
        row_group_start,
        row_group_end,
    } = shard
    {
        params.push(QueryParam::new("block", block_id.clone()));
        params.push(QueryParam::new(
            "rowGroupStart",
            row_group_start.to_string(),
        ));
        params.push(QueryParam::new("rowGroupEnd", row_group_end.to_string()));
    }
}

/// The HTTP transport to one querier at a time.
///
/// It holds no addresses of its own. Each request names the `host:port` the
/// frontend assigned it to, which is an address the membership saw ready
/// within the last refresh; the transport dials it, with the tenant in
/// `X-Scope-OrgID` and a per-request timeout.
///
/// It used to hold a `Vec<String>` and an atomic counter, and pick a target
/// with `next.fetch_add(1) % addrs.len()`. That put a fixed 1/N of every
/// query on an address that might be gone and offered no way to use one that
/// had appeared.
pub struct HttpQuerier {
    pub(crate) http: reqwest::Client,
    pub(crate) scheme: QuerierScheme,
}

impl HttpQuerier {
    /// Build the transport. `timeout` bounds one job.
    ///
    /// Every job dials `scheme://host:port`. The client carries
    /// `internal_client`: its token as the `Authorization` header of every
    /// job, its certificate as the TLS identity, and its CA bundle as the only
    /// roots that a querier certificate can chain to. A querier with
    /// authentication on then serves the frontend as the internal principal.
    /// The frontend has already checked the end user against the tenant, so
    /// that principal should hold every tenant the frontend serves.
    ///
    /// # Errors
    /// Returns `BackendError::Transport` if the client cannot be built.
    pub fn new(
        timeout: Duration,
        scheme: QuerierScheme,
        internal_client: &InternalClient,
    ) -> Result<Self, BackendError> {
        let http = internal_http_client(timeout, internal_client)?;
        Ok(Self { http, scheme })
    }

    /// Sends a GET for `url` with `tenant` in the tenant header.
    async fn send_tenant_get(
        &self,
        url: reqwest::Url,
        tenant: &str,
    ) -> Result<reqwest::Response, BackendError> {
        self.http
            .get(url)
            .header(TENANT_HEADER, tenant)
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))
    }

    pub(crate) fn map_send_err(e: &reqwest::Error) -> BackendError {
        if e.is_timeout() {
            BackendError::Timeout
        } else {
            BackendError::Transport(e.to_string())
        }
    }
}

#[async_trait]
impl QuerierBackend for HttpQuerier {
    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError> {
        let url = format!("{}://{}/api/search", self.scheme.as_str(), req.querier);
        let mut params: Vec<QueryParam> = vec![
            QueryParam::new("q", req.query.clone()),
            QueryParam::new("start", ns_to_seconds(req.start_ns)),
            QueryParam::new("end", ns_to_seconds(req.end_ns)),
            QueryParam::new("limit", req.limit.to_string()),
            QueryParam::new("spss", req.spss.to_string()),
        ];
        push_shard_params(&mut params, &req.shard);
        let resp = self
            .send_tenant_get(build_url(&url, &params)?, req.tenant.as_str())
            .await?;
        let body: SearchResponseJson = decode_json_body(resp, "search").await?;
        Ok(SearchPartial {
            traces: body.traces,
            metrics: body.metrics,
        })
    }

    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError> {
        let hex = crate::frontend::wire::hex16(&req.trace_id);
        let url = format!(
            "{}://{}/api/v2/traces/{hex}",
            self.scheme.as_str(),
            req.querier
        );
        let params: Vec<QueryParam> = vec![
            QueryParam::new("start", ns_to_seconds(req.start_ns)),
            QueryParam::new("end", ns_to_seconds(req.end_ns)),
        ];
        let resp = self
            .send_tenant_get(build_url(&url, &params)?, req.tenant.as_str())
            .await?;
        // The querier returns 404 when it does not hold the trace; treat that as
        // an empty partial rather than an error (another querier may have it).
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(TracePartial::default());
        }
        let body: TraceByIdResponseJson = decode_json_body(resp, "trace").await?;
        Ok(TracePartial {
            trace: body,
            metrics: crate::frontend::wire::Metrics::default(),
        })
    }

    async fn tag_names_job(
        &self,
        req: &TagNamesJobRequest,
    ) -> Result<TagNamesPartial, BackendError> {
        let url = format!(
            "{}://{}/api/v2/search/tags",
            self.scheme.as_str(),
            req.querier
        );
        let mut params: Vec<QueryParam> = vec![
            QueryParam::new("start", ns_to_seconds(req.start_ns)),
            QueryParam::new("end", ns_to_seconds(req.end_ns)),
        ];
        if let Some(scope) = req.scope {
            params.push(QueryParam::new("scope", tag_scope_name(scope).to_string()));
        }
        push_shard_params(&mut params, &req.shard);
        let resp = self
            .send_tenant_get(build_url(&url, &params)?, req.tenant.as_str())
            .await?;
        let body: TagsBody = decode_json_body(resp, "tags").await?;
        Ok(TagNamesPartial {
            tags: body.scoped_tags(),
            metrics: body.metrics,
        })
    }

    async fn tag_values_job(
        &self,
        req: &TagValuesJobRequest,
    ) -> Result<TagValuesPartial, BackendError> {
        // The tag is a client-supplied path segment (e.g. `span:name`,
        // `resource.service.name`); build it via `path_segments_mut` so any
        // special chars (`/`, `?`, `#`, space) are percent-encoded into a single
        // segment rather than corrupting the path/query when re-parsed.
        let mut url = reqwest::Url::parse(&format!("{}://{}", self.scheme.as_str(), req.querier))
            .map_err(|e| BackendError::Transport(format!("invalid querier addr: {e}")))?;
        url.path_segments_mut()
            .map_err(|()| BackendError::Transport("querier url cannot be a base".to_string()))?
            .extend(["api", "v2", "search", "tag", req.tag.as_str(), "values"]);
        let mut params: Vec<QueryParam> = vec![
            QueryParam::new("start", ns_to_seconds(req.start_ns)),
            QueryParam::new("end", ns_to_seconds(req.end_ns)),
        ];
        push_shard_params(&mut params, &req.shard);
        {
            let mut pairs = url.query_pairs_mut();
            for param in &params {
                pairs.append_pair(&param.key, &param.value);
            }
        }
        let resp = self.send_tenant_get(url, req.tenant.as_str()).await?;
        let body: TagValuesBody = decode_json_body(resp, "tag-values").await?;
        let metrics = body.metrics;
        Ok(TagValuesPartial {
            values: body.into_typed_values(),
            metrics,
        })
    }

    async fn metrics_job(&self, req: &MetricsJobRequest) -> Result<MetricsPartial, BackendError> {
        let path = if req.instant { "query" } else { "query_range" };
        let url = format!(
            "{}://{}/api/metrics/{path}",
            self.scheme.as_str(),
            req.querier
        );
        let mut params: Vec<QueryParam> = vec![
            QueryParam::new("q", req.query.clone()),
            QueryParam::new("start", metric_timestamp(req.start_ns, req.instant)),
            QueryParam::new("end", metric_timestamp(req.end_ns, req.instant)),
        ];
        if !req.instant {
            params.push(QueryParam::new("step", ns_to_seconds(req.step_ns)));
        }
        push_shard_params(&mut params, &req.shard);
        let resp = self
            .send_tenant_get(build_url(&url, &params)?, req.tenant.as_str())
            .await?;
        if req.instant {
            let response: InstantMetricsResponseJson =
                decode_json_body(resp, "instant metrics").await?;
            return response
                .into_partial(req.end_ns)
                .map_err(BackendError::Transport);
        }
        let response: MetricsResponseJson = decode_json_body(resp, "metrics").await?;
        Ok(MetricsPartial {
            response,
            metrics: crate::frontend::wire::Metrics::default(),
        })
    }
}

// Tempo's internal instant request builder sends integer nanoseconds. Keep
// historical short timestamps unambiguous under its <=10-digit seconds rule.
fn metric_timestamp(nanos: i64, instant: bool) -> String {
    let integer = nanos.to_string();
    if instant && integer.len() > 10 {
        integer
    } else {
        ns_to_seconds(nanos)
    }
}

#[cfg(test)]
mod metric_timestamp_tests {
    use assert2::assert;

    use super::metric_timestamp;

    #[test]
    fn instant_transport_preserves_modern_nanoseconds_and_historical_bounds() {
        let start = 1_700_000_000_123_000_001;
        let end = 1_700_000_001_987_654_321;
        let uri = format!(
            "/api/metrics/query?start={}&end={}",
            metric_timestamp(start, true),
            metric_timestamp(end, true)
        )
        .parse()
        .unwrap();
        assert!(
            crate::querier::http::instant_metric_bounds(&uri)
                == Ok(crate::querier::http::InstantMetricBounds {
                    start_ns: start,
                    end_ns: end,
                    step_ns: end - start,
                    query_end_ns: end,
                })
        );
        assert!(metric_timestamp(start, false) == "1700000000.123000001");
        assert!(metric_timestamp(1_000_000_000, true) == "1");
        assert!(metric_timestamp(-500_000_000, true) == "-0.5");
    }
}

#[derive(Deserialize)]
pub(super) struct InstantMetricsResponseJson {
    #[serde(default)]
    series: Vec<InstantSeries>,
    #[serde(default)]
    metrics: Metrics,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstantSeries {
    #[serde(default)]
    labels: Vec<KeyValue>,
    #[serde(default = "zero_value")]
    value: Value,
}

fn zero_value() -> Value {
    json!(0.0)
}

impl InstantMetricsResponseJson {
    pub(super) fn into_partial(self, point_ns: i64) -> Result<MetricsPartial, String> {
        let series = self
            .series
            .into_iter()
            .map(|series| {
                let value = series
                    .value
                    .as_f64()
                    .or_else(|| match series.value.as_str() {
                        Some("NaN") => Some(f64::NAN),
                        Some("Infinity") => Some(f64::INFINITY),
                        Some("-Infinity") => Some(f64::NEG_INFINITY),
                        _ => None,
                    })
                    .ok_or_else(|| "invalid instant metric value".to_string())?;
                Ok(MetricSeries {
                    labels: series.labels,
                    prom_labels: String::new(),
                    samples: vec![MetricSample {
                        timestamp_ms: (point_ns / 1_000_000).to_string(),
                        value,
                    }],
                    exemplars: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(MetricsPartial {
            response: MetricsResponseJson { series },
            metrics: self.metrics,
        })
    }
}

#[cfg(test)]
mod response_decode_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn scalar_replies_keep_typed_labels_accounting_and_internal_point() {
        let body = json!({"series": [{
            "labels": [{"key": "kind", "value": {"intValue": "7"}}], "value": 3.0
        }], "metrics": {"totalJobs": 4, "completedJobs": 3}});
        let response: InstantMetricsResponseJson = serde_json::from_value(body).unwrap();
        let partial = response.into_partial(1_700_000_000_123_456_789).unwrap();
        assert!(
            partial.response
                == MetricsResponseJson {
                    series: vec![MetricSeries {
                        labels: vec![KeyValue {
                            key: "kind".into(),
                            value: json!({"intValue":"7"})
                        }],
                        prom_labels: String::new(),
                        samples: vec![MetricSample {
                            timestamp_ms: "1700000000123".into(),
                            value: 3.0
                        }],
                        exemplars: Vec::new(),
                    }]
                }
        );
        assert!(
            partial.metrics
                == Metrics {
                    total_jobs: 4,
                    completed_jobs: 3,
                    ..Metrics::default()
                }
        );
        let omitted: InstantMetricsResponseJson =
            serde_json::from_value(json!({"series":[{}]})).unwrap();
        assert!(
            omitted.into_partial(0).unwrap().response.series[0].samples[0]
                .value
                .to_bits()
                == 0.0_f64.to_bits()
        );
    }

    #[test]
    fn range_fields_and_invalid_scalars_cannot_become_silent_zeroes() {
        for field in ["samples", "exemplars", "promLabels"] {
            let mut series = json!({"value":3});
            series[field] = json!([]);
            assert!(
                serde_json::from_value::<InstantMetricsResponseJson>(json!({"series":[series]}))
                    .is_err()
            );
        }
        for value in [
            json!(null),
            json!(true),
            json!("3"),
            json!("+Inf"),
            json!({}),
        ] {
            let reply: InstantMetricsResponseJson =
                serde_json::from_value(json!({"series":[{"value":value}]})).unwrap();
            assert!(reply.into_partial(0).is_err());
        }
    }

    #[yare::parameterized(
        nan = { "NaN", f64::NAN },
        positive_infinity = { "Infinity", f64::INFINITY },
        negative_infinity = { "-Infinity", f64::NEG_INFINITY },
    )]
    fn nonfinite_values_roundtrip(token: &str, expected: f64) {
        let reply: InstantMetricsResponseJson =
            serde_json::from_value(json!({"series":[{"value":token}]})).unwrap();
        ::core::assert_eq!(
            reply.into_partial(0).unwrap().response.series[0].samples[0]
                .value
                .to_bits(),
            expected.to_bits()
        );
    }
}
