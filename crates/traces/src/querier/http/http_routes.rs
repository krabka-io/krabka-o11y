use super::*;
use axum::http::StatusCode;
use krabka_observability::service_metrics::QueryRequest;
use krabka_observability::service_metrics::RequestOutcome;
use krabka_units::convert::StdDurationExt as _;
use krabka_traceql::Aggregate;
use krabka_traceql::Pipeline;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use crate::tempo_query_routes::tempo_query_routes;

pub(crate) struct AppState<S: SpanStore> {
    pub(crate) engine: Arc<TraceqlEngine<S>>,
    pub(crate) cfg: HttpConfig,
    pub(crate) metrics: Option<ServiceMetrics>,
}

impl<S: SpanStore> Clone for AppState<S> {
    fn clone(&self) -> Self {
        Self {
            engine: Arc::clone(&self.engine),
            cfg: self.cfg.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<S: SpanStore> AppState<S> {
    /// Record one querier request on `route` with the outcome its response
    /// `status` gives and its elapsed time. A 2xx counts as `status="ok"`,
    /// anything else as `status="error"`. This does nothing when metrics are
    /// not wired, as in test routers.
    pub(crate) fn record_query(&self, route: &str, status: StatusCode, start: std::time::Instant) {
        if let Some(metrics) = &self.metrics {
            metrics.record_query(QueryRequest {
                route,
                outcome: RequestOutcome::from_success_status(status),
                elapsed: start.elapsed().as_time(),
            });
        }
    }
}

/// Tempo-compatible build info.
///
/// Grafana's Tempo datasource probes this on every query to detect the backend
/// version. Without it, Grafana treats the backend as a legacy Tempo and falls
/// back to endpoints this crate does not serve, which breaks the trace-by-id
/// view. The Prometheus-style `{status, data:{version,...}}` shape matches
/// Tempo's `/api/status/buildinfo`.
pub(crate) async fn buildinfo() -> Response {
    crate::frontend::server::buildinfo().await
}

pub(crate) async fn echo() -> &'static str {
    "echo"
}

#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub max_trace_spans: usize,
    pub tag_query_filter_autocomplete_limit: usize,
    /// The one place a tenant's read limits come from.
    ///
    /// Its defaults are what an unlisted tenant gets, so a querier with no
    /// overrides file still has every limit here and needs no second set of
    /// globals beside it.
    pub overrides: OverridesProvider,
    /// The policy that every query route resolves a tenant with. Default:
    /// [`TenantPolicy::anonymous`].
    pub tenant_policy: TenantPolicy,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_trace_spans: usize::MAX,
            tag_query_filter_autocomplete_limit: 25,
            overrides: OverridesProvider::new(Limits::default()),
            tenant_policy: TenantPolicy::anonymous(),
        }
    }
}

impl HttpConfig {
    pub(crate) fn limits_for_tenant(&self, tenant: &TenantId) -> Limits {
        self.overrides.for_tenant(tenant.as_str())
    }
}

pub(crate) fn limit_error_response(err: &LimitError) -> Response {
    tempo_limit_error_response(err)
}

pub(crate) fn metrics_operation_name(query: &str) -> Option<&'static str> {
    let query = krabka_traceql::parse(query).ok()?;
    let metric_start = query.pipeline.iter().position(|stage| matches!(stage, Pipeline::Aggregate(aggregate) if matches!(aggregate, Aggregate::Rate | Aggregate::CountOverTime | Aggregate::SumOverTime(_) | Aggregate::AvgOverTime(_) | Aggregate::MinOverTime(_) | Aggregate::MaxOverTime(_) | Aggregate::HistogramOverTime(_) | Aggregate::QuantileOverTime { .. })))?;
    if query.pipeline[metric_start..]
        .iter()
        .any(|stage| matches!(stage, Pipeline::By(fields) if !fields.is_empty()))
    {
        return None;
    }
    query.pipeline[metric_start..].iter().find_map(|stage| {
        let Pipeline::Aggregate(aggregate) = stage else {
            return None;
        };
        match aggregate {
            Aggregate::Rate => Some("rate"),
            Aggregate::CountOverTime => Some("count_over_time"),
            Aggregate::SumOverTime(_) => Some("sum_over_time"),
            Aggregate::AvgOverTime(_) => Some("avg_over_time"),
            Aggregate::MinOverTime(_) => Some("min_over_time"),
            Aggregate::MaxOverTime(_) => Some("max_over_time"),
            Aggregate::HistogramOverTime(_) => Some("histogram_over_time"),
            Aggregate::QuantileOverTime { .. } => Some("quantile_over_time"),
            _ => None,
        }
    })
}

/// The `TraceQL` metrics query string.
///
/// Tempo accepts both `q` and `query` on the metrics endpoints. The Explore
/// `TraceQL` editor and the HTTP API send `q`. The Grafana Tempo datasource
/// that powers the Traces Drilldown app sends `query`. This accepts either, and
/// prefers `q`.
pub(crate) fn metrics_query_param(uri: &Uri) -> Option<String> {
    query_param(uri, "q").or_else(|| query_param(uri, "query"))
}

/// What every querier read handler extracts from its request: the app state,
/// the authenticated principal, the headers, and the URI.
pub(crate) struct QuerierRequest<S: SpanStore> {
    pub(crate) state: AppState<S>,
    pub(crate) principal: Principal,
    pub(crate) headers: HeaderMap,
    pub(crate) uri: Uri,
}

impl<S> FromRequestParts<AppState<S>> for QuerierRequest<S>
where
    S: SpanStore + 'static,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState<S>,
    ) -> Result<Self, Self::Rejection> {
        let Extension(principal) = Extension::<Principal>::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self {
            state: state.clone(),
            principal,
            headers: parts.headers.clone(),
            uri: parts.uri.clone(),
        })
    }
}

impl<S: SpanStore> QuerierRequest<S> {
    /// The tenant and the `TraceQL` metrics query this request names, or the
    /// response that rejects it.
    pub(crate) fn metrics_request(&self) -> Result<super::MetricsRequest, Box<Response>> {
        metrics_request(self.tenant_request())
    }

    /// What this request carries that names its tenant.
    pub(crate) fn tenant_request(&self) -> TenantRequest<'_> {
        TenantRequest {
            headers: &self.headers,
            principal: &self.principal,
            policy: &self.state.cfg.tenant_policy,
            uri: &self.uri,
        }
    }
}

pub(crate) async fn query_instant<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = query_instant_inner(&request).await;
    request.state.record_query("query", resp.status(), start);
    resp
}

pub(crate) async fn query_range<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = query_range_inner(&request).await;
    request
        .state
        .record_query("query_range", resp.status(), start);
    resp
}

pub(crate) async fn query_range_inner<S>(request: &QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest { state, uri, .. } = request;
    let metrics = match request.metrics_request() {
        Ok(request) => request,
        Err(rejection) => return *rejection,
    };
    let time_range = match required_time_range(uri) {
        Ok(range) => range,
        Err(rejection) => return *rejection,
    };
    let limits = state.cfg.limits_for_tenant(&metrics.tenant);
    if let Err(err) =
        QueryEnforcer::check_search_duration(&limits, time_range.start_ns, time_range.end_ns)
    {
        return limit_error_response(&err);
    }
    let step_ns = match step_param(
        uri,
        UnixNano(time_range.start_ns),
        UnixNano(time_range.end_ns),
    ) {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let bounds = match tempo_metric_bounds(time_range.start_ns, time_range.end_ns, step_ns) {
        Ok(bounds) => bounds,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let exemplar_selection = exemplar_selection(uri);
    let mut scan_options = match scan_options_param(uri) {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    // Public requests and the frontend's single unrestricted Live job need
    // final label decoding; explicit per-block jobs retain raw worker labels.
    scan_options.tempo_frontend_labels = scan_options.job.is_none();
    let scan_bounds = bounds
        .as_ref()
        .map_or(time_range, |bounds| super::TimeBounds {
            start_ns: bounds.scan_start_ns,
            end_ns: bounds.scan_end_ns,
        });

    match state
        .engine
        .query_range_with_options(
            metrics.tenant.as_str(),
            &metrics.query,
            scan_bounds.start_ns,
            scan_bounds.end_ns,
            step_ns,
            scan_options,
        )
        .await
    {
        Ok(mut resp) => {
            if let Some(bounds) = bounds {
                if let Err(err) = bounds.shift_points(&mut resp) {
                    return status_text_response(StatusCode::BAD_REQUEST, err);
                }
            } else {
                resp.series.clear();
            }
            Json(trace_metrics_json(
                &filter_metrics_exemplars(resp, exemplar_selection),
                &metrics.query,
            ))
            .into_response()
        }
        Err(err) => status_text_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

/// The querier router with the default limits, and no readiness gate.
///
/// A role with no gates is ready from construction, so `/ready` and `/status`
/// answer `200` at once. That is the honest answer for an in-process querier
/// whose whole startup already ran before the caller got the router. The role
/// binary calls [`router_with_config_and_metrics`](super::router_with_config_and_metrics)
/// with the gates its own start has to clear.
pub fn router<S>(engine: Arc<TraceqlEngine<S>>) -> Router
where
    S: SpanStore + 'static,
{
    router_with_config(engine, HttpConfig::default(), RoleReadiness::new())
}

/// The querier router, with `cfg` for the query limits and `readiness` for
/// what `/ready` and `/status` report.
pub fn router_with_config<S>(
    engine: Arc<TraceqlEngine<S>>,
    cfg: HttpConfig,
    readiness: RoleReadiness,
) -> Router
where
    S: SpanStore + 'static,
{
    router_with_state(
        AppState {
            engine,
            cfg,
            metrics: None,
        },
        readiness,
    )
}

/// Like [`router_with_config`](super::router_with_config), but it also threads
/// a [`ServiceMetrics`] bundle. Each query handler then records
/// `query_requests` and `query_duration_seconds`.
pub fn router_with_config_and_metrics<S>(
    engine: Arc<TraceqlEngine<S>>,
    cfg: HttpConfig,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
) -> Router
where
    S: SpanStore + 'static,
{
    router_with_state(
        AppState {
            engine,
            cfg,
            metrics: Some(metrics),
        },
        readiness,
    )
}

pub(crate) fn router_with_state<S>(state: AppState<S>, readiness: RoleReadiness) -> Router
where
    S: SpanStore + 'static,
{
    tempo_query_routes!(
        Router::new();
        echo = get(echo),
        buildinfo = get(buildinfo),
        overrides = get(overrides::<S>).post(overrides::<S>).patch(overrides::<S>).delete(overrides::<S>),
        search = get(search::<S>),
        search_stream = get(search_stream::<S>),
        trace_v1 = get(trace_by_id_v1::<S>),
        trace_v2 = get(trace_by_id::<S>),
        tags_v1 = get(search_tags::<S>),
        tags_v2 = get(search_tags_v2::<S>),
        tag_values_v1 = get(search_tag_values::<S>),
        tag_values_v2 = get(search_tag_values_v2::<S>),
        metrics_range = get(query_range::<S>),
        metrics_instant = get(query_instant::<S>),
    )
    .merge(tempo_readiness_routes(readiness))
    .with_state(state)
}

pub(crate) async fn query_instant_inner<S>(request: &QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    instant_metrics_response(request)
        .await
        .unwrap_or_else(|rejection| *rejection)
}

/// The instant metrics answer, or the response that rejects the request.
async fn instant_metrics_response<S>(request: &QuerierRequest<S>) -> Result<Response, Box<Response>>
where
    S: SpanStore + 'static,
{
    let QuerierRequest { state, uri, .. } = request;
    let metrics = request.metrics_request()?;
    let bounds = instant_metric_bounds(uri).map_err(|err| Box::new(bad_request_response(err)))?;
    let mut scan_options =
        scan_options_param(uri).map_err(|err| Box::new(bad_request_response(err)))?;
    // The public frontend dispatches one unrestricted job, so reduction can
    // apply final label decoding without merging already reduced averages.
    scan_options.tempo_frontend_labels = scan_options.job.is_none();
    scan_options.tempo_instant_metrics = true;

    Ok(
        match state
            .engine
            .query_range_with_options(
                metrics.tenant.as_str(),
                &metrics.query,
                bounds.start_ns,
                bounds.end_ns,
                bounds.step_ns,
                scan_options,
            )
            .await
        {
            Ok(resp) => Json(trace_metrics_instant_json(&resp, &metrics.query)).into_response(),
            Err(err) => status_text_response(StatusCode::BAD_REQUEST, err.to_string()),
        },
    )
}

fn bad_request_response(err: String) -> Response {
    status_text_response(StatusCode::BAD_REQUEST, err)
}
