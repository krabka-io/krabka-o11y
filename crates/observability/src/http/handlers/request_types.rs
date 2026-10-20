use axum::{
    extract::{FromRequest, FromRequestParts, Request},
    response::IntoResponse,
};

use super::{MetadataQuery, record_metadata_query};
use crate::{
    BTreeSet, Bytes, HeaderMap, HttpQueryError, Instant, Path, QuerierState, QueryKind, RawQuery,
    RequestSecurity, Response, State, StatusCode, Value, execute_detected_field_values_query,
    execute_detected_fields_query, execute_detected_labels_query, execute_format_query,
    execute_label_names_query, execute_patterns_query, handle_api_prom_query,
    handle_api_prom_query_range, handle_query, json, json_response, loki_success,
    parse_posted_series_params, post_query_params, post_query_params_body_first,
};

async fn handle_instant_or_range_query(
    state: QuerierState,
    security: RequestSecurity,
    headers: HeaderMap,
    raw_query: Option<&str>,
    kind: QueryKind,
    metric_name: &'static str,
) -> Response {
    let start = Instant::now();
    let response = handle_query(state.clone(), security, headers, raw_query, kind).await;
    state.record_query(metric_name, response.status(), start);
    response
}

pub(crate) async fn api_prom_query_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let raw_query = request.body_first_query()?;
    handle_api_prom_query(state, request.security, request.headers, Some(&raw_query)).await
}

pub(crate) async fn api_prom_query_range_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let raw_query = request.body_first_query()?;
    handle_api_prom_query_range(state, request.security, request.headers, Some(&raw_query)).await
}

pub(crate) async fn format_query(RawQuery(raw_query): RawQuery) -> Response {
    match execute_format_query(raw_query.as_deref()) {
        Ok(formatted) => loki_success(formatted),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn label_names_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let params = request.series_params()?;
    execute_label_names_query(&state, &request.security, &request.headers, &params).await
}

pub(crate) async fn query_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Response {
    handle_posted_query(state, request, QueryKind::Instant).await
}

pub(crate) async fn query_range_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Response {
    handle_posted_query(state, request, QueryKind::Range).await
}

pub(crate) async fn api_prom_query(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, HttpQueryError> {
    handle_api_prom_query(state, security, headers, raw_query.as_deref()).await
}

pub(crate) async fn api_prom_query_range(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, HttpQueryError> {
    handle_api_prom_query_range(state, security, headers, raw_query.as_deref()).await
}

pub(crate) async fn build_info() -> Response {
    let value = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": "unknown",
        "branch": "unknown",
        "buildDate": "",
        "buildUser": "krabka",
        "goVersion": "not-go",
    });
    json_response(StatusCode::OK, &value)
}

pub(crate) async fn detected_field_values(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_field_values_query(
        &state,
        &security,
        &headers,
        &name,
        raw_query.as_deref(),
    )
    .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn detected_field_values_post(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_detected_field_values_query(&state, &security, &headers, &name, Some(&raw_query))
        .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn detected_fields(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_fields_query(&state, &security, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn detected_fields_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let raw_query = request.body_first_query()?;
    let value = execute_detected_fields_query(
        &state,
        &request.security,
        &request.headers,
        Some(&raw_query),
    )
    .await?;
    Ok(json_response(StatusCode::OK, &value))
}

pub(crate) async fn detected_labels(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_labels_query(&state, &security, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn detected_labels_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let raw_query = request.body_first_query()?;
    let value = execute_detected_labels_query(
        &state,
        &request.security,
        &request.headers,
        Some(&raw_query),
    )
    .await?;
    Ok(json_response(StatusCode::OK, &value))
}

pub(crate) async fn format_query_post(RawQuery(raw_query): RawQuery, body: Bytes) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_format_query(Some(&raw_query)) {
        Ok(formatted) => loki_success(formatted),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn patterns(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_patterns_query(&state, &security, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn patterns_post(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_patterns_query(&state, &security, &headers, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn query(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_instant_or_range_query(
        state.clone(),
        security,
        headers,
        raw_query.as_deref(),
        QueryKind::Instant,
        "query",
    )
    .await
}

pub(crate) async fn query_range(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_instant_or_range_query(
        state.clone(),
        security,
        headers,
        raw_query.as_deref(),
        QueryKind::Range,
        "query_range",
    )
    .await
}

pub(crate) fn status_metrics(component: &'static str) -> Response {
    // `Loki` sets this to 1 on any process that runs its compactor, which for
    // Krabka means the role that writes durable storage -- the block builder
    // -- and the all-in-one that contains it. A `Loki` dashboard keyed on this
    // gauge would otherwise report a stack with no compactor running at all.
    let compactor_running = usize::from(matches!(component, "compactor" | "all"));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(format!(
            "# HELP loki_build_info A metric with a constant '1' value labeled by version, revision, branch, goversion from which loki was built, and the goos and goarch for the build.\n\
             # TYPE loki_build_info gauge\n\
             loki_build_info{{branch=\"unknown\",goarch=\"unknown\",goos=\"unknown\",goversion=\"unknown\",revision=\"unknown\",tags=\"\",version=\"{}\"}} 1\n\
             # HELP loki_boltdb_shipper_compactor_running Value will be 1 if compactor is currently running on this instance\n\
             # TYPE loki_boltdb_shipper_compactor_running gauge\n\
             loki_boltdb_shipper_compactor_running {compactor_running}\n\
             # HELP krabka_observability_service_up Whether the observability service is running.\n\
             # TYPE krabka_observability_service_up gauge\n\
             krabka_observability_service_up{{component=\"{component}\"}} 1\n",
            env!("CARGO_PKG_VERSION")
        ).into())
        .expect("status metrics response headers are valid")
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DetectedFieldStats {
    pub(crate) ty: DetectedFieldType,
    pub(crate) values: BTreeSet<String>,
    pub(crate) parsers: BTreeSet<&'static str>,
}

impl DetectedFieldStats {
    pub(crate) fn new(ty: DetectedFieldType, value: String, parser: &'static str) -> Self {
        Self {
            ty,
            values: BTreeSet::from([value]),
            parsers: BTreeSet::from([parser]),
        }
    }

    pub(crate) fn new_generated(ty: DetectedFieldType, value: String) -> Self {
        Self {
            ty,
            values: BTreeSet::from([value]),
            parsers: BTreeSet::new(),
        }
    }

    pub(crate) fn add(&mut self, ty: DetectedFieldType, value: String, parser: &'static str) {
        self.ty = self.ty.merge(ty);
        self.values.insert(value);
        self.parsers.insert(parser);
    }

    pub(crate) fn add_generated(&mut self, ty: DetectedFieldType, value: String) {
        self.ty = self.ty.merge(ty);
        self.values.insert(value);
    }

    pub(crate) fn parsers_json(self) -> Value {
        if self.parsers.is_empty() {
            Value::Null
        } else {
            json!(self.parsers.into_iter().collect::<Vec<_>>())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DetectedFieldType {
    Boolean,
    Int,
    Float,
    Duration,
    Bytes,
    String,
}

impl DetectedFieldType {
    pub(crate) fn merge(self, other: Self) -> Self {
        if self == Self::String || other == Self::String {
            Self::String
        } else if self == Self::Bytes && other == Self::Bytes {
            Self::Bytes
        } else if self == Self::Duration && other == Self::Duration {
            Self::Duration
        } else if self == Self::Float || other == Self::Float {
            Self::Float
        } else if self == Self::Int && other == Self::Int {
            Self::Int
        } else if self == Self::Boolean && other == Self::Boolean {
            Self::Boolean
        } else {
            Self::String
        }
    }

    pub(crate) fn as_loki_str(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Float => "float",
            Self::Duration => "duration",
            Self::Bytes => "bytes",
            Self::String => "string",
        }
    }
}

#[derive(Debug)]
pub(crate) struct DetectedFieldsParams {
    pub(crate) query: String,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) limit: usize,
    pub(crate) line_limit: usize,
}

#[derive(Debug)]
pub(crate) struct DetectedLabelsParams {
    pub(crate) query: Option<String>,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) limit: usize,
}

#[derive(Debug)]
pub(crate) struct PatternsParams {
    pub(crate) query: String,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) step: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct QueryParams {
    pub(crate) query: String,
    pub(crate) time: Option<i64>,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
    pub(crate) since: Option<i64>,
    pub(crate) step: Option<i64>,
    pub(crate) interval: Option<i64>,
    pub(crate) limit: Option<usize>,
    pub(crate) direction: Option<String>,
    pub(crate) delay_for: Option<i64>,
}

#[derive(Debug, Default)]
pub(crate) struct SeriesParams {
    pub(crate) matchers: Vec<String>,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
    pub(crate) since: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VolumeAggregateBy {
    Series,
    Labels,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VolumeKind {
    Instant,
    Range,
}

#[derive(Debug)]
pub(crate) struct VolumeParams {
    pub(crate) query: String,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) step: Option<i64>,
    pub(crate) limit: usize,
    pub(crate) target_labels: Option<Vec<String>>,
    pub(crate) aggregate_by: VolumeAggregateBy,
}

pub(crate) async fn label_names(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    record_metadata_query(
        state,
        security,
        headers,
        raw_query,
        "labels",
        MetadataQuery::LabelNames,
    )
    .await
}

/// The extracted parts of a Loki query POST: the URL query string and the
/// form body that carries the remaining parameters.
pub(crate) struct PostedQueryRequest {
    pub security: RequestSecurity,
    pub headers: HeaderMap,
    pub raw_query: Option<String>,
    pub body: Bytes,
}

impl PostedQueryRequest {
    /// The request's parameters as one query string, the form body's taking
    /// precedence over the URL's.
    pub(crate) fn body_first_query(&self) -> Result<String, HttpQueryError> {
        post_query_params_body_first(self.raw_query.as_deref(), &self.body)
    }

    /// The request's parameters, read as a series or label-names request.
    pub(crate) fn series_params(&self) -> Result<SeriesParams, HttpQueryError> {
        parse_posted_series_params(self.raw_query.as_deref(), &self.body)
    }
}

/// Extracts the parts in the order a handler taking `RequestSecurity`,
/// `HeaderMap`, `RawQuery` and `Bytes` would, so a request without a
/// principal is refused before its body is read.
impl<S: Send + Sync> FromRequest<S> for PostedQueryRequest {
    type Rejection = Response;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let request_parts = request.into_parts();
        let mut parts = request_parts.0;
        let body = request_parts.1;
        let security = RequestSecurity::from_request_parts(&mut parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let headers = parts.headers.clone();
        let raw_query = parts.uri.query().map(ToOwned::to_owned);
        let body = Bytes::from_request(Request::from_parts(parts, body), state)
            .await
            .map_err(IntoResponse::into_response)?;
        Ok(Self {
            security,
            headers,
            raw_query,
            body,
        })
    }
}

/// Runs a posted `query` or `query_range` request and records its outcome
/// under the route label of `kind`.
pub(crate) async fn handle_posted_query(
    state: QuerierState,
    request: PostedQueryRequest,
    kind: QueryKind,
) -> Response {
    let start = Instant::now();
    let route = kind.route_label();
    let resp = match request.body_first_query() {
        Ok(raw_query) => {
            // Boxed so the handler's own future stays small; the query
            // future is the large part of it.
            Box::pin(handle_query(
                state.clone(),
                request.security,
                request.headers,
                Some(&raw_query),
                kind,
            ))
            .await
        }
        Err(error) => error.into_response(),
    };
    state.record_query(route, resp.status(), start);
    resp
}
