fn status_metrics(component: &'static str) -> Response {
    let compactor_running = usize::from(component == "compactor");
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        format!(
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
        ),
    )
        .into_response()
}

async fn build_info() -> Response {
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

#[derive(Debug)]
struct QueryParams {
    query: String,
    time: Option<i64>,
    start: Option<i64>,
    end: Option<i64>,
    since: Option<i64>,
    step: Option<i64>,
    interval: Option<i64>,
    limit: Option<usize>,
    direction: Option<String>,
    delay_for: Option<i64>,
}

#[derive(Debug, Default)]
struct SeriesParams {
    matchers: Vec<String>,
    start: Option<i64>,
    end: Option<i64>,
    since: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VolumeKind {
    Instant,
    Range,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VolumeAggregateBy {
    Series,
    Labels,
}

#[derive(Debug)]
struct VolumeParams {
    query: String,
    start: i64,
    end: i64,
    step: Option<i64>,
    limit: usize,
    target_labels: Option<Vec<String>>,
    aggregate_by: VolumeAggregateBy,
}

#[derive(Debug)]
struct DetectedFieldsParams {
    query: String,
    start: i64,
    end: i64,
    limit: usize,
    line_limit: usize,
}

#[derive(Debug)]
struct DetectedLabelsParams {
    query: Option<String>,
    start: i64,
    end: i64,
    limit: usize,
}

#[derive(Debug)]
struct PatternsParams {
    query: String,
    start: i64,
    end: i64,
    step: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DetectedFieldType {
    Boolean,
    Int,
    Float,
    Duration,
    Bytes,
    String,
}

impl DetectedFieldType {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::String, _) | (_, Self::String) => Self::String,
            (Self::Bytes, Self::Bytes) => Self::Bytes,
            (Self::Duration, Self::Duration) => Self::Duration,
            (Self::Float, _) | (_, Self::Float) => Self::Float,
            (Self::Int, Self::Int) => Self::Int,
            (Self::Boolean, Self::Boolean) => Self::Boolean,
            _ => Self::String,
        }
    }

    fn as_loki_str(self) -> &'static str {
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

#[derive(Debug, PartialEq, Eq)]
struct DetectedFieldStats {
    ty: DetectedFieldType,
    values: BTreeSet<String>,
    parsers: BTreeSet<&'static str>,
}

impl DetectedFieldStats {
    fn new(ty: DetectedFieldType, value: String, parser: &'static str) -> Self {
        Self {
            ty,
            values: BTreeSet::from([value]),
            parsers: BTreeSet::from([parser]),
        }
    }

    fn new_generated(ty: DetectedFieldType, value: String) -> Self {
        Self {
            ty,
            values: BTreeSet::from([value]),
            parsers: BTreeSet::new(),
        }
    }

    fn add(&mut self, ty: DetectedFieldType, value: String, parser: &'static str) {
        self.ty = self.ty.merge(ty);
        self.values.insert(value);
        self.parsers.insert(parser);
    }

    fn add_generated(&mut self, ty: DetectedFieldType, value: String) {
        self.ty = self.ty.merge(ty);
        self.values.insert(value);
    }

    fn parsers_json(self) -> Value {
        if self.parsers.is_empty() {
            Value::Null
        } else {
            json!(self.parsers.into_iter().collect::<Vec<_>>())
        }
    }
}

async fn query(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = handle_query(
        state.clone(),
        headers,
        raw_query.as_deref(),
        QueryKind::Instant,
    )
    .await;
    state.record_query("query", resp.status().is_success(), start);
    resp
}

async fn query_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => {
            let resp = error.into_response();
            state.record_query("query", resp.status().is_success(), start);
            return resp;
        }
    };
    let resp = handle_query(state.clone(), headers, Some(&raw_query), QueryKind::Instant).await;
    state.record_query("query", resp.status().is_success(), start);
    resp
}

async fn api_prom_query(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_api_prom_query(state, headers, raw_query.as_deref()).await
}

async fn api_prom_query_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    handle_api_prom_query(state, headers, Some(&raw_query)).await
}

async fn api_prom_query_range(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    handle_api_prom_query_range(state, headers, raw_query.as_deref()).await
}

async fn api_prom_query_range_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    handle_api_prom_query_range(state, headers, Some(&raw_query)).await
}

async fn query_range(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = handle_query(
        state.clone(),
        headers,
        raw_query.as_deref(),
        QueryKind::Range,
    )
    .await;
    state.record_query("query_range", resp.status().is_success(), start);
    resp
}

async fn query_range_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let start = Instant::now();
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => {
            let resp = error.into_response();
            state.record_query("query_range", resp.status().is_success(), start);
            return resp;
        }
    };
    let resp = handle_query(state.clone(), headers, Some(&raw_query), QueryKind::Range).await;
    state.record_query("query_range", resp.status().is_success(), start);
    resp
}

async fn format_query(RawQuery(raw_query): RawQuery) -> Response {
    match execute_format_query(raw_query.as_deref()) {
        Ok(formatted) => loki_success(formatted),
        Err(error) => error.into_response(),
    }
}

async fn format_query_post(RawQuery(raw_query): RawQuery, body: Bytes) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_format_query(Some(&raw_query)) {
        Ok(formatted) => loki_success(formatted),
        Err(error) => error.into_response(),
    }
}

async fn patterns(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_patterns_query(&state, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn patterns_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_patterns_query(&state, &headers, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_fields(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_fields_query(&state, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_fields_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_detected_fields_query(&state, &headers, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_labels(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_labels_query(&state, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_labels_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_detected_labels_query(&state, &headers, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_field_values(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_detected_field_values_query(&state, &headers, &name, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn detected_field_values_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_detected_field_values_query(&state, &headers, &name, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn label_names(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_label_names_query(&state, &headers, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("labels", resp.status().is_success(), start);
    resp
}

async fn label_names_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    let params = match parse_series_params(Some(&raw_query)) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

