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

async fn label_values(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_label_values_query(&state, &headers, &name, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("label_values", resp.status().is_success(), start);
    resp
}

async fn label_values_post(
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
    let params = match parse_series_params(Some(&raw_query)) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_label_values_query(&state, &headers, &name, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn series(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => match execute_series_query(&state, &headers, &params).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Err(error) => error.into_response(),
    };
    state.record_query("series", resp.status().is_success(), start);
    resp
}

async fn series_post(
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
    match execute_series_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_label_names(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_label_names_post(
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
    match execute_api_prom_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_label_values(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(_name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_label_values_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    Path(_name): Path<String>,
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
    match execute_api_prom_label_names_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_series(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute_api_prom_series_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn api_prom_series_post(
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
    match execute_api_prom_series_query(&state, &headers, &params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn index_stats(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match execute_index_stats_query(&state, &headers, raw_query.as_deref()).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    };
    state.record_query("index_stats", resp.status().is_success(), start);
    resp
}

async fn index_stats_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_index_stats_query(&state, &headers, Some(&raw_query)).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn index_volume(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match execute_index_volume_query(
        &state,
        &headers,
        raw_query.as_deref(),
        VolumeKind::Instant,
    )
    .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    };
    state.record_query("index_volume", resp.status().is_success(), start);
    resp
}

async fn index_volume_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_index_volume_query(&state, &headers, Some(&raw_query), VolumeKind::Instant).await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn index_volume_range(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_index_volume_query(&state, &headers, raw_query.as_deref(), VolumeKind::Range)
        .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn index_volume_range_post(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let raw_query = match post_query_params_body_first(raw_query.as_deref(), &body) {
        Ok(raw_query) => raw_query,
        Err(error) => return error.into_response(),
    };
    match execute_index_volume_query(&state, &headers, Some(&raw_query), VolumeKind::Range).await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn tail(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let params = match parse_query_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match prepare_http_tail(&state, &headers, &params).await {
        Ok(tail) => ws
            .on_upgrade(move |socket| send_tail_stream(socket, tail))
            .into_response(),
        Err(error) => error.into_response(),
    }
}

async fn handle_query(
    state: QuerierState,
    headers: HeaderMap,
    raw_query: Option<&str>,
    kind: QueryKind,
) -> Response {
    let wants_parquet = wants_loki_parquet(&headers);
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match execute_http_query(&state, &headers, params, kind).await {
        Ok(value) if wants_parquet => match loki_parquet_response(&value) {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

async fn handle_api_prom_query(
    state: QuerierState,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match execute_http_query(&state, &headers, params, QueryKind::Instant).await {
        Ok(value) => api_prom_streams_only_response(&value),
        Err(error) => error.into_response(),
    }
}

async fn handle_api_prom_query_range(
    state: QuerierState,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match execute_http_query(&state, &headers, params, QueryKind::Range).await {
        Ok(value) => api_prom_streams_only_response(&value),
        Err(error) => error.into_response(),
    }
}

fn api_prom_streams_only_response(value: &Value) -> Response {
    if value.pointer("/data/resultType").and_then(Value::as_str) == Some("streams") {
        json_response(StatusCode::OK, value)
    } else {
        text_response(
            StatusCode::BAD_REQUEST,
            "rpc error: code = Code(400) desc = legacy endpoints only support streams result type",
        )
    }
}

async fn execute_http_query(
    state: &QuerierState,
    headers: &HeaderMap,
    params: QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    let tenants = authorized_tenants(state, headers).await?;
    if tenants.len() > 1 {
        return execute_http_multi_tenant_query(state, &tenants, &params, kind).await;
    }
    execute_http_query_for_tenant(state, &tenants[0], &params, kind).await
}

async fn execute_http_multi_tenant_query(
    state: &QuerierState,
    tenants: &[String],
    params: &QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    reject_signed_vector_function_literal(&params.query)?;
    if let Some(result) = scalar_vector_expression_result(&params.query) {
        let time_range = time_range(params, kind)?;
        validate_loki_range_query_range_limit(kind, time_range)?;
        validate_loki_query_range_resolution(params, kind, time_range)?;
        let value = match kind {
            QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
            QueryKind::Range => loki_range_vector_response(
                time_range,
                resolved_range_step(params.step, time_range)?,
                result,
            ),
        };
        return Ok(add_loki_query_stats(value));
    }

    let mut merged = None;
    for tenant in tenants {
        let response = execute_http_query_for_tenant(state, tenant, params, kind).await?;
        match &mut merged {
            Some(merged) => merge_loki_query_response(merged, &response),
            None => merged = Some(response),
        }
    }
    Ok(merged.unwrap_or_else(|| {
        add_loki_query_stats(loki_success_value(json!({
            "resultType": "streams",
            "result": []
        })))
    }))
}

async fn execute_http_query_for_tenant(
    state: &QuerierState,
    tenant: &str,
    params: &QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    let time_range = time_range(params, kind)?;
    validate_loki_range_query_range_limit(kind, time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    validate_loki_query_range_resolution(params, kind, time_range)?;
    let limit = params.limit;
    let direction = loki_direction(params.direction.as_deref())?;
    let interval = params.interval;
    reject_signed_vector_function_literal(&params.query)?;
    if let Some(result) = scalar_vector_expression_result(&params.query) {
        let value = match kind {
            QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
            QueryKind::Range => loki_range_vector_response(
                time_range,
                resolved_range_step(params.step, time_range)?,
                result,
            ),
        };
        return Ok(add_loki_query_stats(value));
    }
    if let Some(sort) = parse_sort_vector_expression(&params.query) {
        return execute_http_sort_vector_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            sort,
            &params.query,
        )
        .await;
    }
    if let Ok(label_replace) = parse_metric_label_replace_query(&params.query) {
        let mut value = execute_http_metric_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            label_replace.query.clone(),
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            &params.query,
        )?;
        return Ok(value);
    }
    if let Some(binary) = parse_label_replace_metric_binary_expression(&params.query) {
        return execute_http_label_replace_metric_binary_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            binary,
            &params.query,
        )
        .await;
    }
    if let Some(label_replace) = parse_label_replace_expression(&params.query) {
        let mut value = execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            &label_replace.query,
            &params.query,
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            &params.query,
        )?;
        return Ok(value);
    }
    if let Ok(label_join) = parse_metric_label_join_query(&params.query) {
        let mut value = execute_http_metric_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            label_join.query.clone(),
        )
        .await?;
        apply_label_join_to_loki_result(&mut value, &label_join);
        return Ok(value);
    }
    execute_http_remaining_query(
        state,
        tenant,
        params,
        kind,
        time_range,
        (direction, limit, interval),
    )
    .await
}

async fn execute_http_remaining_query(
    state: &QuerierState,
    tenant: &str,
    params: &QueryParams,
    kind: QueryKind,
    time_range: TimeRange,
    stream_options: (LokiDirection, Option<usize>, Option<i64>),
) -> Result<Value, HttpQueryError> {
    let (direction, limit, interval) = stream_options;
    if let Some(inner_query) = strip_outer_parenthesized_expression(&params.query) {
        return execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            inner_query,
            &params.query,
        )
        .await;
    }
    if let Some(arithmetic) = parse_metric_vector_arithmetic_expression(&params.query) {
        return execute_http_metric_vector_arithmetic_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            arithmetic,
            &params.query,
        )
        .await;
    }
    if let Some(comparison) = parse_metric_vector_comparison_expression(&params.query) {
        return execute_http_metric_vector_comparison_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            comparison,
            &params.query,
        )
        .await;
    }
    if let Some(set) = parse_metric_vector_set_expression(&params.query) {
        return execute_http_metric_vector_set_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            set,
            &params.query,
        )
        .await;
    }
    if let Ok(arithmetic) = parse_metric_binary_arithmetic_query(&params.query) {
        return execute_http_metric_binary_arithmetic_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            arithmetic,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_binary_comparison_query(&params.query) {
        return execute_http_metric_binary_comparison_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            comparison,
        )
        .await;
    }
    if let Ok(set) = parse_metric_binary_set_query(&params.query) {
        return execute_http_metric_binary_set_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            set,
        )
        .await;
    }
    if let Ok(arithmetic) = parse_metric_scalar_arithmetic_query(&params.query) {
        return execute_http_metric_scalar_arithmetic_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            arithmetic,
            &params.query,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_scalar_comparison_query(&params.query) {
        return execute_http_metric_scalar_comparison_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            comparison,
            &params.query,
        )
        .await;
    }
    let value = if let Ok(query) = parse_metric_query(&params.query) {
        execute_http_metric_query(state, tenant, time_range, params.step, kind, query).await?
    } else {
        execute_http_stream_query(
            state,
            &params.query,
            tenant,
            time_range,
            (
                direction,
                limit,
                interval,
                if matches!(kind, QueryKind::Range) {
                    Some(time_range.end_ns)
                } else {
                    None
                },
            ),
        )
        .await
        .map_err(|error| match error {
            HttpQueryError::Parse(source) => HttpQueryError::LokiParse {
                query: params.query.clone(),
                source,
            },
            error => error,
        })?
    };

    Ok(add_loki_query_stats(value))
}

