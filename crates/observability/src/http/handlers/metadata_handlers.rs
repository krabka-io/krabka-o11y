use std::future::Future;

use axum::response::IntoResponse;

use super::{MetadataQuery, record_metadata_query};
use crate::{
    Bytes, HeaderMap, HttpQueryError, Instant, LabelValuesRequest, LogqlExpr, LokiStreamEncoding,
    Path, PostedQueryRequest, QuerierState, QueryKind, QueryParams, RawQuery, RequestSecurity,
    Response, SeriesParams, State, StatusCode, TenantErrorSurface, TenantId, Value, VolumeKind,
    WebSocketUpgrade, add_loki_encoding_flags, add_loki_query_stats, authorized_tenants,
    execute_api_prom_label_names_query, execute_api_prom_series_query,
    execute_http_query_for_tenant, execute_http_query_for_tenant_inner, execute_index_shards_query,
    execute_index_stats_query, execute_index_volume_query, execute_label_values_query,
    execute_series_query, json, json_response, loki_encoding_flags,
    loki_instant_scalar_or_vector_response, loki_parquet_response, loki_range_vector_response,
    loki_stream_encoding_for_headers, loki_success_value, merge_loki_query_response,
    parse_logql_expr, parse_posted_series_params, parse_query, parse_query_params,
    parse_series_params, prepare_http_tail, reject_signed_vector_function_literal,
    resolved_range_step, scalar_vector_expression_result, send_tail_stream, text_response,
    time_range, validate_loki_query_range_resolution, validate_loki_range_query_range_limit,
    wants_loki_parquet,
};

async fn api_prom_series_query<F, Fut>(raw_query: Option<&str>, execute: F) -> Response
where
    F: FnOnce(SeriesParams) -> Fut,
    Fut: Future<Output = Result<Response, HttpQueryError>>,
{
    let params = match parse_series_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    match execute(params).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

async fn execute_label_values_response(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    label_name: &str,
    series_params: &SeriesParams,
) -> Response {
    match execute_label_values_query(
        state,
        LabelValuesRequest {
            security,
            headers,
            label_name,
            series_params,
        },
    )
    .await
    {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn api_prom_label_names_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let params = request.series_params()?;
    execute_api_prom_label_names_query(&state, &request.security, &request.headers, &params).await
}

pub(crate) async fn api_prom_series_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let params = request.series_params()?;
    execute_api_prom_series_query(&state, &request.security, &request.headers, &params).await
}

pub(crate) fn api_prom_streams_only_response(value: &Value) -> Response {
    if value.pointer("/data/resultType").and_then(Value::as_str) == Some("streams") {
        json_response(StatusCode::OK, value)
    } else {
        text_response(
            StatusCode::BAD_REQUEST,
            "rpc error: code = Code(400) desc = legacy endpoints only support streams result type",
        )
    }
}

pub(crate) async fn index_volume_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    posted_index_volume_response(&state, &request, VolumeKind::Instant).await
}

pub(crate) async fn index_volume_range_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    posted_index_volume_response(&state, &request, VolumeKind::Range).await
}

pub(crate) async fn series_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let params = request.series_params()?;
    execute_series_query(&state, &request.security, &request.headers, &params).await
}

pub(crate) async fn api_prom_label_names(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    api_prom_series_query(raw_query.as_deref(), |params| async move {
        execute_api_prom_label_names_query(&state, &security, &headers, &params).await
    })
    .await
}

/// Loki's legacy `/api/prom/label/{name}/values` answers exactly as
/// `/api/prom/label` does, so the path's label name is unused.
pub(crate) async fn api_prom_label_values(
    state: State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(_name): Path<String>,
    raw_query: RawQuery,
) -> Response {
    api_prom_label_names(state, security, headers, raw_query).await
}

/// Loki's legacy `/api/prom/label/{name}/values` answers a POST exactly as
/// `/api/prom/label` does, so the path's label name is unused.
pub(crate) async fn api_prom_label_values_post(
    state: State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(_name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Result<Response, HttpQueryError> {
    let request = PostedQueryRequest {
        security,
        headers,
        raw_query,
        body,
    };
    api_prom_label_names_post(state, request).await
}

pub(crate) async fn api_prom_series(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    api_prom_series_query(raw_query.as_deref(), |params| async move {
        execute_api_prom_series_query(&state, &security, &headers, &params).await
    })
    .await
}

pub(crate) async fn handle_api_prom_query(
    state: QuerierState,
    security: RequestSecurity,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Result<Response, HttpQueryError> {
    let params = parse_query_params(raw_query)?;
    match execute_http_query(&state, &security, &headers, params, QueryKind::Instant).await {
        Ok(value) => Ok(api_prom_streams_only_response(&value)),
        // The legacy endpoint reaches Loki's querier over gRPC for every query
        // it accepts, so a tenant error keeps the gRPC status prefix.
        Err(HttpQueryError::Tenant { source, .. }) => Err(HttpQueryError::Tenant {
            source,
            surface: TenantErrorSurface::QuerierRead,
        }),
        Err(error) => Err(error),
    }
}

pub(crate) async fn handle_api_prom_query_range(
    state: QuerierState,
    security: RequestSecurity,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> Result<Response, HttpQueryError> {
    let params = parse_query_params(raw_query)?;
    let value = execute_http_query(&state, &security, &headers, params, QueryKind::Range).await?;
    Ok(api_prom_streams_only_response(&value))
}

pub(crate) async fn index_stats(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp =
        match execute_index_stats_query(&state, &security, &headers, raw_query.as_deref()).await {
            Ok(value) => json_response(StatusCode::OK, &value),
            Err(error) => error.into_response(),
        };
    state.record_query("index_stats", resp.status(), start);
    resp
}

pub(crate) async fn index_stats_post(
    State(state): State<QuerierState>,
    request: PostedQueryRequest,
) -> Result<Response, HttpQueryError> {
    let raw_query = request.body_first_query()?;
    let value = execute_index_stats_query(
        &state,
        &request.security,
        &request.headers,
        Some(&raw_query),
    )
    .await?;
    Ok(json_response(StatusCode::OK, &value))
}

pub(crate) async fn index_volume(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match execute_index_volume_query(
        &state,
        &security,
        &headers,
        raw_query.as_deref(),
        VolumeKind::Instant,
    )
    .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    };
    state.record_query("index_volume", resp.status(), start);
    resp
}

pub(crate) async fn index_volume_range(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    match execute_index_volume_query(
        &state,
        &security,
        &headers,
        raw_query.as_deref(),
        VolumeKind::Range,
    )
    .await
    {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn label_values(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let resp = match parse_series_params(raw_query.as_deref()) {
        Ok(params) => {
            execute_label_values_response(&state, &security, &headers, &name, &params).await
        }
        Err(error) => error.into_response(),
    };
    state.record_query("label_values", resp.status(), start);
    resp
}

pub(crate) async fn label_values_post(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    let params = match parse_posted_series_params(raw_query.as_deref(), &body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    execute_label_values_response(&state, &security, &headers, &name, &params).await
}

/// Answers a `POST` to `/index/volume` or `/index/volume_range`, whose form
/// body takes precedence over its URL query string.
pub(crate) async fn posted_index_volume_response(
    state: &QuerierState,
    request: &PostedQueryRequest,
    kind: VolumeKind,
) -> Result<Response, HttpQueryError> {
    let raw_query = request.body_first_query()?;
    let value = execute_index_volume_query(
        state,
        &request.security,
        &request.headers,
        Some(&raw_query),
        kind,
    )
    .await?;
    Ok(json_response(StatusCode::OK, &value))
}

pub(crate) async fn tail(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    let params = match parse_query_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    match prepare_http_tail(&state, &security, &headers, &params).await {
        Ok(tail) => ws
            .on_upgrade(move |socket| send_tail_stream(socket, tail))
            .into_response(),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn execute_http_multi_tenant_query(
    state: &QuerierState,
    tenants: &[TenantId],
    params: &QueryParams,
    kind: QueryKind,
    encoding: LokiStreamEncoding,
) -> Result<Value, HttpQueryError> {
    reject_signed_vector_function_literal(&params.query)?;
    if parse_logql_expr(&params.query)
        .is_ok_and(|expression| federated_metric_expression(&expression))
    {
        let state = state.with_federated_metric_tenants(tenants);
        if let Some(tenant) = tenants.first() {
            // One evaluator and cap for all tenants, without a cache keyed by
            // only the first tenant's identity.
            return execute_http_query_for_tenant_inner(&state, tenant, params, kind, encoding)
                .await;
        }
    }
    if let Some(result) = scalar_vector_expression_result(&params.query) {
        let time_range = time_range(params, kind)?;
        // A scalar expression reads no data, but the window cap still applies,
        // and it applies per tenant: `Loki` takes the smallest of the limits
        // the named tenants carry, so every one of them is checked.
        for tenant in tenants {
            validate_loki_range_query_range_limit(
                &state.with_tenant_limits(tenant),
                kind,
                time_range,
            )?;
        }
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
        let response = execute_http_query_for_tenant(state, tenant, params, kind, encoding).await?;
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

fn federated_metric_expression(expression: &LogqlExpr) -> bool {
    match expression {
        LogqlExpr::Metric { query, .. } => crate::metric_query_uses_approx_topk(query),
        LogqlExpr::Variants { .. }
        | LogqlExpr::Selection {
            approximate: true, ..
        } => true,
        LogqlExpr::Aggregation { expr, .. }
        | LogqlExpr::Sort { expr, .. }
        | LogqlExpr::Selection { expr, .. }
        | LogqlExpr::LabelReplace { expr, .. }
        | LogqlExpr::LabelJoin { expr, .. }
        | LogqlExpr::Vector(expr) => federated_metric_expression(expr),
        LogqlExpr::Arithmetic { left, right, .. }
        | LogqlExpr::Comparison { left, right, .. }
        | LogqlExpr::Set { left, right, .. } => {
            federated_metric_expression(left) || federated_metric_expression(right)
        }
        LogqlExpr::Stream { .. } | LogqlExpr::Scalar(_) => false,
    }
}

pub(crate) async fn execute_http_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    let tenants = authorized_tenants(
        state,
        security,
        headers,
        query_tenant_error_surface(&params, kind),
    )
    .await?;
    let encoding = loki_stream_encoding_for_headers(headers);
    if let [tenant] = tenants.as_slice() {
        return execute_http_query_for_tenant(state, tenant, &params, kind, encoding).await;
    }
    execute_http_multi_tenant_query(state, &tenants, &params, kind, encoding).await
}

// Loki 3.5.1 sends a range log query to its querier over gRPC, so a tenant
// error there comes back with the gRPC status prefix. A metric query answers
// 400 without it, and an instant log query answers 500. A query that is not a
// log query takes the metric answer. For a query that does not parse at all,
// Loki reports the parse error first. Krabka reports the tenant error, with the
// same 400.
fn query_tenant_error_surface(params: &QueryParams, kind: QueryKind) -> TenantErrorSurface {
    if parse_query(&params.query).is_err() {
        TenantErrorSurface::Read
    } else if matches!(kind, QueryKind::Range) {
        TenantErrorSurface::QuerierRead
    } else {
        TenantErrorSurface::InstantLogQuery
    }
}

pub(crate) async fn handle_query(
    state: QuerierState,
    security: RequestSecurity,
    headers: HeaderMap,
    raw_query: Option<&str>,
    kind: QueryKind,
) -> Response {
    let wants_parquet = wants_loki_parquet(&headers);
    let params = match parse_query_params(raw_query) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };

    let flags = loki_encoding_flags(&headers);
    match execute_http_query(&state, &security, &headers, params, kind).await {
        Ok(value) if wants_parquet => match loki_parquet_response(&value) {
            Ok(response) => response,
            Err(error) => error.into_response(),
        },
        Ok(mut value) => {
            add_loki_encoding_flags(&mut value, &flags);
            json_response(StatusCode::OK, &value)
        }
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn index_shards(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let start = Instant::now();
    let response =
        match execute_index_shards_query(&state, &security, &headers, raw_query.as_deref()).await {
            Ok(value) => json_response(StatusCode::OK, &value),
            Err(error) => error.into_response(),
        };
    state.record_query("index_shards", response.status(), start);
    response
}

pub(crate) async fn series(
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
        "series",
        MetadataQuery::Series,
    )
    .await
}
