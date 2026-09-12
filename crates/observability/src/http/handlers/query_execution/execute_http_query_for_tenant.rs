use super::{
    HttpQueryError, LokiStreamEncoding, QuerierState, QueryKind, QueryParams, TenantId, Value,
    clamp_query_lookback, current_unix_time_ns, execute_http_logql_expr, loki_direction,
    parse_logql_expr, populate_loki_query_execution_stats, reject_signed_vector_function_literal,
    time_range, validate_loki_query_range_resolution, validate_loki_range_query_range_limit,
    validate_query_entries_limit, validate_query_range_limit, validate_query_string_bytes_limit,
};
use crate::execute_logs_query_frontend;

pub(crate) async fn execute_http_query_for_tenant(
    state: &QuerierState,
    tenant: &TenantId,
    params: &QueryParams,
    kind: QueryKind,
    encoding: LokiStreamEncoding,
) -> Result<Value, HttpQueryError> {
    let started = std::time::Instant::now();
    let mut result = if matches!(kind, QueryKind::Range) {
        execute_logs_query_frontend(state, tenant, params, encoding).await
    } else {
        execute_http_query_for_tenant_inner(state, tenant, params, kind, encoding).await
    };
    if let Ok(response) = &mut result {
        let queue_time = response
            .pointer("/data/stats/summary/queueTime")
            .and_then(Value::as_f64)
            .map(std::time::Duration::from_secs_f64)
            .unwrap_or_default();
        populate_loki_query_execution_stats(response, started.elapsed(), queue_time);
    }
    result
}

pub(crate) async fn execute_http_query_for_tenant_inner(
    state: &QuerierState,
    tenant: &TenantId,
    params: &QueryParams,
    kind: QueryKind,
    encoding: LokiStreamEncoding,
) -> Result<Value, HttpQueryError> {
    // One resolution for the whole query: every check below, and every
    // validator the helpers call, reads the tenant's limits from this state.
    let state = &state.with_tenant_limits(tenant);
    let tenant = tenant.as_str();
    let time_range = time_range(params, kind)?;
    // Clamped before the window caps, as `Loki`'s limits middleware does.
    let time_range = clamp_query_lookback(&state.limits, time_range, current_unix_time_ns());
    validate_loki_range_query_range_limit(state, kind, time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_string_bytes_limit(state, &params.query)?;
    validate_query_entries_limit(state, params.limit)?;
    validate_loki_query_range_resolution(params, kind, time_range)?;
    let limit = params.limit;
    let direction = loki_direction(params.direction.as_deref())?;
    let interval = params.interval;
    reject_signed_vector_function_literal(&params.query)?;
    let expression =
        parse_logql_expr(&params.query).map_err(|source| HttpQueryError::LokiParse {
            query: params.query.clone(),
            source,
        })?;
    execute_http_logql_expr(
        state,
        tenant,
        time_range,
        params.step,
        kind,
        &expression,
        (direction, limit, interval),
        encoding,
        &params.query,
    )
    .await
}
