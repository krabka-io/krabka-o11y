use super::{
    HeaderMap, HttpQueryError, QuerierState, RequestSecurity, TenantErrorSurface, TimeRange, Value,
    VolumeKind, add_loki_query_stats_for_stream_plan, authorized_tenant, clamp_query_lookback,
    current_unix_time_ns, index_volume_samples, json, loki_volume_matrix_response,
    loki_volume_vector_response, parse_query, parse_volume_params, plan_stream_query,
    validate_loki_volume_query_range_limit, validate_query_bytes_limit, validate_query_range_limit,
    validate_query_series_limit, validate_query_string_bytes_limit,
};

pub(crate) async fn execute_index_volume_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    kind: VolumeKind,
) -> Result<Value, HttpQueryError> {
    let params = parse_volume_params(raw_query)?;
    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Read).await?;
    // One resolution for the whole request: every check below reads the
    // tenant's limits from this state.
    let state = &state.with_tenant_limits(&tenant);
    let tenant = tenant.as_str();
    let time_range = TimeRange::new(params.start, params.end)?;
    let time_range = clamp_query_lookback(&state.limits, time_range, current_unix_time_ns());
    validate_loki_volume_query_range_limit(state, time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_string_bytes_limit(state, &params.query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let volumes = index_volume_samples(&state, tenant, &plan, &params);
    let mut response = match kind {
        VolumeKind::Instant => loki_volume_vector_response(volumes, params.end, params.limit),
        VolumeKind::Range => {
            if params.step.is_some_and(|step| step <= 0) {
                return Err(HttpQueryError::InvalidStep);
            }
            // Loki writes an empty `volume_range` answer as a `vector`, and
            // one with series in it as a `matrix`.
            if volumes.is_empty() {
                loki_volume_vector_response(volumes, params.end, params.limit)
            } else {
                loki_volume_matrix_response(volumes, params.limit)
            }
        }
    };
    // A volume read scans no lines, and Loki's stats for one say so. The stats
    // are built from an empty answer, so a `matrix`'s samples are not counted
    // as lines processed.
    response["data"]["stats"] =
        add_loki_query_stats_for_stream_plan(json!({}), &plan)["data"]["stats"].take();
    Ok(response)
}
