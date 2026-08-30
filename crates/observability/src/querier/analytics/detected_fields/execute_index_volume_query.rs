use super::{
    HeaderMap, HttpQueryError, QuerierState, TimeRange, Value, VolumeKind,
    add_loki_query_stats_for_stream_plan, authorized_tenant, index_volume_samples,
    loki_volume_vector_response, parse_query, parse_volume_params, plan_stream_query,
    validate_loki_volume_query_range_limit, validate_query_bytes_limit,
    validate_query_length_limit, validate_query_range_limit, validate_query_series_limit,
};

pub(crate) async fn execute_index_volume_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    kind: VolumeKind,
) -> Result<Value, HttpQueryError> {
    let params = parse_volume_params(raw_query)?;
    let tenant = authorized_tenant(state, headers).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_loki_volume_query_range_limit(time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
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
    let response = match kind {
        VolumeKind::Instant => loki_volume_vector_response(volumes, params.end, params.limit),
        VolumeKind::Range => {
            if params.step.is_some_and(|step| step <= 0) {
                return Err(HttpQueryError::InvalidStep);
            }
            loki_volume_vector_response(volumes, params.end, params.limit)
        }
    };
    Ok(add_loki_query_stats_for_stream_plan(response, &plan))
}
