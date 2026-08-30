use super::{
    BTreeSet, ByteSizeExt, HeaderMap, HttpQueryError, QuerierState, TimeRange, Value,
    authorized_tenant, count_index_stats_entries, json, parse_query, parse_query_params,
    plan_stream_query, planned_block_bytes, validate_loki_volume_query_range_limit,
    validate_query_bytes_limit, validate_query_length_limit, validate_query_range_limit,
    validate_query_series_limit,
};

pub(crate) async fn execute_index_stats_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_query_params(raw_query)?;
    let tenant = authorized_tenant(state, headers).await?;
    let start = params
        .start
        .ok_or(HttpQueryError::MissingQueryParameter("start"))?;
    let end = params
        .end
        .ok_or(HttpQueryError::MissingQueryParameter("end"))?;
    let time_range = TimeRange::new(start, end).map_err(HttpQueryError::from)?;
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
    let entries = count_index_stats_entries(&state, &plan).await?;
    let bytes = planned_block_bytes(&plan).bytes_u64();
    let streams = plan
        .blocks
        .iter()
        .flat_map(|block| block.fingerprints.iter())
        .filter(|fingerprint| plan.fingerprints.contains(fingerprint))
        .copied()
        .collect::<BTreeSet<_>>()
        .len();

    Ok(json!({
        "streams": u64::try_from(streams).unwrap_or(u64::MAX),
        "chunks": u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX),
        "entries": entries,
        "bytes": bytes,
    }))
}
