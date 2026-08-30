use super::{
    CompactionFrontier, CompactionFrontierSource, HeaderMap, HttpQueryError,
    LOKI_DEFAULT_TAIL_LIMIT, QuerierState, QueryParams, TailStream, active_log_delete_filters,
    authorized_tenant, optional_start_end_range, parse_query, plan_stream_query,
    validate_loki_tail_delay_for, validate_query_length_limit,
};

pub(crate) async fn prepare_http_tail(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &QueryParams,
) -> Result<TailStream, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let time_range = optional_start_end_range(params.start, params.since, params.end)?;
    let delay_for = params.delay_for.unwrap_or(0);
    validate_loki_tail_delay_for(delay_for)?;
    validate_query_length_limit(state, &params.query)?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let delete_filters = active_log_delete_filters(state, tenant, time_range)?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    let (source, frontier) = state.hot_tail.as_ref().map_or(
        (
            None,
            CompactionFrontierSource::Snapshot(CompactionFrontier::new(i64::MAX)),
        ),
        |hot_tail| (Some(hot_tail.source.clone()), hot_tail.frontier.clone()),
    );

    Ok(TailStream {
        plan,
        source,
        frontier,
        delete_filters,
        limit: Some(params.limit.unwrap_or(LOKI_DEFAULT_TAIL_LIMIT)),
        delay_for,
    })
}
