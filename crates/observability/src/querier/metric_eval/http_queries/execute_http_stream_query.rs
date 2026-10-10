use super::{
    Arc, HttpQueryError, LokiDirection, LokiStreamEncoding, QuerierState, QueryHotTail,
    StreamScanOptions, TimeRange, Value, active_log_delete_filters,
    add_loki_query_stats_for_stream_blocks_with_hot_tail, add_loki_query_stats_for_stream_plan,
    add_loki_query_stats_for_stream_plan_with_hot_tail, apply_loki_stream_options,
    execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options,
    execute_stream_query_with_deletes, execute_stream_query_with_hot_tail_frontier_and_deletes,
    hot_tail_snapshot, parse_query, plan_stream_query, validate_loki_interval,
    validate_query_bytes_limit, validate_query_series_limit,
};

/// How a log query orders, caps and steps the entries it returns.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LokiStreamOptions {
    pub(crate) direction: LokiDirection,
    pub(crate) limit: Option<usize>,
    pub(crate) interval: Option<i64>,
}

/// One log query, as the HTTP query API runs it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HttpStreamQuery<'a> {
    pub(crate) query: &'a str,
    pub(crate) tenant: &'a str,
    pub(crate) time_range: TimeRange,
    pub(crate) options: LokiStreamOptions,
    /// The end a range query keeps entries strictly before, or `None` for a
    /// query that keeps the whole of `time_range`.
    pub(crate) end_exclusive: Option<i64>,
    pub(crate) encoding: LokiStreamEncoding,
}

pub(crate) async fn execute_http_stream_query(
    state: &QuerierState,
    stream_query: HttpStreamQuery<'_>,
) -> Result<Value, HttpQueryError> {
    let HttpStreamQuery {
        query,
        tenant,
        time_range,
        options,
        end_exclusive,
        encoding,
    } = stream_query;
    validate_loki_interval(options.interval)?;
    let query = parse_query(query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;
    if let Some(cold_store) = &state.cold_store {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let scan = execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
            Arc::clone(&cold_store.store),
            &cold_store.prefix,
            &plan,
            &state.label_index,
            QueryHotTail {
                records: &records,
                frontier: &frontier,
                delete_filters: &delete_filters,
            },
            StreamScanOptions::from_stream_options(options, end_exclusive)
                .with_block_fetch_concurrency(state.cold_block_fetch_concurrency)
                .with_encoding(encoding),
        )
        .await
        .map_err(HttpQueryError::from)?;
        let response = apply_loki_stream_options(scan.value, options, end_exclusive);
        return Ok(add_loki_query_stats_for_stream_blocks_with_hot_tail(
            response,
            &scan.scanned_blocks,
            &plan,
            &records,
            &frontier,
        ));
    }
    if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_shared_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        let response = execute_stream_query_with_hot_tail_frontier_and_deletes(
            &state.root,
            &plan,
            &state.label_index,
            &records,
            &frontier,
            &delete_filters,
            encoding,
        )
        .await
        .map_err(HttpQueryError::from)?;
        let response = apply_loki_stream_options(response, options, end_exclusive);
        return Ok(add_loki_query_stats_for_stream_plan_with_hot_tail(
            response, &plan, &records, &frontier,
        ));
    }
    let response = execute_stream_query_with_deletes(
        &state.root,
        &plan,
        &state.label_index,
        &delete_filters,
        encoding,
    )
    .await
    .map_err(HttpQueryError::from)?;
    let response = apply_loki_stream_options(response, options, end_exclusive);
    Ok(add_loki_query_stats_for_stream_plan(response, &plan))
}
