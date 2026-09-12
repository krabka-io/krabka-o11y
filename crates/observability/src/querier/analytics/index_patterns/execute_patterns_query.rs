use super::{
    BTreeMap, HeaderMap, HttpQueryError, QuerierState, QueryError, RequestSecurity,
    TenantErrorSurface, TimeRange, Value, active_log_delete_filters, authorized_tenant,
    clamp_query_lookback, current_unix_time_ns, is_deleted_log_entry, json, log_line_pattern,
    loki_success_value, parse_patterns_params, parse_query, plan_stream_query,
    read_planned_log_block, sample_time_bucket, validate_query_bytes_limit,
    validate_query_range_limit, validate_query_series_limit, validate_query_string_bytes_limit,
};

pub(crate) async fn execute_patterns_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_patterns_params(raw_query)?;
    if params.step <= 0 {
        return Err(HttpQueryError::InvalidQueryParameter {
            name: "step",
            value: params.step.to_string(),
        });
    }

    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Patterns).await?;
    // One resolution for the whole request: every check below reads the
    // tenant's limits from this state.
    let state = &state.with_tenant_limits(&tenant);
    let tenant = tenant.as_str();
    let time_range = TimeRange::new(params.start, params.end)?;
    let time_range = clamp_query_lookback(&state.limits, time_range, current_unix_time_ns());
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
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;

    let mut patterns = BTreeMap::<String, BTreeMap<i64, u64>>::new();
    for block in &plan.blocks {
        let Some(rows) = read_planned_log_block(&state, &block.key).await? else {
            continue;
        };
        for row in rows {
            if !plan.fingerprints.contains(&row.series_fingerprint)
                || row.timestamp_ns < plan.time_range.start_ns
                || row.timestamp_ns >= plan.time_range.end_ns
            {
                continue;
            }
            let labels = state
                .label_index
                .labels_for(tenant, row.series_fingerprint)
                .ok_or(QueryError::MissingSeriesLabels {
                    tenant: tenant.to_string(),
                    fingerprint: row.series_fingerprint,
                })?;
            if is_deleted_log_entry(
                &delete_filters,
                labels,
                &row.line,
                &row.structured_metadata,
                row.timestamp_ns,
            ) {
                continue;
            }
            if !plan
                .query
                .matches_with_fields(labels, &row.line, &row.structured_metadata)
            {
                continue;
            }
            let bucket = sample_time_bucket(row.timestamp_ns, params.start, params.step);
            *patterns
                .entry(log_line_pattern(&row.line))
                .or_default()
                .entry(bucket)
                .or_default() += 1;
        }
    }

    let data = patterns
        .into_iter()
        .map(|(pattern, samples)| {
            json!({
                "pattern": pattern,
                "samples": samples
                    .into_iter()
                    .map(|(timestamp_ns, count)| json!([timestamp_ns / 1_000_000_000, count]))
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    Ok(loki_success_value(data))
}
