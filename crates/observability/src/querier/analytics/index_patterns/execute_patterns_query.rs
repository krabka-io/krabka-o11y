use super::{
    AnalyticsQuery, BTreeMap, HeaderMap, HttpQueryError, PlannedRead, QuerierState, RangeEnd,
    RequestSecurity, TenantErrorSurface, TimeRange, Value, VolumeRangeLimit,
    active_log_delete_filters, authorized_tenant, json, log_line_pattern, loki_success_value,
    parse_patterns_params, plan_analytics_query, read_planned_log_block, reads_planned_row,
    sample_time_bucket,
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
    let time_range = TimeRange::new(params.start, params.end)?;
    let (state, plan, time_range) = plan_analytics_query(
        state,
        AnalyticsQuery {
            tenant: &tenant,
            time_range,
            query: &params.query,
            volume_range_limit: VolumeRangeLimit::Skip,
        },
    )
    .await?;
    let tenant = tenant.as_str();
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;
    let read = PlannedRead {
        state: &state,
        plan: &plan,
        delete_filters: &delete_filters,
        end: RangeEnd::Exclusive,
    };

    let mut patterns = BTreeMap::<String, BTreeMap<i64, u64>>::new();
    for block in &plan.blocks {
        let Some(rows) = read_planned_log_block(&state, &block.key).await? else {
            continue;
        };
        for row in rows {
            if !reads_planned_row(&read, &row)? {
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
