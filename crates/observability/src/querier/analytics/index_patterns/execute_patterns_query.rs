use super::{
    BTreeMap, HeaderMap, HttpQueryError, QuerierState, RangeEnd, RequestSecurity,
    TenantErrorSurface, Value, VolumeRangeLimit, json, log_line_pattern, loki_success_value,
    parse_patterns_params, read_planned_log_block, reads_planned_row, sample_time_bucket,
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

    let planned = crate::querier::analytics::authorize_and_prepare_analytics_read(
        state,
        security,
        headers,
        TenantErrorSurface::Patterns,
        params.start,
        params.end,
        &params.query,
        VolumeRangeLimit::Skip,
    )
    .await?;
    let read = planned.read(RangeEnd::Exclusive);

    let mut patterns = BTreeMap::<String, BTreeMap<i64, u64>>::new();
    for block in &planned.plan.blocks {
        let Some(rows) = read_planned_log_block(&planned.state, &block.key).await? else {
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
        .map(|entry| {
            json!({
                "pattern": entry.0,
                "samples": entry.1
                    .into_iter()
                    .map(|(timestamp_ns, count)| json!([timestamp_ns / 1_000_000_000, count]))
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    Ok(loki_success_value(data))
}
