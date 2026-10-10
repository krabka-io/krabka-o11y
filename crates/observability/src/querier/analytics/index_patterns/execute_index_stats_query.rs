use super::{
    AnalyticsQuery, BTreeSet, ByteSizeExt, HeaderMap, HttpQueryError, QuerierState,
    RequestSecurity, TenantErrorSurface, TimeRange, Value, VolumeRangeLimit, authorized_tenant,
    count_index_stats_entries, json, parse_query_params, plan_analytics_query, planned_block_bytes,
};

pub(crate) async fn execute_index_stats_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_query_params(raw_query)?;
    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Read).await?;
    let start = params
        .start
        .ok_or(HttpQueryError::MissingQueryParameter("start"))?;
    let end = params
        .end
        .ok_or(HttpQueryError::MissingQueryParameter("end"))?;
    let time_range = TimeRange::new(start, end)?;
    let (state, plan, _) = plan_analytics_query(
        state,
        AnalyticsQuery {
            tenant: &tenant,
            time_range,
            query: &params.query,
            volume_range_limit: VolumeRangeLimit::Check,
        },
    )
    .await?;
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
