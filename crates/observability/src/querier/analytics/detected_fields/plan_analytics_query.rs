use super::{
    HttpQueryError, QuerierState, StreamPlan, TimeRange, clamp_query_lookback,
    current_unix_time_ns, parse_query, plan_stream_query, validate_loki_volume_query_range_limit,
    validate_query_bytes_limit, validate_query_range_limit, validate_query_series_limit,
    validate_query_string_bytes_limit,
};
use crate::TenantId;

/// Whether an analytics read is also held to the volume endpoints' range
/// limit.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum VolumeRangeLimit {
    Check,
    Skip,
}

/// An index analytics read of `query` over `time_range` for `tenant`.
pub(crate) struct AnalyticsQuery<'a> {
    pub(crate) tenant: &'a TenantId,
    pub(crate) time_range: TimeRange,
    pub(crate) query: &'a str,
    pub(crate) volume_range_limit: VolumeRangeLimit,
}

/// Plans an index analytics read, and checks it against the tenant's limits.
///
/// Returns the request's state, the plan, and the clamped time range.
pub(crate) async fn plan_analytics_query(
    state: &QuerierState,
    request: AnalyticsQuery<'_>,
) -> Result<(QuerierState, StreamPlan, TimeRange), HttpQueryError> {
    let AnalyticsQuery {
        tenant,
        time_range,
        query,
        volume_range_limit,
    } = request;
    // One resolution for the whole request: every check below reads the
    // tenant's limits from this state.
    let state = &state.with_tenant_limits(tenant);
    let tenant = tenant.as_str();
    let time_range = clamp_query_lookback(&state.limits, time_range, current_unix_time_ns());
    if volume_range_limit == VolumeRangeLimit::Check {
        validate_loki_volume_query_range_limit(state, time_range)?;
    }
    validate_query_range_limit(state, time_range)?;
    validate_query_string_bytes_limit(state, query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let parsed = parse_query(query).map_err(|source| HttpQueryError::LokiParse {
        query: query.to_string(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        parsed,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    Ok((state, plan, time_range))
}
