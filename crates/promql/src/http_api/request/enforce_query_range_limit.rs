use super::{ApiError, MetricStore, PrometheusApiState, QueryEnforcer, TenantId, unix_now_ms};

pub(crate) fn enforce_query_range_limit<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    start_ms: i64,
    end_ms: i64,
) -> Result<(), ApiError> {
    let now_ms = unix_now_ms()?;
    QueryEnforcer::check_range(
        state.query_limits.for_tenant(tenant.as_str()),
        start_ms,
        end_ms,
        now_ms,
    )
    .map_err(ApiError::from)
}
