use super::{ApiError, MetricStore, PrometheusApiState, QueryEnforcer, TenantId};

pub(crate) fn enforce_selected_series_limit<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    selected: usize,
) -> Result<(), ApiError> {
    QueryEnforcer::check_series_count(
        state.query_limits.for_tenant(tenant.as_str()),
        u64::try_from(selected).unwrap_or(u64::MAX),
    )
    .map_err(ApiError::from)
}
