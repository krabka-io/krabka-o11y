use super::{MetricStore, PrometheusApiState, ApiError, QueryEnforcer};

pub(crate) fn enforce_selected_series_limit<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    selected: usize,
) -> Result<(), ApiError> {
    let Some(limits) = &state.query_limits else {
        return Ok(());
    };
    QueryEnforcer::check_series_count(
        limits.for_tenant(tenant),
        u64::try_from(selected).unwrap_or(u64::MAX),
    )
    .map_err(ApiError::from)
}
