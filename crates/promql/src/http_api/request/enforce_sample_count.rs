use super::*;

pub(crate) fn enforce_sample_count<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    processed: u64,
) -> Result<(), ApiError> {
    let Some(limits) = &state.query_limits else {
        return Ok(());
    };
    QueryEnforcer::check_sample_count(limits.for_tenant(tenant), processed).map_err(ApiError::from)
}
