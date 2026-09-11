use super::{ApiError, MetricStore, PrometheusApiState, QueryEnforcer, TenantId};

pub(crate) fn enforce_sample_count<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &TenantId,
    processed: u64,
) -> Result<(), ApiError> {
    QueryEnforcer::check_sample_count(state.query_limits.for_tenant(tenant.as_str()), processed)
        .map_err(ApiError::from)
}
