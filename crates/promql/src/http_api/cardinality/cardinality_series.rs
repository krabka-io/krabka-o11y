use super::*;

/// Resolves the series set a cardinality request operates on.
///
/// The result is the selector match when the request gives a `selector`. If the
/// request gives no `selector`, the result is every active series for the tenant.
pub(crate) async fn cardinality_series<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    params: &CardinalityParams,
) -> Result<Vec<Labels>, ApiError> {
    if params.selector.is_some() {
        cardinality_series_for_params(state, tenant, params).await
    } else {
        state
            .store
            .cardinality_active_series(tenant)
            .await
            .map_err(ApiError::from)
    }
}
