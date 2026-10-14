use krabka_blockstore::TenantId;

use super::{
    CardinalityParams, Labels, MetricStore, PrometheusApiState, Rejection, RequestAuth,
    cardinality_series,
};

pub(crate) struct AuthorizedCardinalitySeries {
    pub(crate) tenant: TenantId,
    pub(crate) series: Vec<Labels>,
}

/// Resolves the request's tenant and the series its cardinality parameters
/// select, or the error response that ends the request.
pub(crate) async fn authorized_cardinality_series<S: MetricStore>(
    state: &PrometheusApiState<S>,
    auth: RequestAuth<'_>,
    cardinality_params: &CardinalityParams,
) -> Result<AuthorizedCardinalitySeries, Rejection> {
    let tenant = auth.tenant()?;
    let series = cardinality_series(state, tenant.as_str(), cardinality_params)
        .await
        .map_err(Rejection::of)?;
    Ok(AuthorizedCardinalitySeries { tenant, series })
}
