use krabka_blockstore::TenantId;
use serde::Serialize;

use super::{
    Arc, DiscoveryParams, IntoResponse, MetricStore, PrometheusApiState, Rejection, RequestAuth,
    Response, apply_limit, discovery_matchers, discovery_window, enforce_query_range_limit,
    enforce_selected_series_limit, success_data_response,
};
use crate::PromqlMatcher as LabelMatcher;

/// What a discovery request reads: its tenant, its time window, and its
/// `match[]` selectors.
pub(crate) struct DiscoveryScope {
    pub(crate) tenant: TenantId,
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) matcher_sets: Vec<Vec<LabelMatcher>>,
}

/// Resolves and validates a discovery request's scope, or returns the error
/// response that ends the request.
pub(crate) fn discovery_scope<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    discovery_params: &DiscoveryParams,
) -> Result<DiscoveryScope, Rejection> {
    let tenant = auth.tenant()?;
    let window = discovery_window(discovery_params).map_err(Rejection::of)?;
    let matcher_sets = discovery_matchers(discovery_params).map_err(Rejection::of)?;
    enforce_query_range_limit(state, &tenant, window.start_ms, window.end_ms)
        .map_err(Rejection::of)?;
    Ok(DiscoveryScope {
        tenant,
        start_ms: window.start_ms,
        end_ms: window.end_ms,
        matcher_sets,
    })
}

impl DiscoveryScope {
    /// The result caps for this scope's tenant and the request's `limit`.
    pub(crate) fn limits(&self, discovery_params: &DiscoveryParams) -> DiscoveryLimits<'_> {
        DiscoveryLimits {
            tenant: &self.tenant,
            limit: discovery_params.limit,
        }
    }
}

/// The caps on how many results a discovery request returns.
#[derive(Clone, Copy)]
pub(crate) struct DiscoveryLimits<'a> {
    /// The tenant whose selected-series limit applies.
    pub(crate) tenant: &'a TenantId,
    /// The request's own `limit` parameter.
    pub(crate) limit: Option<usize>,
}

/// Rejects more results than the tenant's series limit, then truncates them
/// to the request's `limit`.
pub(crate) fn limit_discovery_results<S: MetricStore, T>(
    state: &Arc<PrometheusApiState<S>>,
    limits: DiscoveryLimits<'_>,
    results: &mut Vec<T>,
) -> Result<(), Rejection> {
    enforce_selected_series_limit(state, limits.tenant, results.len()).map_err(Rejection::of)?;
    apply_limit(results, limits.limit);
    Ok(())
}

/// Answers a discovery request with its found results, or with the rejection
/// that ended it.
pub(crate) fn discovery_response(found: Result<impl Serialize, Rejection>) -> Response {
    match found {
        Ok(found) => success_data_response(found),
        Err(rejection) => rejection.into_response(),
    }
}
