use super::{
    ApiError, BTreeMap, MetricStore, PrometheusApiState, Rejection, RequestAuth, TenantId,
};

/// A tenant's configured rule groups, keyed by namespace and then group name.
pub(crate) type TenantRuleGroups = BTreeMap<String, BTreeMap<String, serde_yaml::Value>>;

/// The request's tenant and a copy of its configured rule groups. An `Err` is
/// the error response that ends the request.
pub(crate) fn tenant_ruler_rules<S: MetricStore>(
    state: &PrometheusApiState<S>,
    auth: RequestAuth<'_>,
) -> Result<(TenantId, TenantRuleGroups), Rejection> {
    let tenant = auth.tenant()?;
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules.get(&tenant).cloned().unwrap_or_default(),
        Err(_) => {
            return Err(Rejection::of(ApiError::internal(
                "ruler rules lock poisoned",
            )));
        }
    };
    Ok((tenant, rules))
}
