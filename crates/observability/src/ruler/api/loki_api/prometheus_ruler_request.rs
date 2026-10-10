use super::{
    HeaderMap, IntoResponse, LokiRuleNamespaces, PrometheusRulesFilters, QuerierState,
    RequestSecurity, Response, TenantErrorSurface, TenantId, authorized_ruler_tenant,
    current_unix_time_ns,
};

/// What the Prometheus-compatible rule and alert endpoints read from a
/// request: the tenant, the filters, the evaluation time, and the tenant's
/// rule namespaces, if it has any.
pub(crate) struct PrometheusRulerRequest {
    pub(crate) tenant: TenantId,
    pub(crate) filters: PrometheusRulesFilters,
    pub(crate) evaluation_time: i64,
    pub(crate) namespaces: Option<LokiRuleNamespaces>,
}

/// The parts of a Prometheus-compatible rule or alert request that
/// [`PrometheusRulerRequest`] is resolved from.
pub(crate) struct PrometheusRulerInputs<'a> {
    pub(crate) state: &'a QuerierState,
    pub(crate) security: &'a RequestSecurity,
    pub(crate) headers: &'a HeaderMap,
    pub(crate) raw_query: Option<&'a str>,
}

impl PrometheusRulerInputs<'_> {
    /// Authorizes the request's tenant and parses its filters. The error is
    /// the response to send instead.
    pub(crate) async fn resolve(self) -> Result<PrometheusRulerRequest, Box<Response>> {
        let Self {
            state,
            security,
            headers,
            raw_query,
        } = self;
        let tenant = authorized_ruler_tenant(
            state,
            security,
            headers,
            TenantErrorSurface::PrometheusRuler,
        )
        .await
        .map_err(|error| Box::new(error.into_response()))?;
        let filters = PrometheusRulesFilters::parse(raw_query)
            .map_err(|error| Box::new(error.into_response()))?;
        let evaluation_time = filters.evaluation_time.unwrap_or_else(current_unix_time_ns);
        let namespaces = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned")
            .get(tenant.as_str())
            .cloned();
        Ok(PrometheusRulerRequest {
            tenant,
            filters,
            evaluation_time,
            namespaces,
        })
    }
}
