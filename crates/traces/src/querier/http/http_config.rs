use super::{Limits, OverridesProvider, TenantId, TenantPolicy};

#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub max_trace_spans: usize,
    pub tag_query_filter_autocomplete_limit: usize,
    /// The one place a tenant's read limits come from.
    ///
    /// Its defaults are what an unlisted tenant gets, so a querier with no
    /// overrides file still has every limit here and needs no second set of
    /// globals beside it.
    pub overrides: OverridesProvider,
    /// The policy that every query route resolves a tenant with. Default:
    /// [`TenantPolicy::anonymous`].
    pub tenant_policy: TenantPolicy,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_trace_spans: usize::MAX,
            tag_query_filter_autocomplete_limit: 25,
            overrides: OverridesProvider::new(Limits::default()),
            tenant_policy: TenantPolicy::anonymous(),
        }
    }
}

impl HttpConfig {
    pub(crate) fn limits_for_tenant(&self, tenant: &TenantId) -> &Limits {
        self.overrides.for_tenant(tenant.as_str())
    }
}
