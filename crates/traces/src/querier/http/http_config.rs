use super::*;

#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub max_trace_spans: usize,
    pub tag_query_filter_autocomplete_limit: usize,
    pub limits: Limits,
    pub overrides: Option<OverridesProvider>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_trace_spans: usize::MAX,
            tag_query_filter_autocomplete_limit: 25,
            limits: Limits::default(),
            overrides: None,
        }
    }
}

impl HttpConfig {
    pub(crate) fn limits_for_tenant(&self, tenant: &str) -> &Limits {
        self.overrides
            .as_ref()
            .map_or(&self.limits, |overrides| overrides.for_tenant(tenant))
    }
}
