use super::TenantId;

/// What a service does with a request that names no tenant.
///
/// Each Grafana component makes this choice through its multi-tenancy switch.
/// Mimir and Loki default the switch on, and reject a request without an
/// `X-Scope-OrgID` header. Tempo and Pyroscope default it off, and put every
/// such request under one fixed tenant. A Krabka service picks the variant its
/// upstream uses, and [`TenantId::resolve`] applies it the same way at every
/// header and metadata boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantPolicy {
    /// A request without a tenant is rejected.
    Required,
    /// A request without a tenant is served as this tenant.
    Fallback(TenantId),
}

impl TenantPolicy {
    /// The fallback policy that uses [`super::ANONYMOUS_TENANT`].
    #[must_use]
    pub fn anonymous() -> Self {
        Self::Fallback(TenantId::anonymous())
    }
}
