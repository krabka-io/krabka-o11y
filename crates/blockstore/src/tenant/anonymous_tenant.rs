/// The tenant a single-tenant deployment uses when a request names none.
///
/// Grafana Tempo and Grafana Pyroscope both use this name when multi-tenancy
/// is off, so a Krabka service that accepts a request without an
/// `X-Scope-OrgID` header reads as those components do. It is a valid
/// [`super::TenantId`], and each signal decides for itself whether a request
/// without a tenant gets this name or a rejection.
pub const ANONYMOUS_TENANT: &str = "anonymous";
