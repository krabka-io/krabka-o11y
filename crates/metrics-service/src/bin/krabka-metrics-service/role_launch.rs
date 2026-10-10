use super::{AuditHandle, Cli, ClientSecurity, RoleReadiness};

/// What every serving role starts from: its configuration, its metrics and
/// readiness, its WAL client security and its audit handle.
pub(crate) struct RoleLaunch {
    pub(crate) cli: Cli,
    pub(crate) metrics: krabka_promql::metrics::ServiceMetrics,
    pub(crate) readiness: RoleReadiness,
    pub(crate) wal_security: Option<ClientSecurity>,
    pub(crate) audit: AuditHandle,
}
