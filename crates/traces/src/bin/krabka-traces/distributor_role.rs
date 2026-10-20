use krabka_observability::RoleReadiness;

use super::{CancellationToken, Cli, ProcessSecurity, ServiceMetrics};

/// Whether the distributor also serves its push router on `--listen`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PrimaryListen {
    /// A standalone distributor owns `--listen`.
    Serve,
    /// Under `--target all`, `--listen` is the query-frontend's Tempo API
    /// port, so the distributor leaves it alone.
    Skip,
}

/// One distributor role to run: the process configuration and the shared
/// state it reports through, whether it serves `--listen`, and the security
/// its listeners and WAL producer use.
pub(crate) struct DistributorRole<'a> {
    pub(crate) cli: Cli,
    pub(crate) metrics: ServiceMetrics,
    pub(crate) readiness: RoleReadiness,
    pub(crate) shutdown: CancellationToken,
    pub(crate) primary_listen: PrimaryListen,
    pub(crate) security: &'a ProcessSecurity,
}
