use krabka_observability::RoleReadiness;

use super::{CancellationToken, Cli, ProcessSecurity, ServiceMetrics};

/// A role that serves on a listener its caller has already bound.
///
/// The caller binds `listener` because under `--target all` the role takes an
/// ephemeral loopback port, and another role in the same process has to be
/// told which one it got.
pub(crate) struct ListeningRole<'a> {
    pub(crate) cli: Cli,
    pub(crate) metrics: ServiceMetrics,
    pub(crate) readiness: RoleReadiness,
    pub(crate) shutdown: CancellationToken,
    pub(crate) listener: tokio::net::TcpListener,
    pub(crate) security: &'a ProcessSecurity,
}
