use krabka_observability::RoleReadiness;

use super::{CancellationToken, Cli, ProcessSecurity, ReadRole, ServiceMetrics, run_read_role};

/// Answers a query from the WAL tail this role keeps and the blocks its index
/// names.
///
/// `security` sets the TLS and authentication of `--listen`, and the TLS and
/// SASL of the WAL tail.
///
/// # Errors
/// Returns an error when the object store or the block index cannot be
/// reached, when `--listen` cannot be bound, or when a supervised task ends
/// before the role was asked to stop.
pub(crate) async fn run_querier(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    security: ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error>> {
    run_read_role(
        ReadRole::Querier,
        cli,
        metrics,
        readiness,
        shutdown,
        security,
    )
    .await
}
