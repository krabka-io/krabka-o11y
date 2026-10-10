use krabka_observability::RoleReadiness;

use super::{
    CancellationToken, Cli, ProcessSecurity, ReadRole, ReadRoleInputs, ServiceMetrics,
    run_read_role,
};

/// Answers a query by splitting its range into `--query-frontend-shard-width`
/// shards, executing them through the shared bounded fan-out and result-cache
/// pipeline, and merging what each returns.
///
/// `security` sets the TLS and authentication of `--listen`, and the TLS and
/// SASL of the WAL tail.
///
/// # Errors
/// Returns an error when the object store or the block index cannot be
/// reached, when `--listen` cannot be bound, or when a supervised task ends
/// before the role was asked to stop.
pub(crate) async fn run_query_frontend(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    security: ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error>> {
    run_read_role(
        ReadRole::QueryFrontend,
        ReadRoleInputs {
            cli,
            metrics,
            readiness,
            shutdown,
            security,
        },
    )
    .await
}
