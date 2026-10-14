use std::{fmt::Debug, future::Future};

use krabka_observability::{CancellationToken, compaction_metrics::CompactionMetrics};

/// Runs a compactor role with shutdown already requested and checks it exits
/// cleanly without completing a pass.
pub(super) async fn assert_starts_stopped<RoleError, RoleFuture>(
    compaction: &CompactionMetrics,
    start_role: impl FnOnce(CancellationToken) -> RoleFuture,
) where
    RoleError: Debug,
    RoleFuture: Future<Output = Result<(), RoleError>>,
{
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let outcome = start_role(shutdown).await;
    super::compaction_passes::assert_stopped_without_a_pass(&outcome, compaction);
}
