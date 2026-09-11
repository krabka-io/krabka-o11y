use super::{Arc, CancellationToken, JoinHandle, LogIngestLimiter};

/// Spawns the task that keeps the distributor's ingest limiter fresh until
/// `token` is cancelled.
///
/// The role supervises it. A broker-backed limiter refreshes its ACL snapshot
/// here, and a limiter without cached state only waits for the cancel.
pub(crate) fn ingest_limiter_refresh_task(
    limiter: &Arc<dyn LogIngestLimiter>,
    token: &CancellationToken,
) -> (&'static str, JoinHandle<()>) {
    let limiter = Arc::clone(limiter);
    let token = token.clone();
    (
        "distributor ingest limits",
        tokio::spawn(async move { limiter.keep_fresh(token).await }),
    )
}
