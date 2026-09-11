use super::{
    Future, ReadinessGate, Shutdown, Time, WalHead, WalHeadConsumerCommit, WalHeadConsumerPoll,
    run_wal_head_consumer_loop,
};

/// Runs the WAL head consumer, and reports through `wal_head` whether the
/// querier can currently answer for the recent window.
///
/// The querier binds its listener without waiting for the broker, so between
/// bind and connect it can serve a query whose recent half is simply missing.
/// The gate keeps it out of rotation for exactly that window.
pub(crate) fn spawn_wal_head_consumer_task<C, Build, BuildFuture>(
    build_consumer: Build,
    wal_head: WalHead,
    wal_topic: String,
    poll_timeout: Time,
    shutdown: Shutdown,
    wal_head_gate: ReadinessGate,
) -> tokio::task::JoinHandle<()>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + Send + 'static,
    Build: FnOnce() -> BuildFuture + Send + 'static,
    BuildFuture: Future<Output = Result<C, String>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut consumer = match build_consumer().await {
            Ok(consumer) => consumer,
            Err(error) => {
                tracing::error!(%error, "metrics WAL head consumer failed to start; shutting down");
                shutdown.trigger();
                return;
            }
        };
        wal_head_gate.mark_ready();
        let consumer_stop = shutdown.rx.clone();
        let result = run_wal_head_consumer_loop(
            &mut consumer,
            &wal_head,
            &wal_topic,
            poll_timeout,
            move |_| *consumer_stop.borrow(),
        )
        .await;
        // The head stops advancing here, so the querier's recent window starts
        // going stale whether the loop ended on an error or on shutdown.
        wal_head_gate.mark_unready();
        if let Err(error) = result {
            tracing::error!(%error, "metrics WAL head consumer stopped; shutting down");
        }
        shutdown.trigger();
    })
}
