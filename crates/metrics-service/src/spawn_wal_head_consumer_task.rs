use super::{
    Future, Shutdown, Time, WalHead, WalHeadConsumerCommit, WalHeadConsumerPoll,
    run_wal_head_consumer_loop,
};

pub(crate) fn spawn_wal_head_consumer_task<C, Build, BuildFuture>(
    build_consumer: Build,
    wal_head: WalHead,
    wal_topic: String,
    poll_timeout: Time,
    shutdown: Shutdown,
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
        let consumer_stop = shutdown.rx.clone();
        let result = run_wal_head_consumer_loop(
            &mut consumer,
            &wal_head,
            &wal_topic,
            poll_timeout,
            move |_| *consumer_stop.borrow(),
        )
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "metrics WAL head consumer stopped; shutting down");
        }
        shutdown.trigger();
    })
}
