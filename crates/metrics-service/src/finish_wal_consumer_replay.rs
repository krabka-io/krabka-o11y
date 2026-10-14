use super::{WalHeadConsumerCommit, WalHeadConsumerError, WalHeadReplayResult};

pub(crate) async fn finish_wal_consumer_replay<C, E>(
    consumer: &mut C,
    result: WalHeadReplayResult,
    map_commit_error: impl FnOnce(WalHeadConsumerError) -> E,
    after_commit: impl FnOnce(),
) -> Result<WalHeadReplayResult, E>
where
    C: WalHeadConsumerCommit + ?Sized,
{
    let span = tracing::Span::current();
    span.record("polled", result.polled_records);
    span.record("replayed", result.replayed_records);
    if result.replayed_records > 0 {
        consumer.commit_sync().await.map_err(map_commit_error)?;
        after_commit();
    }
    Ok(result)
}
