use super::{
    Time, WalHead, WalHeadConsumerCommit, WalHeadConsumerError, WalHeadConsumerLoopSummary,
    WalHeadConsumerPoll, poll_wal_head_consumer_once,
};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_wal_head_consumer_loop<C, Stop>(
    consumer: &mut C,
    head: &WalHead,
    wal_topic: &str,
    timeout: Time,
    mut should_stop: Stop,
) -> Result<WalHeadConsumerLoopSummary, WalHeadConsumerError>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
    Stop: FnMut(&WalHeadConsumerLoopSummary) -> bool,
{
    let mut summary = WalHeadConsumerLoopSummary::default();
    loop {
        let result = poll_wal_head_consumer_once(consumer, head, wal_topic, timeout).await?;
        summary.polls += 1;
        summary.polled_records += result.polled_records;
        summary.replayed_records += result.replayed_records;
        summary.committed_offsets.extend(result.committed_offsets);

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}
