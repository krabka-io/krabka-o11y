use super::{HaElectionConsumerPoll, HaElectionConsumerCommit, HaElectionConsumerLoopSummary, HaTracker, Time, HaElectionConsumerError, poll_ha_election_consumer_once};

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn run_ha_election_consumer_loop<C, Stop>(
    consumer: &mut C,
    tracker: &HaTracker,
    ha_topic: &str,
    timeout: Time,
    mut should_stop: Stop,
) -> Result<HaElectionConsumerLoopSummary, HaElectionConsumerError>
where
    C: HaElectionConsumerPoll + HaElectionConsumerCommit + ?Sized,
    Stop: FnMut(&HaElectionConsumerLoopSummary) -> bool,
{
    let mut summary = HaElectionConsumerLoopSummary::default();
    loop {
        let result = poll_ha_election_consumer_once(consumer, tracker, ha_topic, timeout).await?;
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
