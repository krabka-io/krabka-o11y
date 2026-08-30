use super::*;

pub(crate) async fn poll_accumulated_log_compaction_records(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    initial_timeout: Time,
    accumulation_window: Time,
    accumulation_poll_timeout: Time,
    max_records_per_batch: NonZeroUsize,
) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
    let mut records = consumer.poll(initial_timeout).await?;
    if records.is_empty() || records.len() >= max_records_per_batch.get() {
        return Ok(records);
    }

    let deadline = Instant::now() + accumulation_window.to_std();
    while records.len() < max_records_per_batch.get() {
        let remaining = deadline.saturating_duration_since(Instant::now()).as_time();
        if remaining <= <Time as TimeExt>::ZERO {
            break;
        }

        // `Time` is `PartialOrd` but not `Ord`, so `Time::min` rather than
        // `std::cmp::min`.
        let poll_timeout = remaining.min(accumulation_poll_timeout);
        let next = consumer.poll(poll_timeout).await?;
        if next.is_empty() {
            break;
        }
        records.extend(next);
    }

    Ok(records)
}
