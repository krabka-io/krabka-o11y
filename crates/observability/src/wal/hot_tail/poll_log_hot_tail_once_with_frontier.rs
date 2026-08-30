use super::*;

pub(crate) async fn poll_log_hot_tail_once_with_frontier(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    hot_tail: &BufferedLogHotTail,
    timeout: Time,
    frontier: Option<&SharedCompactionFrontier>,
) -> Result<usize, HotTailPollError> {
    let batch = consumer.poll(timeout).await?;
    let records = batch
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;
    let decoded = records.len();
    hot_tail.append_records(records);
    if let Some(frontier) = frontier {
        let _ = hot_tail.prune_compacted(&frontier.snapshot());
    }
    Ok(decoded)
}
