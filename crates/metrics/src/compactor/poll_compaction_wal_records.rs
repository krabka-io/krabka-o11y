use super::{
    CompactionConsumerPoll, CompactionPollError, CompactionWalRecord, ServiceMetrics, Time,
    compaction_wal_records_from_consumer_records,
};

pub(crate) struct PolledCompactionWalRecords {
    pub(crate) polled_records: usize,
    pub(crate) wal_records: Vec<CompactionWalRecord>,
}

pub(crate) async fn poll_compaction_wal_records<P: CompactionConsumerPoll + ?Sized>(
    poller: &mut P,
    wal_topic: &str,
    timeout: Time,
    metrics: &ServiceMetrics,
) -> Result<PolledCompactionWalRecords, CompactionPollError> {
    let records = poller
        .poll(timeout)
        .await
        .inspect_err(|_| metrics.wal_consumer.record_poll_failure())?;
    metrics.wal_consumer.record_poll(&records);
    let polled_records = records.len();
    let wal_records = compaction_wal_records_from_consumer_records(wal_topic, &records)?;
    Ok(PolledCompactionWalRecords {
        polled_records,
        wal_records,
    })
}
