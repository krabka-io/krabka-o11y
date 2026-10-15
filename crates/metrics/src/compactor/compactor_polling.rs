use super::*;
use krabka_observability::{KafkaWalHeader, set_remote_parent_from_kafka_headers};
use krabka_observability::persisted_format::{PersistedHeader, validate_persisted_format};

/// Converts polled consumer records from the metrics WAL topic into compactor
/// inputs.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn compaction_wal_records_from_consumer_records(
    wal_topic: &str,
    records: &[ConsumerRecord],
) -> Result<Vec<CompactionWalRecord>, CompactionConsumerRecordError> {
    let mut out = Vec::new();
    for record in records {
        if record.topic != wal_topic {
            continue;
        }
        validate_persisted_format(record.headers.iter().map(|header| PersistedHeader {
            key: header.key.as_str(),
            value: header.value.as_deref(),
        }))
        .map_err(|error| CompactionConsumerRecordError::UnsupportedFormat {
            partition: PartitionIndex(record.partition),
            offset: Offset(record.offset),
            message: error.to_string(),
        })?;
        let value = record
            .value
            .as_ref()
            .ok_or(CompactionConsumerRecordError::MissingValue {
                partition: PartitionIndex(record.partition),
                offset: Offset(record.offset),
            })?;
        out.push(CompactionWalRecord {
            partition: PartitionIndex(record.partition),
            offset: Offset(record.offset),
            value: value.to_vec(),
        });
    }
    Ok(out)
}

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

/// Polls, compacts, and commits once with a single mutable consumer handle.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn poll_compactor_consumer_once<C, S>(
    consumer: &mut C,
    block_writer: &BlockWriter,
    index_sink: &S,
    wal_topic: &str,
    timeout: Time,
    metrics: &ServiceMetrics,
) -> Result<CompactionPollResult, CompactionPollError>
where
    C: CompactionConsumerPoll + CompactionConsumerCommitMut + ?Sized,
    S: CompactionIndexSink + ?Sized,
{
    let polled = poll_compaction_wal_records(consumer, wal_topic, timeout, metrics).await?;
    let compacted_records = polled.wal_records.len();
    let batch = process_compaction_record_batch_with_consumer(
        block_writer,
        index_sink,
        consumer,
        &polled.wal_records,
    )
    .await?;

    Ok(CompactionPollResult {
        polled_records: polled.polled_records,
        compacted_records,
        batch,
    })
}

/// Polls the metrics WAL consumer once, compacts the returned records, and
/// commits on success.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn poll_compactor_once<P, S, C>(
    poller: &mut P,
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    wal_topic: &str,
    timeout: Time,
    metrics: &ServiceMetrics,
) -> Result<CompactionPollResult, CompactionPollError>
where
    P: CompactionConsumerPoll + ?Sized,
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    let poll_result = poll_compaction_wal_records(poller, wal_topic, timeout, metrics).await?;
    let compacted_records = poll_result.wal_records.len();
    let batch = process_compaction_record_batch(
        block_writer,
        index_sink,
        committer,
        &poll_result.wal_records,
    )
    .await?;

    Ok(CompactionPollResult {
        polled_records: poll_result.polled_records,
        compacted_records,
        batch,
    })
}


/// Builds the per-poll-batch `metrics_compaction` consumer span and joins it to
/// the producer trace on a WAL record's `traceparent` header, if there is one.
///
/// There is ONE span per poll batch, and not one per record. `set_remote_parent`
/// does nothing when no polled record carries a valid trace context, so this
/// span is always safe to build. The first record with a `traceparent` header
/// anchors the parent.
pub(crate) fn compaction_batch_span(
    records: &[ConsumerRecord],
    wal_records: usize,
) -> tracing::Span {
    let span = tracing::info_span!(
        "metrics_compaction",
        otel.kind = "consumer",
        krabka.wal.records = wal_records,
    );
    if let Some(record) = records.iter().find(|record| {
        record
            .headers
            .iter()
            .any(|header| header.key == TRACEPARENT)
    }) {
        let headers = record
            .headers
            .iter()
            .map(|header| KafkaWalHeader {
                key: header.key.clone(),
                value: header.value.as_ref().map(|value| value.to_vec()),
            })
            .collect::<Vec<_>>();
        set_remote_parent_from_kafka_headers(&span, &headers);
    }
    span
}
