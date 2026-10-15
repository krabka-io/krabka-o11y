use crate::persisted_format::{PersistedFormatError, PersistedHeader, validate_persisted_format};
use crate::wal_produce::wal_record_headers;
use krabka_client_core::ClientSecurity;
use krabka_units::convert::TimeExt;

use crate::{
    Acks, Arc, AutoOffsetReset, BTreeMap, Bytes, CancellationToken, ClientResourcePolicy,
    CompactionFrontier, Consumer, ConsumerError, HotTailPollError, JoinHandle, KafkaWalHeader,
    KafkaWalRecord, LogHotTail, LogWalConsumer, LogWalSink, Mutex, Offset, PartitionIndex,
    Producer, ProducerError, ProducerHeader, ProducerRecord, SharedCompactionFrontier, Time,
    WalConsumerError, WalLogRecord, WalPosition, WalRecordDecodeError, WalSinkError, async_trait,
    decode_native_kafka_log_record, has_native_kafka_log_headers, hot_tail_bucket_key, minutes,
    series_fingerprint, sleep, wal_consumer_metrics::WalConsumerMetrics,
};

mod buffered_log_hot_tail;
mod hot_tail_buffer;
mod kafka_log_wal_consumer;
mod kafka_log_wal_sink;

pub use buffered_log_hot_tail::BufferedLogHotTail;
pub(crate) use hot_tail_buffer::HotTailBuffer;
pub use kafka_log_wal_consumer::KafkaLogWalConsumer;
pub use kafka_log_wal_sink::KafkaLogWalSink;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn decode_kafka_wal_record(
    value: &[u8],
    partition: PartitionIndex,
    offset: Offset,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    let mut record: WalLogRecord = serde_json::from_slice(value)?;
    record.position = Some(WalPosition { partition, offset });
    Ok(record)
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn poll_log_hot_tail_once(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    hot_tail: &BufferedLogHotTail,
    timeout: Time,
) -> Result<usize, HotTailPollError> {
    poll_log_hot_tail_once_with_frontier(consumer, hot_tail, timeout, None).await
}

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
    consumer.records_applied().await;
    Ok(decoded)
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn build_kafka_wal_record(
    topic: impl Into<String>,
    record: &WalLogRecord,
) -> Result<ProducerRecord, WalSinkError> {
    let fingerprint = series_fingerprint(&record.labels);
    let mut headers = vec![
        ProducerHeader {
            key: "krabka-wal-record-type".to_string(),
            value: Some(Bytes::from_static(b"log")),
        },
        ProducerHeader {
            key: "krabka-tenant".to_string(),
            value: Some(Bytes::from(record.tenant.clone())),
        },
    ];
    // The format version, then the current span's W3C trace context, so the
    // compactor can stitch its consume/compaction span onto the ingest trace.
    headers.extend(wal_record_headers());
    Ok(ProducerRecord {
        topic: topic.into(),
        partition: None,
        key: Some(Bytes::from(format!("{}:{fingerprint}", record.tenant))),
        value: Some(Bytes::from(serde_json::to_vec(record)?)),
        headers,
        timestamp_ms: Some(record.timestamp_ns / 1_000_000),
    })
}

/// Decodes one record of the logs WAL topic.
///
/// The topic holds two record kinds. A Krabka distributor writes a JSON record
/// with the `krabka-format-version` header. A plain Kafka client writes a
/// native record: the log line is the value, and `krabka-tenant`,
/// `krabka-log-label-*` and the other `krabka-log-*` headers carry the rest. A
/// native record has no format version header, so a record without one decodes
/// only as a native record.
///
/// # Errors
/// Returns an error when the format version header is malformed or
/// unsupported, when a record without the header is not a native record, or
/// when the record does not decode.
pub fn decode_kafka_wal_record_envelope(
    record: KafkaWalRecord,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    match validate_persisted_format(record.headers.iter().map(|header| PersistedHeader {
        key: header.key.as_str(),
        value: header.value.as_deref(),
    })) {
        Ok(()) => {}
        Err(PersistedFormatError::Missing) if has_native_kafka_log_headers(&record.headers) => {
            return decode_native_kafka_log_record(record);
        }
        Err(error) => return Err(WalRecordDecodeError::UnsupportedFormat(error.to_string())),
    }
    match decode_kafka_wal_record(&record.value, record.partition, record.offset) {
        Ok(record) => Ok(record),
        Err(_) if has_native_kafka_log_headers(&record.headers) => {
            decode_native_kafka_log_record(record)
        }
        Err(error) => Err(error),
    }
}

#[cfg_attr(test, mutants::skip)]

#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_log_hot_tail_poller(
    consumer: Arc<tokio::sync::Mutex<Box<dyn LogWalConsumer>>>,
    hot_tail: BufferedLogHotTail,
    frontier: Option<SharedCompactionFrontier>,
    poll_interval: Time,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let result = tokio::select! {
                () = token.cancelled() => return,
                result = async {
                let mut consumer = consumer.lock().await;
                poll_log_hot_tail_once_with_frontier(
                    consumer.as_mut(),
                    &hot_tail,
                    poll_interval,
                    frontier.as_ref(),
                )
                .await
                } => result,
            };
            let should_back_off = match result {
                Ok(decoded) => decoded == 0,
                Err(error) => {
                    tracing::warn!(%error, "querier WAL hot-tail poll failed; retrying");
                    true
                }
            };
            if should_back_off {
                tokio::select! {
                    () = token.cancelled() => return,
                    () = sleep(poll_interval.to_std()) => {}
                }
            }
        }
    })
}
