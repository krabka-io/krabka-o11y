//! WAL-head consumer types and replay flow.

use krabka_observability::{
    ReadinessGate,
    persisted_format::{PersistedFormatError, PersistedHeader, validate_persisted_format},
    wal_consumer_metrics::WalConsumerMetrics,
};

use super::{
    BTreeMap, ConsumerRecord, Offset, PartitionIndex, Time, WalHead, WalRecord, WalRecordAt,
};

#[async_trait::async_trait]
pub trait WalHeadConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), WalHeadConsumerError>;
}

#[derive(Debug, thiserror::Error)]
/// An error from the metric WAL consumer that maintains the recent-sample head.
pub enum WalHeadConsumerError {
    #[error("metrics WAL consumer poll failed: {0}")]
    Poll(String),

    #[error("metrics WAL record has unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error(transparent)]
    Replay(#[from] WalHeadReplayError),

    #[error("metrics WAL consumer commit failed: {0}")]
    Commit(String),

    #[error("metrics WAL consumer close failed: {0}")]
    Close(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalHeadConsumerLoopSummary {
    pub polls: usize,
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<WalHeadPartitionOffset>,
}

impl WalHeadConsumerLoopSummary {
    pub(crate) fn record_poll(
        &mut self,
        polled_records: usize,
        replayed_records: usize,
        committed_offsets: impl IntoIterator<Item = WalHeadPartitionOffset>,
    ) {
        self.polls += 1;
        self.polled_records += polled_records;
        self.replayed_records += replayed_records;
        self.committed_offsets.extend(committed_offsets);
    }
}

#[async_trait::async_trait]
pub trait WalHeadConsumerPoll: Send {
    async fn poll(&mut self, timeout: Time) -> Result<Vec<ConsumerRecord>, WalHeadConsumerError>;

    /// Awaits the consumer coordinator's shutdown before the role exits.
    async fn close(self) -> Result<(), WalHeadConsumerError>
    where
        Self: Sized;

    async fn recovery_state(&mut self) -> Option<WalHeadRecoveryState> {
        None
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalHeadConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Option<Vec<u8>>,
}

/// Checks the persisted-format header of every polled record on `topic`, and
/// only then converts the whole batch into replay records.
///
/// # Errors
/// Returns the format error of the first record on `topic` whose header is
/// missing, duplicated, or names a version this build does not read.
pub(crate) fn checked_replay_records(
    records: Vec<ConsumerRecord>,
    topic: &str,
) -> Result<Vec<WalHeadConsumerRecord>, PersistedFormatError> {
    for record in records.iter().filter(|record| record.topic == topic) {
        validate_persisted_format(record.headers.iter().map(|header| PersistedHeader {
            key: header.key.as_str(),
            value: header.value.as_deref(),
        }))?;
    }
    Ok(records
        .into_iter()
        .map(|record| WalHeadConsumerRecord {
            topic: record.topic,
            partition: record.partition.into(),
            offset: record.offset.into(),
            value: record.value.map(|value| value.to_vec()),
        })
        .collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalHeadPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset: the next offset after the last replayed record.
    pub offset: Offset,
}

pub use krabka_observability::wal_consumer_metrics::WalPartitionAssignment;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalHeadRecoveryState {
    pub assignment: Vec<WalPartitionAssignment>,
    pub at_log_end: bool,
}

#[derive(Debug, thiserror::Error)]
/// An error while applying one metric WAL record to the recent-sample head.
pub enum WalHeadReplayError {
    #[error("metrics WAL record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("metrics WAL record decode failed: {0}")]
    Decode(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalHeadReplayResult {
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<WalHeadPartitionOffset>,
}

pub(crate) async fn poll_checked_replay_records<C, E>(
    consumer: &mut C,
    topic: &str,
    timeout: Time,
    observe_records: impl FnOnce(&[ConsumerRecord]),
    map_poll_error: impl FnOnce(WalHeadConsumerError) -> E,
    map_format_error: impl FnOnce(PersistedFormatError) -> E,
) -> Result<Vec<WalHeadConsumerRecord>, E>
where
    C: WalHeadConsumerPoll + ?Sized,
{
    let records = consumer.poll(timeout).await.map_err(map_poll_error)?;
    observe_records(&records);
    checked_replay_records(records, topic).map_err(map_format_error)
}

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

struct DecodedWalRecord {
    record: WalRecord,
    partition: PartitionIndex,
    offset: Offset,
}

#[tracing::instrument(
    level = "debug",
    name = "metrics.wal_head.replay",
    skip_all,
    fields(wal_topic = %wal_topic, records = records.len()),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub fn replay_wal_head_records(
    head: &WalHead,
    wal_topic: &str,
    records: &[WalHeadConsumerRecord],
) -> Result<WalHeadReplayResult, WalHeadReplayError> {
    let mut committed_offsets = BTreeMap::<PartitionIndex, Offset>::new();
    let mut newest_timestamp_ms: Option<i64> = None;
    // Decoded first, applied second. Decoding touches no shared state, so it
    // stays outside the head's write lock, and the whole poll then reaches the
    // head as one batch: one lock acquisition and at most one copy-on-write
    // clone, rather than one of each per record. A query running beside the
    // tail sees the poll's records all at once or not at all.
    let mut decoded = Vec::with_capacity(records.len());
    for record in records {
        if record.topic != wal_topic {
            continue;
        }
        let value = record
            .value
            .as_deref()
            .ok_or(WalHeadReplayError::MissingValue {
                partition: record.partition,
                offset: record.offset,
            })?;
        let wal_record = WalRecord::decode(value)
            .map_err(|error| WalHeadReplayError::Decode(error.to_string()))?;
        if let Some(timestamp_ms) = wal_record_max_timestamp_ms(&wal_record) {
            newest_timestamp_ms =
                Some(newest_timestamp_ms.map_or(timestamp_ms, |current| current.max(timestamp_ms)));
        }
        // partition/offset are now the shared krabka_ids types promql also uses,
        // so they pass straight through with no conversion at the seam.
        decoded.push(DecodedWalRecord {
            record: wal_record,
            partition: record.partition,
            offset: record.offset,
        });
        committed_offsets
            .entry(record.partition)
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }
    let replayed_records = decoded.len();
    head.apply_wal_records_at(
        decoded
            .iter()
            .map(|record| WalRecordAt::new(&record.record, record.partition, record.offset)),
    );
    if let Some(timestamp_ms) = newest_timestamp_ms {
        let _ = head.prune(timestamp_ms);
    }

    Ok(WalHeadReplayResult {
        polled_records: records.len(),
        replayed_records,
        committed_offsets: committed_offsets
            .into_iter()
            .map(|entry| WalHeadPartitionOffset {
                partition: entry.0,
                offset: entry.1,
            })
            .collect(),
    })
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_wal_head_consumer_loop<C, Stop>(
    consumer: &mut C,
    head: &WalHead,
    wal_topic: &str,
    timeout: Time,
    metrics: Option<&WalConsumerMetrics>,
    catch_up: Option<&ReadinessGate>,
    mut should_stop: Stop,
) -> Result<WalHeadConsumerLoopSummary, WalHeadConsumerError>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
    Stop: FnMut(&WalHeadConsumerLoopSummary) -> bool,
{
    let mut summary = WalHeadConsumerLoopSummary::default();
    loop {
        let result =
            poll_wal_head_consumer_once(consumer, head, wal_topic, timeout, metrics).await?;
        summary.record_poll(
            result.polled_records,
            result.replayed_records,
            result.committed_offsets,
        );

        if let Some(recovery) = consumer.recovery_state().await {
            if let Some(metrics) = metrics {
                metrics.record_assignment(&recovery.assignment, recovery.at_log_end);
            }
            if let Some(gate) = catch_up {
                if recovery.at_log_end {
                    gate.mark_ready();
                } else {
                    gate.mark_unready();
                }
            }
        } else if let Some(gate) = catch_up {
            if result.replayed_records == 0 {
                gate.mark_ready();
            } else {
                gate.mark_unready();
            }
        }

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}

#[tracing::instrument(
    level = "debug",
    name = "metrics.wal_head.poll_once",
    skip_all,
    fields(wal_topic = %wal_topic, polled = tracing::field::Empty, replayed = tracing::field::Empty),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn poll_wal_head_consumer_once<C>(
    consumer: &mut C,
    head: &WalHead,
    wal_topic: &str,
    timeout: Time,
    metrics: Option<&WalConsumerMetrics>,
) -> Result<WalHeadReplayResult, WalHeadConsumerError>
where
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
{
    let replay_records = poll_checked_replay_records(
        consumer,
        wal_topic,
        timeout,
        |records| {
            if let Some(metrics) = metrics {
                metrics.record_poll(records);
            }
        },
        |error| error,
        |error| WalHeadConsumerError::UnsupportedFormat(error.to_string()),
    )
    .await?;
    let result = replay_wal_head_records(head, wal_topic, &replay_records)?;
    finish_wal_consumer_replay(
        consumer,
        result,
        |error| error,
        || {
            if let Some(metrics) = metrics {
                metrics.record_commit();
            }
        },
    )
    .await
}

pub(crate) fn wal_record_max_timestamp_ms(record: &WalRecord) -> Option<i64> {
    record
        .payload
        .timestamp_ms()
        .into_iter()
        .chain(
            record
                .exemplars
                .iter()
                .map(|exemplar| exemplar.timestamp_ms),
        )
        .max()
}
