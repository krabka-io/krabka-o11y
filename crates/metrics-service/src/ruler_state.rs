//! Durable ruler state records, sinks, and consumer replay.

pub use krabka_observability::topic_contract::METRICS_RULER_STATE_TOPIC as RULER_STATE_TOPIC;

use super::{
    Arc, BTreeMap, Bytes, MetricStore, Offset, PartitionIndex, Producer, PrometheusApiState,
    RulerAlertStateRecord, RulerGroupStateRecord, RulerStateSink, RulerWalError, Time,
    WalHeadConsumerCommit, WalHeadConsumerLoopSummary, WalHeadConsumerPoll, WalHeadConsumerRecord,
    WalHeadPartitionOffset, WalHeadReplayResult, keyed_producer_record,
    poll_ruler_state_consumer_once,
};

/// Applies a restored ruler group or alert record to the API state.
pub fn apply_ruler_state_record<S: MetricStore>(
    state: &PrometheusApiState<S>,
    record: RulerStateWalRecord,
) {
    match record {
        RulerStateWalRecord::Group(record) => state.apply_ruler_group_state(record),
        RulerStateWalRecord::Alert(record) => state.apply_ruler_alert_state(record),
    }
}

pub struct KafkaRulerStateSink {
    pub(crate) producer: Arc<Producer>,
    pub(crate) topic: String,
}

impl KafkaRulerStateSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }

    pub(crate) async fn append_state_record(
        &self,
        record: RulerStateWalRecord,
    ) -> Result<(), RulerWalError> {
        let key = ruler_state_compaction_key(&record);
        let value = record
            .encode()
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        self.producer
            .send(keyed_producer_record(self.topic.clone(), key, value))
            .await
            .map_err(|error| RulerWalError::Append(error.to_string()))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl RulerStateSink for KafkaRulerStateSink {
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.append_state_record(RulerStateWalRecord::Group(record))
            .await
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.append_state_record(RulerStateWalRecord::Alert(record))
            .await
    }
}

pub struct PrometheusRulerStateSink<S: MetricStore> {
    pub(crate) state: Arc<PrometheusApiState<S>>,
}

impl<S: MetricStore> PrometheusRulerStateSink<S> {
    #[must_use]
    pub fn new(state: Arc<PrometheusApiState<S>>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl<S> RulerStateSink for PrometheusRulerStateSink<S>
where
    S: MetricStore + 'static,
{
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.state.apply_ruler_group_state(record);
        Ok(())
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.state.apply_ruler_alert_state(record);
        Ok(())
    }
}

#[tracing::instrument(
    level = "debug",
    name = "metrics.ruler_state.replay",
    skip_all,
    fields(state_topic = %state_topic, records = records.len()),
    err
)]
///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub fn replay_ruler_state_records<S: MetricStore>(
    state: &PrometheusApiState<S>,
    state_topic: &str,
    records: &[WalHeadConsumerRecord],
) -> Result<WalHeadReplayResult, RulerStateReplayError> {
    let mut committed_offsets = BTreeMap::<PartitionIndex, Offset>::new();
    let mut replayed_records = 0;
    for record in records {
        if record.topic != state_topic {
            continue;
        }
        let value = record
            .value
            .as_deref()
            .ok_or(RulerStateReplayError::MissingValue {
                partition: record.partition,
                offset: record.offset,
            })?;
        let state_record = RulerStateWalRecord::decode(value)
            .map_err(|error| RulerStateReplayError::Decode(error.to_string()))?;
        apply_ruler_state_record(state, state_record);
        replayed_records += 1;
        committed_offsets
            .entry(record.partition)
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
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

#[must_use]
/// Returns the stable broker compaction key for one ruler group or alert.
pub fn ruler_state_compaction_key(record: &RulerStateWalRecord) -> Bytes {
    match record {
        RulerStateWalRecord::Group(record) => Bytes::from(format!(
            "group\0{}\0{}\0{}",
            record.tenant, record.namespace, record.group
        )),
        RulerStateWalRecord::Alert(record) => {
            if record.labels.has_byte_values() {
                let mut bytes = b"alert-bytes\0".to_vec();
                for field in [record.tenant.as_bytes(), record.rule_id.as_bytes()] {
                    bytes.extend_from_slice(&(field.len() as u64).to_le_bytes());
                    bytes.extend_from_slice(field);
                }
                bytes.extend_from_slice(&record.labels.byte_key());
                return Bytes::from(bytes);
            }
            let mut key = format!("alert\0{}\0{}", record.tenant, record.rule_id);
            for label in record.labels.iter() {
                key.push('\0');
                key.push_str(label.name);
                key.push('=');
                key.push_str(label.value);
            }
            Bytes::from(key)
        }
    }
}

#[derive(Debug, thiserror::Error)]
/// An error from the consumer that restores ruler groups and active alerts.
pub enum RulerStateConsumerError {
    #[error("ruler state consumer poll failed: {0}")]
    Poll(String),

    #[error("ruler state record has unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error(transparent)]
    Replay(#[from] RulerStateReplayError),

    #[error("ruler state consumer commit failed: {0}")]
    Commit(String),
}

pub struct RulerStateFanoutSink<A, B> {
    pub(crate) first: A,
    pub(crate) second: B,
}

impl<A, B> RulerStateFanoutSink<A, B> {
    #[must_use]
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

#[async_trait::async_trait]
impl<A, B> RulerStateSink for RulerStateFanoutSink<A, B>
where
    A: RulerStateSink,
    B: RulerStateSink,
{
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.first.persist_ruler_group_state(record.clone()).await?;
        self.second.persist_ruler_group_state(record).await
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.first.persist_ruler_alert_state(record.clone()).await?;
        self.second.persist_ruler_alert_state(record).await
    }
}

#[derive(Debug, thiserror::Error)]
/// An error while applying one ruler-state WAL record.
pub enum RulerStateReplayError {
    #[error("ruler state record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("ruler state record decode failed: {0}")]
    Decode(String),
}

/// The compacted ruler-state topic: alert and rule-group state per entity.
///
/// Its `cleanup.policy=compact` requirement is part of the topic contract in
/// [`krabka_observability::topic_contract`], which this crate re-exports the
/// name from.

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RulerStateWalRecord {
    Group(RulerGroupStateRecord),
    Alert(RulerAlertStateRecord),
}

impl RulerStateWalRecord {
    ///
    /// # Errors
    /// Returns an error if the operation cannot be completed.
    pub fn encode(&self) -> Result<Vec<u8>, RulerStateWalRecordError> {
        serde_json::to_vec(self)
            .map_err(|error| RulerStateWalRecordError::Encode(error.to_string()))
    }

    ///
    /// # Errors
    /// Returns an error if the operation cannot be completed.
    pub fn decode(bytes: &[u8]) -> Result<Self, RulerStateWalRecordError> {
        serde_json::from_slice(bytes)
            .map_err(|error| RulerStateWalRecordError::Decode(error.to_string()))
    }
}

#[derive(Debug, thiserror::Error)]
/// An error while encoding or decoding a ruler-state WAL record.
pub enum RulerStateWalRecordError {
    #[error("ruler state record encode failed: {0}")]
    Encode(String),

    #[error("ruler state record decode failed: {0}")]
    Decode(String),
}

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn run_ruler_state_consumer_loop<S, C, Stop>(
    consumer: &mut C,
    state: &PrometheusApiState<S>,
    state_topic: &str,
    timeout: Time,
    mut should_stop: Stop,
) -> Result<WalHeadConsumerLoopSummary, RulerStateConsumerError>
where
    S: MetricStore,
    C: WalHeadConsumerPoll + WalHeadConsumerCommit + ?Sized,
    Stop: FnMut(&WalHeadConsumerLoopSummary) -> bool,
{
    let mut summary = WalHeadConsumerLoopSummary::default();
    loop {
        let result = poll_ruler_state_consumer_once(consumer, state, state_topic, timeout).await?;
        summary.record_poll(
            result.polled_records,
            result.replayed_records,
            result.committed_offsets,
        );

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}
