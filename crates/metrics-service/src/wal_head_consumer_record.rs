use krabka_observability::persisted_format::{PersistedFormatError, validate_persisted_format};

use super::{ConsumerRecord, Offset, PartitionIndex};

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
        validate_persisted_format(
            record
                .headers
                .iter()
                .map(|header| (header.key.as_str(), header.value.as_deref())),
        )?;
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
