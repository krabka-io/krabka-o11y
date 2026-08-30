use super::{Bytes, ProducerRecord};

#[must_use]
/// Builds a keyed producer record for the ruler topics.
///
/// Shared by the state and recording-rule sinks so both agree on the shape,
/// and separated from the send so it can be checked without a broker. The
/// partition is deliberately absent: the producer keys on `key`, which is
/// what keeps a compacted topic's records for one entity on one partition.
pub(crate) fn keyed_producer_record(topic: String, key: Bytes, value: Vec<u8>) -> ProducerRecord {
    ProducerRecord {
        topic,
        partition: None,
        key: Some(key),
        value: Some(Bytes::from(value)),
        ..Default::default()
    }
}
