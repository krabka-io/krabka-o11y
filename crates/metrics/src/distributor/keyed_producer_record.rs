use super::*;

#[must_use]
/// Builds a keyed producer record for a compacted topic.
///
/// Separated from the send so the record's shape can be checked without a
/// broker. The partition is deliberately absent: the producer keys on `key`,
/// which is what keeps a compacted topic's records for one entity together.
pub(crate) fn keyed_producer_record(topic: String, key: Bytes, value: Vec<u8>) -> ProducerRecord {
    ProducerRecord {
        topic,
        partition: None,
        key: Some(key),
        value: Some(Bytes::from(value)),
        ..Default::default()
    }
}
