use krabka_observability::persisted_format::{
    PERSISTED_FORMAT_HEADER, PERSISTED_FORMAT_VERSION, keyed_persisted_record,
};

use super::{Bytes, ProducerHeader, ProducerRecord, WAL_TOPIC};

#[must_use]
/// Builds a keyed producer record for a compacted topic.
///
/// Separated from the send so the record's shape can be checked without a
/// broker. The partition is deliberately absent: the producer keys on `key`,
/// which is what keeps a compacted topic's records for one entity together.
pub(crate) fn keyed_producer_record(topic: String, key: Bytes, value: Vec<u8>) -> ProducerRecord {
    keyed_persisted_record(topic, key, value)
}

pub(crate) struct TraceHeader {
    pub(crate) key: String,
    pub(crate) value: String,
}

/// Builds the WAL producer record for a serialized entry.
///
/// Separated from the send so the record's shape can be checked without a
/// broker: the topic it lands on, that partitioning is left to the producer,
/// and that the key and value are not transposed.
pub(crate) fn wal_producer_record(
    key: Bytes,
    value: Vec<u8>,
    trace_headers: Vec<TraceHeader>,
) -> ProducerRecord {
    ProducerRecord {
        topic: WAL_TOPIC.to_string(),
        // No explicit partition: the producer's partitioner keys on `key`, so
        // every record for a series lands on one partition and stays ordered.
        partition: None,
        key: Some(key),
        value: Some(Bytes::from(value)),
        headers: std::iter::once(ProducerHeader {
            key: PERSISTED_FORMAT_HEADER.to_string(),
            value: Some(Bytes::from_static(PERSISTED_FORMAT_VERSION)),
        })
        .chain(trace_headers.into_iter().map(|header| ProducerHeader {
            key: header.key,
            value: Some(Bytes::from(header.value.into_bytes())),
        }))
        .collect(),
        ..Default::default()
    }
}
