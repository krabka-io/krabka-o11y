use super::{Bytes, ProducerRecord, WAL_TOPIC, ProducerHeader};

/// Builds the WAL producer record for a serialized entry.
///
/// Separated from the send so the record's shape can be checked without a
/// broker: the topic it lands on, that partitioning is left to the producer,
/// and that the key and value are not transposed.
pub(crate) fn wal_producer_record(
    key: Bytes,
    value: Vec<u8>,
    trace_headers: Vec<(String, String)>,
) -> ProducerRecord {
    ProducerRecord {
        topic: WAL_TOPIC.to_string(),
        // No explicit partition: the producer's partitioner keys on `key`, so
        // every record for a series lands on one partition and stays ordered.
        partition: None,
        key: Some(key),
        value: Some(Bytes::from(value)),
        headers: trace_headers
            .into_iter()
            .map(|(key, value)| ProducerHeader {
                key,
                value: Some(Bytes::from(value.into_bytes())),
            })
            .collect(),
        ..Default::default()
    }
}
