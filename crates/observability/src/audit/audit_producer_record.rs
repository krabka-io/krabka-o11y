use axum::body::Bytes;
use krabka_client_producer::{Header, ProducerRecord};
use krabka_ids::PartitionIndex;

use super::AuditRecord;

/// The Kafka record for one audit record, pinned to `partition`.
///
/// The record carries every audit header, and the chain headers with them. It
/// has no key, because the partition is fixed. The producer stamps the record
/// time. The event time is in the OCSF value.
pub fn audit_producer_record(
    topic: &str,
    partition: PartitionIndex,
    record: AuditRecord,
) -> ProducerRecord {
    ProducerRecord {
        topic: topic.to_owned(),
        partition: Some(partition.get()),
        key: None,
        value: Some(Bytes::from(record.value)),
        headers: record
            .headers
            .into_iter()
            .map(|(key, value)| Header {
                key,
                value: Some(Bytes::from(value)),
            })
            .collect(),
        timestamp_ms: None,
    }
}
