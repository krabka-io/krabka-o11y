use krabka_observability::persisted_format::{PersistedHeader, validate_persisted_format};

use super::{BTreeMap, ConsumerRecord, OffsetRange, PartitionWindow, SpanRecord, TracesError};

/// Decode Kafka consumer records into per-partition span windows.
///
/// This function ignores tombstones and records without values. They do not
/// extend the inclusive offset range.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_consumer_records(
    records: &[ConsumerRecord],
) -> Result<BTreeMap<i32, PartitionWindow>, TracesError> {
    let mut windows = BTreeMap::<i32, PartitionWindow>::new();
    for record in records {
        validate_persisted_format(record.headers.iter().map(|header| PersistedHeader {
            key: header.key.as_str(),
            value: header.value.as_deref(),
        }))
        .map_err(|error| TracesError::Wal(error.to_string()))?;
        let Some(value) = &record.value else {
            continue;
        };
        let decoded = SpanRecord::decode(value)?;
        let window = windows.entry(record.partition).or_insert(PartitionWindow {
            offset_range: OffsetRange::new(record.offset, record.offset),
            records: Vec::new(),
        });
        window.offset_range.min = window.offset_range.min.min(record.offset);
        window.offset_range.max = window.offset_range.max.max(record.offset);
        window.records.push(decoded);
    }
    Ok(windows)
}
