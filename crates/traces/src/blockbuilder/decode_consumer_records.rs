use super::{ConsumerRecord, BTreeMap, PartitionWindow, TracesError, SpanRecord};

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
        let Some(value) = &record.value else {
            continue;
        };
        let decoded = SpanRecord::decode(value)?;
        let window = windows.entry(record.partition).or_insert(PartitionWindow {
            offset_range: (record.offset, record.offset),
            records: Vec::new(),
        });
        window.offset_range.0 = window.offset_range.0.min(record.offset);
        window.offset_range.1 = window.offset_range.1.max(record.offset);
        window.records.push(decoded);
    }
    Ok(windows)
}
