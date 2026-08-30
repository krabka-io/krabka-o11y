use super::*;

///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_consumer_records(records: Vec<ConsumerRecord>) -> Result<Vec<SpanRecord>, SinkError> {
    records
        .into_iter()
        .filter_map(|record| {
            record.value.map(|value| {
                let size = ByteSize::from_bytes(u64::try_from(value.len()).unwrap_or(u64::MAX));
                wal::SpanRecord::decode(&value)
                    .map(|wal| project_wal_record(wal, size))
                    .map_err(|err| SinkError::Decode(err.to_string()))
            })
        })
        .collect()
}
