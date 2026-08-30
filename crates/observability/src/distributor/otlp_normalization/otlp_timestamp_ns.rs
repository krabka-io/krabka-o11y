use super::{DistributorError, Value, validate_ingest_timestamp_ns};

pub(crate) fn otlp_timestamp_ns(timestamp: &Value) -> Result<i64, DistributorError> {
    let timestamp_ns = match timestamp {
        Value::String(timestamp) => timestamp
            .parse()
            .map_err(|_| DistributorError::InvalidTimestamp),
        Value::Number(timestamp) => timestamp.as_i64().ok_or(DistributorError::InvalidTimestamp),
        _ => Err(DistributorError::InvalidTimestamp),
    }?;
    validate_ingest_timestamp_ns(timestamp_ns)
}
