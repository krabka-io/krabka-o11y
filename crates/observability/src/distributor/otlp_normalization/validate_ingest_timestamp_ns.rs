use super::*;

pub(crate) fn validate_ingest_timestamp_ns(timestamp_ns: i64) -> Result<i64, DistributorError> {
    if timestamp_ns < 0 {
        Err(DistributorError::InvalidTimestamp)
    } else {
        Ok(timestamp_ns)
    }
}
