use super::{ByteSize, ByteSizeExt, DistributorError, Limits};

pub(crate) fn validate_ingest_body_limit(
    limits: &Limits,
    body: ByteSize,
) -> Result<(), DistributorError> {
    if limits.max_ingest_body <= ByteSize::ZERO || body <= limits.max_ingest_body {
        return Ok(());
    }
    // The error carries plain integers so its rendered message is fixed by
    // the `#[error]` format string alone.
    Err(DistributorError::IngestBodyTooLarge {
        body_bytes: body.bytes_usize(),
        max_bytes: limits.max_ingest_body.bytes_usize(),
    })
}
