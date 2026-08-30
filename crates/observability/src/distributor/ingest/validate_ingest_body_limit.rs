use super::*;

pub(crate) fn validate_ingest_body_limit(
    state: &DistributorState,
    body: ByteSize,
) -> Result<(), DistributorError> {
    let Some(max) = state.max_ingest_body else {
        return Ok(());
    };
    if body > max {
        // The error carries plain integers so its rendered message is fixed by
        // the `#[error]` format string alone.
        return Err(DistributorError::IngestBodyTooLarge {
            body_bytes: body.bytes_usize(),
            max_bytes: max.bytes_usize(),
        });
    }
    Ok(())
}
