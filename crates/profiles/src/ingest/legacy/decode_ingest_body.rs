use super::*;

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn decode_ingest_body(
    query: &IngestQuery,
    content_type: Option<&str>,
    body: bytes::Bytes,
    max: ByteSize,
) -> Result<RawProfile, ProfilesError> {
    decode_ingest_body_with_limits(
        query,
        content_type,
        body,
        max,
        LegacyDecodeLimits::default(),
    )
    .await
}
