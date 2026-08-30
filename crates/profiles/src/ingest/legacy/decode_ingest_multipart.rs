use super::{
    ByteSize, IngestQuery, LegacyDecodeLimits, ProfilesError, RawProfile,
    decode_ingest_multipart_with_limits,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn decode_ingest_multipart(
    query: &IngestQuery,
    content_type: &str,
    body: bytes::Bytes,
    max: ByteSize,
) -> Result<RawProfile, ProfilesError> {
    decode_ingest_multipart_with_limits(
        query,
        content_type,
        body,
        max,
        LegacyDecodeLimits::default(),
    )
    .await
}
