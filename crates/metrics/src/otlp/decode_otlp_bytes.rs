use super::*;

/// Decodes a protobuf-encoded `MetricsData` body.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_otlp_bytes(
    body: &[u8],
    strategy: TranslationStrategy,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_otlp(&MetricsData::decode(body)?, strategy)
}
