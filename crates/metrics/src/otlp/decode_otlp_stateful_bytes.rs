use super::*;

/// Decodes a protobuf-encoded `MetricsData` body under `options`, keeping
/// the series that translate when others in the body are rejected.
pub(crate) fn decode_otlp_stateful_bytes_partial(
    body: &[u8],
    options: OtlpDecodeOptions<'_>,
) -> Result<PartialOtlpDecode, OtlpError> {
    decode_otlp_inner_partial(&MetricsData::decode(body)?, options)
}

/// Decodes a protobuf-encoded `MetricsData` body and handles the delta state.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_otlp_stateful_bytes(
    body: &[u8],
    strategy: TranslationStrategy,
    accumulator: &mut DeltaAccumulator,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_otlp_stateful(&MetricsData::decode(body)?, strategy, accumulator)
}
