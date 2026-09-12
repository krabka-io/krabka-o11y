use super::*;

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

pub(crate) fn decode_otlp_stateful_bytes_with_promoted_resource_attributes(
    body: &[u8],
    strategy: TranslationStrategy,
    accumulator: &mut DeltaAccumulator,
    additional_resource_attributes: &[String],
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_otlp_stateful_with_promoted_resource_attributes(
        &MetricsData::decode(body)?,
        strategy,
        accumulator,
        additional_resource_attributes,
    )
}
