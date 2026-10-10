use super::{
    DecodedSeries, DeltaAccumulator, MetricsData, OtlpDecodeOptions, OtlpError,
    TranslationStrategy, decode_otlp_inner,
};

/// Translates OTLP metrics into the common ingest representation.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_otlp(
    data: &MetricsData,
    strategy: TranslationStrategy,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let mut accumulator = DeltaAccumulator::default();
    decode_otlp_inner(
        data,
        OtlpDecodeOptions {
            strategy,
            accumulator: Some(&mut accumulator),
            additional_resource_attributes: &[],
        },
    )
}
