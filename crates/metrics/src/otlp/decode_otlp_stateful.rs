use super::{MetricsData, TranslationStrategy, DeltaAccumulator, DecodedSeries, OtlpError, decode_otlp_inner};

/// Translates OTLP metrics and accumulates delta temporality across calls.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_otlp_stateful(
    data: &MetricsData,
    strategy: TranslationStrategy,
    accumulator: &mut DeltaAccumulator,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    decode_otlp_inner(data, strategy, Some(accumulator))
}
