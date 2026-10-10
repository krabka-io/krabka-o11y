use super::{NativeHistogram, WireError, validate_spans_and_counts};

/// Admits a histogram decoded from either `remote_write` version once its
/// spans and counts pass [`validate_spans_and_counts`].
pub(crate) fn validated_native_histogram(
    histogram: NativeHistogram,
) -> Result<NativeHistogram, WireError> {
    validate_spans_and_counts(
        histogram.schema,
        &histogram.positive_spans,
        &histogram.positive_counts,
        &histogram.negative_spans,
        &histogram.negative_counts,
        histogram.custom_values.as_deref(),
    )?;
    Ok(histogram)
}
