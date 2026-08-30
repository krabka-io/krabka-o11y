use super::{
    NativeHistogram, WireError, counts, is_v1_float, pb, schema_i8, v1_count, v1_reset_hint,
    v1_spans, v1_zero_count, validate_spans_and_counts,
};

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn v1_histogram_to_native(histogram: &pb::v1::Histogram) -> Result<NativeHistogram, WireError> {
    let schema = schema_i8(histogram.schema)?;
    let positive_spans = v1_spans(&histogram.positive_spans);
    let positive_counts = counts(&histogram.positive_counts, &histogram.positive_deltas);
    let negative_spans = v1_spans(&histogram.negative_spans);
    let negative_counts = counts(&histogram.negative_counts, &histogram.negative_deltas);
    let custom_values =
        (!histogram.custom_values.is_empty()).then(|| histogram.custom_values.clone());
    validate_spans_and_counts(
        schema,
        &positive_spans,
        &positive_counts,
        &negative_spans,
        &negative_counts,
        custom_values.as_deref(),
    )?;

    Ok(NativeHistogram {
        schema,
        is_float: is_v1_float(histogram),
        reset_hint: v1_reset_hint(histogram.reset_hint),
        zero_threshold: histogram.zero_threshold,
        zero_count: v1_zero_count(histogram),
        count: v1_count(histogram),
        sum: histogram.sum,
        positive_spans,
        positive_counts,
        negative_spans,
        negative_counts,
        custom_values,
        start_timestamp_ms: None,
    })
}
