use super::{pb, NativeHistogram, WireError, schema_i8, v2_spans, counts, validate_spans_and_counts, is_v2_float, v2_reset_hint, v2_zero_count, v2_count};

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn v2_histogram_to_native(histogram: &pb::v2::Histogram) -> Result<NativeHistogram, WireError> {
    let schema = schema_i8(histogram.schema)?;
    let positive_spans = v2_spans(&histogram.positive_spans);
    let positive_counts = counts(&histogram.positive_counts, &histogram.positive_deltas);
    let negative_spans = v2_spans(&histogram.negative_spans);
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
        is_float: is_v2_float(histogram),
        reset_hint: v2_reset_hint(histogram.reset_hint),
        zero_threshold: histogram.zero_threshold,
        zero_count: v2_zero_count(histogram),
        count: v2_count(histogram),
        sum: histogram.sum,
        positive_spans,
        positive_counts,
        negative_spans,
        negative_counts,
        custom_values,
        start_timestamp_ms: (histogram.start_timestamp != 0).then_some(histogram.start_timestamp),
    })
}
