use super::{
    NativeHistogram, WireError, counts, is_v2_float, pb, schema_i8, v2_count, v2_reset_hint,
    v2_spans, v2_zero_count, validated_native_histogram,
};

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn v2_histogram_to_native(histogram: &pb::v2::Histogram) -> Result<NativeHistogram, WireError> {
    validated_native_histogram(NativeHistogram {
        schema: schema_i8(histogram.schema)?,
        is_float: is_v2_float(histogram),
        reset_hint: v2_reset_hint(histogram.reset_hint),
        zero_threshold: histogram.zero_threshold,
        zero_count: v2_zero_count(histogram),
        count: v2_count(histogram),
        sum: histogram.sum,
        positive_spans: v2_spans(&histogram.positive_spans),
        positive_counts: counts(&histogram.positive_counts, &histogram.positive_deltas)?,
        negative_spans: v2_spans(&histogram.negative_spans),
        negative_counts: counts(&histogram.negative_counts, &histogram.negative_deltas)?,
        custom_values: (!histogram.custom_values.is_empty())
            .then(|| histogram.custom_values.clone()),
        start_timestamp_ms: (histogram.start_timestamp != 0).then_some(histogram.start_timestamp),
    })
}
