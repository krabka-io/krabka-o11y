use super::{NativeHistogram, RecordBatch, HistogramCodecError, validate_span_count_consistency, UInt64Builder, Int64Builder, Int8Builder, BooleanBuilder, Float64Builder, new_span_list_builder, new_f64_list_builder, append_spans, append_f64_list, ArrayRef, Arc, native_histogram_schema};

/// Encodes `(fingerprint, timestamp, NativeHistogram)` rows into a
/// `RecordBatch` that matches [`native_histogram_schema`].
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_native_histograms(
    rows: &[(u64, i64, NativeHistogram)],
) -> Result<RecordBatch, HistogramCodecError> {
    for (_, _, histogram) in rows {
        validate_span_count_consistency(&histogram.positive_spans, &histogram.positive_counts)?;
        validate_span_count_consistency(&histogram.negative_spans, &histogram.negative_counts)?;
    }

    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut schemas = Int8Builder::new();
    let mut is_floats = BooleanBuilder::new();
    let mut reset_hints = Int8Builder::new();
    let mut zero_thresholds = Float64Builder::new();
    let mut zero_counts = Float64Builder::new();
    let mut counts = Float64Builder::new();
    let mut sums = Float64Builder::new();
    let mut positive_spans = new_span_list_builder();
    let mut positive_counts = new_f64_list_builder();
    let mut negative_spans = new_span_list_builder();
    let mut negative_counts = new_f64_list_builder();
    let mut custom_values = new_f64_list_builder();
    let mut start_timestamps = Int64Builder::new();

    for (fingerprint, timestamp, histogram) in rows {
        fingerprints.append_value(*fingerprint);
        timestamps.append_value(*timestamp);
        schemas.append_value(histogram.schema);
        is_floats.append_value(histogram.is_float);
        reset_hints.append_value(histogram.reset_hint.as_i8());
        zero_thresholds.append_value(histogram.zero_threshold);
        zero_counts.append_value(histogram.zero_count);
        counts.append_value(histogram.count);
        sums.append_value(histogram.sum);
        append_spans(&mut positive_spans, &histogram.positive_spans);
        append_f64_list(&mut positive_counts, &histogram.positive_counts);
        append_spans(&mut negative_spans, &histogram.negative_spans);
        append_f64_list(&mut negative_counts, &histogram.negative_counts);
        match &histogram.custom_values {
            Some(values) => append_f64_list(&mut custom_values, values),
            None => custom_values.append(false),
        }
        match histogram.start_timestamp_ms {
            Some(start_timestamp) => start_timestamps.append_value(start_timestamp),
            None => start_timestamps.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(schemas.finish()),
        Arc::new(is_floats.finish()),
        Arc::new(reset_hints.finish()),
        Arc::new(zero_thresholds.finish()),
        Arc::new(zero_counts.finish()),
        Arc::new(counts.finish()),
        Arc::new(sums.finish()),
        Arc::new(positive_spans.finish()),
        Arc::new(positive_counts.finish()),
        Arc::new(negative_spans.finish()),
        Arc::new(negative_counts.finish()),
        Arc::new(custom_values.finish()),
        Arc::new(start_timestamps.finish()),
    ];

    Ok(RecordBatch::try_new(native_histogram_schema(), columns)?)
}
