use super::{
    BooleanBuilder, Float64Builder, HistogramCodecError, Int8Builder, Int64Builder, ListBuilder,
    NativeHistogram, RecordBatch, StructBuilder, UInt64Builder, append_f64_list, append_spans,
    native_histogram_schema, new_f64_list_builder, new_span_list_builder,
    validate_span_count_consistency,
};

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

    let mut columns = NativeHistogramColumns::new();

    for (fingerprint, timestamp, histogram) in rows {
        columns.fingerprints.append_value(*fingerprint);
        columns.timestamps.append_value(*timestamp);
        columns.schemas.append_value(histogram.schema);
        columns.is_floats.append_value(histogram.is_float);
        columns
            .reset_hints
            .append_value(histogram.reset_hint.as_i8());
        columns
            .zero_thresholds
            .append_value(histogram.zero_threshold);
        columns.zero_counts.append_value(histogram.zero_count);
        columns.counts.append_value(histogram.count);
        columns.sums.append_value(histogram.sum);
        append_spans(&mut columns.positive_spans, &histogram.positive_spans);
        append_f64_list(&mut columns.positive_counts, &histogram.positive_counts);
        append_spans(&mut columns.negative_spans, &histogram.negative_spans);
        append_f64_list(&mut columns.negative_counts, &histogram.negative_counts);
        match &histogram.custom_values {
            Some(values) => append_f64_list(&mut columns.custom_values, values),
            None => columns.custom_values.append(false),
        }
        match histogram.start_timestamp_ms {
            Some(start_timestamp) => columns.start_timestamps.append_value(start_timestamp),
            None => columns.start_timestamps.append_null(),
        }
    }

    Ok(RecordBatch::try_new(
        native_histogram_schema(),
        columns.finish(),
    )?)
}

#[derive(krabka_column_macros::ColumnBuilders)]
struct NativeHistogramColumns {
    fingerprints: UInt64Builder,
    timestamps: Int64Builder,
    schemas: Int8Builder,
    is_floats: BooleanBuilder,
    reset_hints: Int8Builder,
    zero_thresholds: Float64Builder,
    zero_counts: Float64Builder,
    counts: Float64Builder,
    sums: Float64Builder,
    #[column(init = "new_span_list_builder()")]
    positive_spans: ListBuilder<StructBuilder>,
    #[column(init = "new_f64_list_builder()")]
    positive_counts: ListBuilder<Float64Builder>,
    #[column(init = "new_span_list_builder()")]
    negative_spans: ListBuilder<StructBuilder>,
    #[column(init = "new_f64_list_builder()")]
    negative_counts: ListBuilder<Float64Builder>,
    #[column(init = "new_f64_list_builder()")]
    custom_values: ListBuilder<Float64Builder>,
    start_timestamps: Int64Builder,
}
