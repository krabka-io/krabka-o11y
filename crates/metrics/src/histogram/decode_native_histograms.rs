use super::*;

/// Decodes a `RecordBatch` that [`encode_native_histograms`] produced.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_native_histograms(
    batch: &RecordBatch,
) -> Result<Vec<(u64, i64, NativeHistogram)>, HistogramCodecError> {
    let fingerprints = typed_column::<UInt64Array>(batch, COL_FINGERPRINT)?;
    let timestamps = typed_column::<Int64Array>(batch, COL_TIMESTAMP)?;
    let schemas = typed_column::<Int8Array>(batch, COL_NH_SCHEMA)?;
    let is_floats = typed_column::<BooleanArray>(batch, COL_NH_IS_FLOAT)?;
    let reset_hints = typed_column::<Int8Array>(batch, COL_NH_RESET_HINT)?;
    let zero_thresholds = typed_column::<Float64Array>(batch, COL_NH_ZERO_THRESHOLD)?;
    let zero_counts = typed_column::<Float64Array>(batch, COL_NH_ZERO_COUNT)?;
    let counts = typed_column::<Float64Array>(batch, COL_NH_COUNT)?;
    let sums = typed_column::<Float64Array>(batch, COL_NH_SUM)?;
    let positive_spans = typed_column::<ListArray>(batch, COL_NH_POS_SPANS)?;
    let positive_counts = typed_column::<ListArray>(batch, COL_NH_POS_COUNTS)?;
    let negative_spans = typed_column::<ListArray>(batch, COL_NH_NEG_SPANS)?;
    let negative_counts = typed_column::<ListArray>(batch, COL_NH_NEG_COUNTS)?;
    let custom_values = typed_column::<ListArray>(batch, COL_NH_CUSTOM_VALUES)?;
    let start_timestamps = typed_column::<Int64Array>(batch, COL_NH_START_TS)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        require_non_null(fingerprints, row, COL_FINGERPRINT)?;
        require_non_null(timestamps, row, COL_TIMESTAMP)?;
        require_non_null(schemas, row, COL_NH_SCHEMA)?;
        require_non_null(is_floats, row, COL_NH_IS_FLOAT)?;
        require_non_null(reset_hints, row, COL_NH_RESET_HINT)?;
        require_non_null(zero_thresholds, row, COL_NH_ZERO_THRESHOLD)?;
        require_non_null(zero_counts, row, COL_NH_ZERO_COUNT)?;
        require_non_null(counts, row, COL_NH_COUNT)?;
        require_non_null(sums, row, COL_NH_SUM)?;

        let positive_spans = read_spans(positive_spans, row, COL_NH_POS_SPANS)?;
        let positive_counts = read_f64_list(positive_counts, row, COL_NH_POS_COUNTS)?;
        let negative_spans = read_spans(negative_spans, row, COL_NH_NEG_SPANS)?;
        let negative_counts = read_f64_list(negative_counts, row, COL_NH_NEG_COUNTS)?;

        validate_span_count_consistency(&positive_spans, &positive_counts)?;
        validate_span_count_consistency(&negative_spans, &negative_counts)?;

        rows.push((
            fingerprints.value(row),
            timestamps.value(row),
            NativeHistogram {
                schema: schemas.value(row),
                is_float: is_floats.value(row),
                reset_hint: ResetHint::from_i8(reset_hints.value(row)),
                zero_threshold: zero_thresholds.value(row),
                zero_count: zero_counts.value(row),
                count: counts.value(row),
                sum: sums.value(row),
                positive_spans,
                positive_counts,
                negative_spans,
                negative_counts,
                custom_values: if custom_values.is_null(row) {
                    None
                } else {
                    Some(read_f64_list(custom_values, row, COL_NH_CUSTOM_VALUES)?)
                },
                start_timestamp_ms: if start_timestamps.is_null(row) {
                    None
                } else {
                    Some(start_timestamps.value(row))
                },
            },
        ));
    }

    Ok(rows)
}
