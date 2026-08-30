use super::{COL_FINGERPRINT, COL_TIMESTAMP, COL_VALUE, Float64Array, HistogramCodecError, Int64Array, RecordBatch, UInt64Array, require_non_null, typed_column};

/// Decodes a float-sample `RecordBatch` into `(fingerprint, timestamp, value)`
/// rows.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_float_samples(
    batch: &RecordBatch,
) -> Result<Vec<(u64, i64, f64)>, HistogramCodecError> {
    let fingerprints = typed_column::<UInt64Array>(batch, COL_FINGERPRINT)?;
    let timestamps = typed_column::<Int64Array>(batch, COL_TIMESTAMP)?;
    let values = typed_column::<Float64Array>(batch, COL_VALUE)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        require_non_null(fingerprints, row, COL_FINGERPRINT)?;
        require_non_null(timestamps, row, COL_TIMESTAMP)?;
        require_non_null(values, row, COL_VALUE)?;

        rows.push((
            fingerprints.value(row),
            timestamps.value(row),
            values.value(row),
        ));
    }

    Ok(rows)
}
