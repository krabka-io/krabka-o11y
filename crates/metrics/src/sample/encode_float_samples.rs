use super::*;

/// Encodes `(fingerprint, timestamp, value)` rows into a `RecordBatch` that
/// matches [`float_sample_schema`].
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_float_samples(rows: &[(u64, i64, f64)]) -> Result<RecordBatch, HistogramCodecError> {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut values = Float64Builder::new();

    for (fingerprint, timestamp, value) in rows {
        fingerprints.append_value(*fingerprint);
        timestamps.append_value(*timestamp);
        values.append_value(*value);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(values.finish()),
    ];

    Ok(RecordBatch::try_new(float_sample_schema(), columns)?)
}
