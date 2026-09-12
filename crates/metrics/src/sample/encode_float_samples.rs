use super::{
    Arc, ArrayRef, Float64Builder, FloatSampleRow, HistogramCodecError, Int64Builder, RecordBatch,
    UInt64Builder, float_sample_schema,
};

/// Encodes `(fingerprint, timestamp, value, start timestamp)` rows into a `RecordBatch` that
/// matches [`float_sample_schema`].
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_float_samples(rows: &[FloatSampleRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut values = Float64Builder::new();
    let mut start_timestamps = Int64Builder::new();

    for (fingerprint, timestamp, value, start_timestamp) in rows {
        fingerprints.append_value(*fingerprint);
        timestamps.append_value(*timestamp);
        values.append_value(*value);
        match start_timestamp {
            Some(start_timestamp) => start_timestamps.append_value(*start_timestamp),
            None => start_timestamps.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(values.finish()),
        Arc::new(start_timestamps.finish()),
    ];

    Ok(RecordBatch::try_new(float_sample_schema(), columns)?)
}
