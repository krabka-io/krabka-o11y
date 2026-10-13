use super::{
    Arc, ArrayRef, Float64Builder, FloatSampleRow, HistogramCodecError, Int64Builder, RecordBatch,
    UInt64Builder, float_sample_schema,
};
use crate::append_optional_i64;

/// Encodes `(fingerprint, timestamp, value, start timestamp)` rows into a `RecordBatch` that
/// matches [`float_sample_schema`].
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_float_samples(rows: &[FloatSampleRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut values = Float64Builder::new();
    let mut start_timestamps = Int64Builder::new();

    for row in rows {
        fingerprints.append_value(row.fingerprint);
        timestamps.append_value(row.timestamp_ms);
        values.append_value(row.value);
        append_optional_i64(&mut start_timestamps, row.start_timestamp_ms);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(values.finish()),
        Arc::new(start_timestamps.finish()),
    ];

    Ok(RecordBatch::try_new(float_sample_schema(), columns)?)
}
