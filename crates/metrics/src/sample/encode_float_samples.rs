use super::{
    Float64Builder, FloatSampleRow, HistogramCodecError, Int64Builder, RecordBatch, UInt64Builder,
    float_sample_schema,
};

/// Encodes `(fingerprint, timestamp, value, start timestamp)` rows into a `RecordBatch` that
/// matches [`float_sample_schema`].
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_float_samples(rows: &[FloatSampleRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut columns = FloatSampleColumns::new();

    for (fingerprint, timestamp, value, start_timestamp) in rows {
        columns.fingerprints.append_value(*fingerprint);
        columns.timestamps.append_value(*timestamp);
        columns.values.append_value(*value);
        match start_timestamp {
            Some(start_timestamp) => columns.start_timestamps.append_value(*start_timestamp),
            None => columns.start_timestamps.append_null(),
        }
    }

    Ok(RecordBatch::try_new(
        float_sample_schema(),
        columns.finish(),
    )?)
}

#[derive(krabka_column_macros::ColumnBuilders)]
struct FloatSampleColumns {
    fingerprints: UInt64Builder,
    timestamps: Int64Builder,
    values: Float64Builder,
    start_timestamps: Int64Builder,
}
