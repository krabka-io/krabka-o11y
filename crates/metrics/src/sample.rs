//! Float-sample Arrow codec.

use std::sync::Arc;

use arrow::{
    array::{Array, Float64Array, Int64Array, Int64Builder, UInt64Array},
    record_batch::RecordBatch,
};

use crate::{
    arrow_codec::{decode_rows_with_identity, require_non_null, typed_column},
    histogram::HistogramCodecError,
    numeric_sample_columns::encode_numeric_sample_columns,
    schema::{COL_FINGERPRINT, COL_NH_START_TS, COL_TIMESTAMP, float_sample_schema},
    append_optional_i64,
};

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn float_samples_round_trip() {
        let rows = [
            FloatSampleRow {
                fingerprint: 1,
                timestamp_ms: 100,
                value: 1.5,
                start_timestamp_ms: Some(50),
            },
            FloatSampleRow {
                fingerprint: 2,
                timestamp_ms: 200,
                value: -3.0,
                start_timestamp_ms: None,
            },
            FloatSampleRow {
                fingerprint: 1,
                timestamp_ms: 300,
                value: 0.0,
                start_timestamp_ms: Some(250),
            },
        ];

        let batch = encode_float_samples(&rows).unwrap();
        let decoded = decode_float_samples(&batch).unwrap();

        assert!(decoded == rows);
    }
}
impl crate::numeric_sample_columns::NumericSampleRow for FloatSampleRow {
    fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    fn value(&self) -> f64 {
        self.value
    }
}

/// One encoded float sample with its optional creation timestamp.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FloatSampleRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub value: f64,
    pub start_timestamp_ms: Option<i64>,
}

pub(crate) const COL_VALUE: &str = "value";

/// Encodes `(fingerprint, timestamp, value, start timestamp)` rows into a
/// `RecordBatch` that matches [`float_sample_schema`].
///
/// # Errors
/// Returns an error when Arrow rejects the assembled batch.
pub fn encode_float_samples(rows: &[FloatSampleRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut start_timestamps = Int64Builder::new();
    for row in rows {
        append_optional_i64(&mut start_timestamps, row.start_timestamp_ms);
    }
    let mut columns = Vec::from(encode_numeric_sample_columns(rows));
    columns.push(Arc::new(start_timestamps.finish()));
    Ok(RecordBatch::try_new(float_sample_schema(), columns)?)
}

/// Decodes a float-sample `RecordBatch` into rows.
///
/// # Errors
/// Returns an error when the batch has a missing or invalid column.
pub fn decode_float_samples(
    batch: &RecordBatch,
) -> Result<Vec<FloatSampleRow>, HistogramCodecError> {
    let fingerprints = typed_column::<UInt64Array>(batch, COL_FINGERPRINT)?;
    let timestamps = typed_column::<Int64Array>(batch, COL_TIMESTAMP)?;
    let values = typed_column::<Float64Array>(batch, COL_VALUE)?;
    let start_timestamps = typed_column::<Int64Array>(batch, COL_NH_START_TS)?;
    decode_rows_with_identity(batch, fingerprints, timestamps, |identity, row| {
        require_non_null(values, row, COL_VALUE)?;
        Ok(FloatSampleRow {
            fingerprint: identity.fingerprint,
            timestamp_ms: identity.timestamp_ms,
            value: values.value(row),
            start_timestamp_ms: (!start_timestamps.is_null(row))
                .then(|| start_timestamps.value(row)),
        })
    })
}
