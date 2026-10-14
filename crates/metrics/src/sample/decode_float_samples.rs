use arrow::array::Array;

use super::{
    COL_FINGERPRINT, COL_NH_START_TS, COL_TIMESTAMP, COL_VALUE, Float64Array, FloatSampleRow,
    HistogramCodecError, Int64Array, RecordBatch, UInt64Array, require_non_null, typed_column,
};
use crate::arrow_codec::decode_rows_with_identity;

/// Decodes a float-sample `RecordBatch` into
/// `(fingerprint, timestamp, value, start timestamp)` rows.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
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
