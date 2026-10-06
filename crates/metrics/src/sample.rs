//! Float-sample Arrow codec.

use arrow::{
    array::{Float64Array, Float64Builder, Int64Array, Int64Builder, UInt64Array, UInt64Builder},
    record_batch::RecordBatch,
};

use crate::{
    arrow_codec::{require_non_null, typed_column},
    histogram::HistogramCodecError,
    schema::{COL_FINGERPRINT, COL_NH_START_TS, COL_TIMESTAMP, float_sample_schema},
};

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use assert2::assert;

    use super::*;

    #[test]
    fn float_samples_round_trip() {
        let rows = [
            (1_u64, 100_i64, 1.5_f64, Some(50)),
            (2, 200, -3.0, None),
            (1, 300, 0.0, Some(250)),
        ];

        let batch = encode_float_samples(&rows).unwrap();
        let fingerprints = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let starts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(fingerprints.value(0) == 1);
        assert!(timestamps.value(0) == 100);
        assert!(values.value(0) == 1.5);
        assert!(starts.value(0) == 50);
        assert!(starts.is_null(1));
        let decoded = decode_float_samples(&batch).unwrap();

        assert!(decoded == rows);
    }
}

mod col_value;
mod decode_float_samples;
mod encode_float_samples;

use col_value::COL_VALUE;
pub use decode_float_samples::decode_float_samples;
pub use encode_float_samples::encode_float_samples;

/// One encoded float sample: fingerprint, timestamp, value, and optional creation timestamp.
pub type FloatSampleRow = (u64, i64, f64, Option<i64>);
