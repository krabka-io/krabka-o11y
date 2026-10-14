//! Float-sample Arrow codec.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder, UInt64Array,
        UInt64Builder,
    },
    record_batch::RecordBatch,
};

use crate::{
    arrow_codec::{require_non_null, typed_column},
    histogram::HistogramCodecError,
    schema::{COL_FINGERPRINT, COL_NH_START_TS, COL_TIMESTAMP, float_sample_schema},
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

mod col_value;
mod decode_float_samples;
mod encode_float_samples;

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

use col_value::COL_VALUE;
pub use decode_float_samples::decode_float_samples;
pub use encode_float_samples::encode_float_samples;

/// One encoded float sample with its optional creation timestamp.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FloatSampleRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub value: f64,
    pub start_timestamp_ms: Option<i64>,
}
