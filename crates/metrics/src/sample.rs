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
    schema::{COL_FINGERPRINT, COL_TIMESTAMP, float_sample_schema},
};

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn float_samples_round_trip() {
        let rows = [(1_u64, 100_i64, 1.5_f64), (2, 200, -3.0), (1, 300, 0.0)];

        let batch = encode_float_samples(&rows).unwrap();
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
