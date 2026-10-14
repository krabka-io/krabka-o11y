use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, Int64Builder, UInt64Builder};

pub(crate) trait NumericSampleRow {
    fn fingerprint(&self) -> u64;
    fn timestamp_ms(&self) -> i64;
    fn value(&self) -> f64;
}

pub(crate) fn encode_numeric_sample_columns(rows: &[impl NumericSampleRow]) -> [ArrayRef; 3] {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut values = Float64Builder::new();
    for row in rows {
        fingerprints.append_value(row.fingerprint());
        timestamps.append_value(row.timestamp_ms());
        values.append_value(row.value());
    }
    [
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(values.finish()),
    ]
}
