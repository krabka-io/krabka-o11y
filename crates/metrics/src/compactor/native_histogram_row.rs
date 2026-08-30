use super::NativeHistogram;

/// One sorted native histogram row ready for block encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeHistogramRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub hist: NativeHistogram,
}
