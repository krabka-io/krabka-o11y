use super::{Serialize, Deserialize, ResetHint, BucketSpan};

/// A native histogram sample with absolute bucket counts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NativeHistogram {
    pub schema: i8,
    pub is_float: bool,
    pub reset_hint: ResetHint,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub positive_spans: Vec<BucketSpan>,
    pub positive_counts: Vec<f64>,
    pub negative_spans: Vec<BucketSpan>,
    pub negative_counts: Vec<f64>,
    pub custom_values: Option<Vec<f64>>,
    pub start_timestamp_ms: Option<i64>,
}

impl NativeHistogram {
    /// Sentinel schema for NHCB, a native histogram with custom buckets.
    #[must_use]
    pub fn is_nhcb(&self) -> bool {
        self.schema == -53
    }
}
