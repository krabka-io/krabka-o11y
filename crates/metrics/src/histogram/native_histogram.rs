use super::{BucketSpan, Deserialize, ResetHint, Serialize};

/// A native histogram sample with absolute bucket counts.
#[derive(bon::Builder, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NativeHistogram {
    #[builder(default)]
    pub schema: i8,
    #[builder(default)]
    pub is_float: bool,
    #[builder(default)]
    pub reset_hint: ResetHint,
    #[builder(default)]
    pub zero_threshold: f64,
    #[builder(default)]
    pub zero_count: f64,
    #[builder(default)]
    pub count: f64,
    #[builder(default)]
    pub sum: f64,
    #[builder(default)]
    pub positive_spans: Vec<BucketSpan>,
    #[builder(default)]
    pub positive_counts: Vec<f64>,
    #[builder(default)]
    pub negative_spans: Vec<BucketSpan>,
    #[builder(default)]
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
