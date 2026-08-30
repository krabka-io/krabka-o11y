use super::*;

/// One float sample with its series identity resolved to a label set.
pub struct LabeledSample {
    pub fp: SeriesFingerprint,
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}
