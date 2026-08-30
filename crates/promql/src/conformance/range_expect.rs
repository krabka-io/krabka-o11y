use super::*;

/// Expected sample timestamps for an instant query that returns a range vector.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeExpect {
    /// Expected first sample timestamp in milliseconds.
    pub start_ms: i64,
    /// Expected step between samples.
    pub step: Time,
}
