use super::*;

pub(crate) struct HistogramBucketJson {
    pub(crate) boundary_rule: u8,
    pub(crate) lower: f64,
    pub(crate) upper: f64,
    pub(crate) count: f64,
}
