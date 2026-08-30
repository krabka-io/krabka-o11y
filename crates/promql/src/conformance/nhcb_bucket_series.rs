use super::*;

#[derive(Clone)]
pub(crate) struct NhcbBucketSeries {
    pub(crate) upper_bound: f64,
    pub(crate) values: Vec<SampleSpec>,
}
