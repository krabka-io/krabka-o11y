use super::*;

#[derive(Clone)]
pub(crate) struct NhcbGroup {
    pub(crate) labels: Labels,
    pub(crate) buckets: Vec<NhcbBucketSeries>,
    pub(crate) sum_values: Option<Vec<SampleSpec>>,
}
