use super::{Labels, NhcbBucketSeries, SampleSpec};

#[derive(Clone)]
pub(crate) struct NhcbGroup {
    pub(crate) labels: Labels,
    pub(crate) buckets: Vec<NhcbBucketSeries>,
    pub(crate) sum_values: Option<Vec<SampleSpec>>,
}

impl NhcbGroup {
    pub(crate) fn new(labels: Labels) -> Self {
        Self {
            labels,
            buckets: Vec::new(),
            sum_values: None,
        }
    }
}
