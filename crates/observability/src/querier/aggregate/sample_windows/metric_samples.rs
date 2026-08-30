use super::*;

pub(crate) type MetricSamples = BTreeMap<Labels, BTreeMap<i64, MetricSampleState>>;
