use super::{BTreeMap, Labels, MetricSampleState};

pub(crate) type MetricSamples = BTreeMap<Labels, BTreeMap<i64, MetricSampleState>>;
