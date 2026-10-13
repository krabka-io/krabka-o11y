use super::{BTreeMap, Labels, MetricQuery, MetricSamples, MetricValue, range_sample_value};

pub(crate) fn select_all_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut selected = BTreeMap::new();
    for sample_entry in samples {
        let labels = sample_entry.0;
        let mut values_at_time = BTreeMap::new();
        for value_entry in sample_entry.1 {
            values_at_time.insert(value_entry.0, range_sample_value(&value_entry.1, query));
        }
        selected.insert(labels, values_at_time);
    }
    selected
}
