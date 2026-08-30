use super::*;

pub(crate) fn group_range_samples(
    samples: MetricSamples,
    grouping: &VectorGrouping,
) -> MetricSamples {
    let mut grouped: MetricSamples = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, Some(grouping));
        let grouped_values = grouped.entry(grouped_labels).or_default();
        for (time, value) in values {
            grouped_values.entry(time).or_default().merge(value);
        }
    }

    grouped
}
