use super::{BTreeMap, MetricSamples, VectorGrouping, vector_group_labels};

pub(crate) fn group_range_samples(
    samples: MetricSamples,
    grouping: &VectorGrouping,
) -> MetricSamples {
    let mut grouped: MetricSamples = BTreeMap::new();

    for series in samples {
        let labels = series.0;
        let values = series.1;
        let grouped_labels = vector_group_labels(&labels, Some(grouping));
        let grouped_values = grouped.entry(grouped_labels).or_default();
        for sample in values {
            grouped_values.entry(sample.0).or_default().merge(sample.1);
        }
    }

    grouped
}
