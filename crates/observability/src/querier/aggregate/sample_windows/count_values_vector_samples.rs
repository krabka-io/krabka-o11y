use super::{
    BTreeMap, Labels, MetricQuery, MetricSamples, MetricValue, VectorGrouping, format_metric_value,
    range_sample_value, vector_group_labels,
};

pub(crate) fn count_values_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    value_label: &str,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut counted: BTreeMap<Labels, BTreeMap<i64, u64>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, grouping);
        for (time, value) in values {
            let value = range_sample_value(value, query);
            let mut output_labels = grouped_labels.clone();
            output_labels.insert(value_label.to_string(), format_metric_value(value));
            *counted
                .entry(output_labels)
                .or_default()
                .entry(time)
                .or_default() += 1;
        }
    }

    counted
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, count)| (time, MetricValue::integer(count)))
                    .collect(),
            )
        })
        .collect()
}
