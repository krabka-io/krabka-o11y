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

    for sample_set in samples {
        let grouped_labels = vector_group_labels(&sample_set.0, grouping);
        for sample in sample_set.1 {
            let time = sample.0;
            let mut sample_value = sample.1;
            let value = range_sample_value(&mut sample_value, query);
            let mut output_labels = grouped_labels.clone();
            output_labels.insert(value_label.to_string(), format_metric_value(value));
            *counted
                .entry(output_labels)
                .or_default()
                .entry(time)
                .or_default() += 1;
        }
    }

    let mut output = BTreeMap::new();
    for label_counts in counted {
        let mut samples = BTreeMap::new();
        for count in label_counts.1 {
            samples.insert(count.0, MetricValue::integer(count.1));
        }
        output.insert(label_counts.0, samples);
    }
    output
}
