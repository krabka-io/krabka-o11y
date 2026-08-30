use super::{BTreeMap, Labels, MetricQuery, MetricSamples, MetricValue, range_sample_value};

pub(crate) fn select_all_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    samples
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, value)| (time, range_sample_value(value, query)))
                    .collect(),
            )
        })
        .collect()
}
