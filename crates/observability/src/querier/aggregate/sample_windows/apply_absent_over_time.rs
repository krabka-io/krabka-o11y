use super::{
    BTreeMap, MetricQuery, MetricSampleState, MetricSamples, MetricValue, RangeAggregation,
    absent_metric_labels,
};

pub(crate) fn apply_absent_over_time(
    samples: &mut MetricSamples,
    query: &MetricQuery,
    eval_times: &[i64],
) {
    if !matches!(query.aggregation, RangeAggregation::AbsentOverTime) {
        return;
    }

    let mut absent_values = BTreeMap::new();
    for eval_time_ns in eval_times {
        let has_sample = samples.values().any(|values| {
            values
                .get(eval_time_ns)
                .is_some_and(MetricSampleState::has_samples)
        });
        if !has_sample {
            let mut sample = MetricSampleState::default();
            sample.record(*eval_time_ns, MetricValue::integer(1));
            absent_values.insert(*eval_time_ns, sample);
        }
    }

    samples.clear();
    if !absent_values.is_empty() {
        samples.insert(absent_metric_labels(query), absent_values);
    }
}
