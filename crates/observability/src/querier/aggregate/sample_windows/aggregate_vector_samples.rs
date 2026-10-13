use super::{
    BTreeMap, Labels, MetricQuery, MetricSamples, MetricValue, VectorAggregation,
    VectorAggregationOp, VectorAggregationState, VectorSelection, count_values_vector_samples,
    range_sample_value, select_all_vector_samples, select_vector_samples, vector_group_labels,
};

pub(crate) fn aggregate_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    vector_aggregation: &VectorAggregation,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    match &vector_aggregation.op {
        VectorAggregationOp::TopK(limit) | VectorAggregationOp::ApproxTopK(limit) => {
            return select_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                *limit,
                VectorSelection::Largest,
            );
        }
        VectorAggregationOp::BottomK(limit) => {
            return select_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                *limit,
                VectorSelection::Smallest,
            );
        }
        VectorAggregationOp::CountValues(label) => {
            return count_values_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                label,
            );
        }
        VectorAggregationOp::Sort | VectorAggregationOp::SortDesc => {
            return select_all_vector_samples(samples, query);
        }
        _ => {}
    }

    let mut states: BTreeMap<Labels, BTreeMap<i64, VectorAggregationState>> = BTreeMap::new();

    for sample_set in samples {
        let grouped_labels =
            vector_group_labels(&sample_set.0, vector_aggregation.grouping.as_ref());
        for sample in sample_set.1 {
            let time = sample.0;
            states
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .record(range_sample_value(&sample.1, query));
        }
    }

    let mut output = BTreeMap::new();
    for label_states in states {
        let mut samples = BTreeMap::new();
        for state in label_states.1 {
            samples.insert(state.0, state.1.finish(&vector_aggregation.op));
        }
        output.insert(label_states.0, samples);
    }
    output
}
