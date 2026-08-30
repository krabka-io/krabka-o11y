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

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, vector_aggregation.grouping.as_ref());
        for (time, value) in values {
            states
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .record(range_sample_value(value, query));
        }
    }

    states
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, state)| (time, state.finish(&vector_aggregation.op)))
                    .collect(),
            )
        })
        .collect()
}
