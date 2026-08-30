use super::{MetricQuery, VectorAggregationOp};

pub(crate) fn metric_query_uses_count_values(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::CountValues(_)))
}
