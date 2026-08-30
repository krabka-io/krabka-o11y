use super::*;

pub(crate) fn metric_query_uses_approx_topk(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::ApproxTopK(_)))
}
