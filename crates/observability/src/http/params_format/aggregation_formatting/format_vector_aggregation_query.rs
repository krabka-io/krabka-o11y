use super::*;

pub(crate) fn format_vector_aggregation_query(
    aggregation: &VectorAggregation,
    inner: &str,
) -> Option<String> {
    let grouping = aggregation
        .grouping
        .as_ref()
        .map(|grouping| format!(" {}", format_vector_grouping(grouping)))
        .unwrap_or_default();
    match &aggregation.op {
        VectorAggregationOp::Sum => Some(format!("sum{grouping}({inner})")),
        VectorAggregationOp::Count => Some(format!("count{grouping}({inner})")),
        VectorAggregationOp::Min => Some(format!("min{grouping}({inner})")),
        VectorAggregationOp::Max => Some(format!("max{grouping}({inner})")),
        VectorAggregationOp::Avg => Some(format!("avg{grouping}({inner})")),
        VectorAggregationOp::Stddev => Some(format!("stddev{grouping}({inner})")),
        VectorAggregationOp::Stdvar => Some(format!("stdvar{grouping}({inner})")),
        VectorAggregationOp::TopK(limit) => Some(format!("topk{grouping}({limit},{inner})")),
        VectorAggregationOp::BottomK(limit) => Some(format!("bottomk{grouping}({limit},{inner})")),
        VectorAggregationOp::ApproxTopK(limit) if aggregation.grouping.is_none() => {
            Some(format!("approx_topk({limit},{inner})"))
        }
        VectorAggregationOp::Sort if aggregation.grouping.is_none() => {
            Some(format!("sort({inner})"))
        }
        VectorAggregationOp::SortDesc if aggregation.grouping.is_none() => {
            Some(format!("sort_desc({inner})"))
        }
        VectorAggregationOp::CountValues(_)
        | VectorAggregationOp::ApproxTopK(_)
        | VectorAggregationOp::Sort
        | VectorAggregationOp::SortDesc => None,
    }
}
