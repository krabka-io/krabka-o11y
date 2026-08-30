use super::*;

/// Maps the planner [`SimpleAggregateOp`] to the interpreter [`AggregateOp`].
///
/// [`SimpleAggregateOp`] shapes the `DataFusion` plan, and [`AggregateOp`]
/// drives the shared `apply_simple_aggregate` kernel. Both enumerate the same
/// six simple ops, so the map is total. This is the seam that lets the
/// histogram-bearing operator path reuse the reduction core of the interpreter.
pub(crate) fn simple_aggregate_op_to_aggregate_op(op: SimpleAggregateOp) -> AggregateOp {
    match op {
        SimpleAggregateOp::Sum => AggregateOp::Sum,
        SimpleAggregateOp::Avg => AggregateOp::Avg,
        SimpleAggregateOp::Min => AggregateOp::Min,
        SimpleAggregateOp::Max => AggregateOp::Max,
        SimpleAggregateOp::Count => AggregateOp::Count,
        SimpleAggregateOp::Group => AggregateOp::Group,
    }
}
