use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorAggregationOp {
    Sum,
    Count,
    Min,
    Max,
    Avg,
    Stddev,
    Stdvar,
    CountValues(String),
    TopK(u64),
    BottomK(u64),
    ApproxTopK(u64),
    Sort,
    SortDesc,
}
