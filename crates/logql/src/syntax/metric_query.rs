use super::{RangeAggregation, VectorAggregation, VectorGrouping, StreamQuery, DurationNanos, OffsetNanos};

#[derive(Clone, Debug, PartialEq)]
pub struct MetricQuery {
    pub aggregation: RangeAggregation,
    pub vector_aggregation: Option<VectorAggregation>,
    pub range_grouping: Option<VectorGrouping>,
    pub stream: StreamQuery,
    pub range_ns: DurationNanos,
    pub offset_ns: OffsetNanos,
}
