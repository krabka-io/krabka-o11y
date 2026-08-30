use super::*;

pub(crate) fn range_aggregation_supports_grouping(aggregation: &RangeAggregation) -> bool {
    matches!(
        aggregation,
        RangeAggregation::AvgOverTime
            | RangeAggregation::StdvarOverTime
            | RangeAggregation::StddevOverTime
            | RangeAggregation::QuantileOverTime(_)
            | RangeAggregation::MinOverTime
            | RangeAggregation::MaxOverTime
            | RangeAggregation::FirstOverTime
            | RangeAggregation::LastOverTime
    )
}
