use super::RangeAggregation;

pub(crate) fn format_range_aggregation_name(
    aggregation: &RangeAggregation,
) -> Option<&'static str> {
    match aggregation {
        RangeAggregation::CountOverTime => Some("count_over_time"),
        RangeAggregation::Rate => Some("rate"),
        RangeAggregation::RateCounter => Some("rate_counter"),
        RangeAggregation::BytesRate => Some("bytes_rate"),
        RangeAggregation::BytesOverTime => Some("bytes_over_time"),
        RangeAggregation::AbsentOverTime => Some("absent_over_time"),
        RangeAggregation::PresentOverTime => Some("present_over_time"),
        RangeAggregation::SumOverTime => Some("sum_over_time"),
        RangeAggregation::AvgOverTime => Some("avg_over_time"),
        RangeAggregation::StdvarOverTime => Some("stdvar_over_time"),
        RangeAggregation::StddevOverTime => Some("stddev_over_time"),
        RangeAggregation::MinOverTime => Some("min_over_time"),
        RangeAggregation::MaxOverTime => Some("max_over_time"),
        RangeAggregation::FirstOverTime => Some("first_over_time"),
        RangeAggregation::LastOverTime => Some("last_over_time"),
        RangeAggregation::QuantileOverTime(_) => None,
    }
}
