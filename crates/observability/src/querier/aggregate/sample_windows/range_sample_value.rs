use super::*;

pub(crate) fn range_sample_value(value: MetricSampleState, query: &MetricQuery) -> MetricValue {
    match query.aggregation {
        RangeAggregation::CountOverTime
        | RangeAggregation::BytesOverTime
        | RangeAggregation::AbsentOverTime
        | RangeAggregation::SumOverTime => value.sum,
        RangeAggregation::PresentOverTime => MetricValue::integer(1),
        RangeAggregation::Rate | RangeAggregation::BytesRate => {
            rate_metric_value(value.sum, query.range_ns.0)
        }
        RangeAggregation::RateCounter => {
            rate_metric_value(value.counter_increase(), query.range_ns.0)
        }
        RangeAggregation::AvgOverTime => value.average(),
        RangeAggregation::StdvarOverTime => value.stdvar(),
        RangeAggregation::StddevOverTime => value.stddev(),
        RangeAggregation::QuantileOverTime(quantile) => value.quantile(quantile),
        RangeAggregation::MinOverTime => value.min.unwrap_or_else(MetricValue::zero),
        RangeAggregation::MaxOverTime => value.max.unwrap_or_else(MetricValue::zero),
        RangeAggregation::FirstOverTime => value
            .first
            .map_or_else(MetricValue::zero, |(_, value)| value),
        RangeAggregation::LastOverTime => value
            .last
            .map_or_else(MetricValue::zero, |(_, value)| value),
    }
}
