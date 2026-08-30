use super::RangeAggregation;

pub(crate) enum RangeAggregationKind {
    Standard(RangeAggregation),
    QuantileOverTime,
}
