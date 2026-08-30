use super::*;

pub(crate) enum RangeAggregationKind {
    Standard(RangeAggregation),
    QuantileOverTime,
}
