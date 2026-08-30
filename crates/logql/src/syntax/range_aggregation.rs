use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeAggregation {
    CountOverTime,
    Rate,
    RateCounter,
    BytesRate,
    BytesOverTime,
    AbsentOverTime,
    PresentOverTime,
    SumOverTime,
    AvgOverTime,
    StdvarOverTime,
    StddevOverTime,
    QuantileOverTime(Quantile),
    MinOverTime,
    MaxOverTime,
    FirstOverTime,
    LastOverTime,
}
