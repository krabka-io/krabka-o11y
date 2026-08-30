use super::*;

#[derive(Clone, Debug, PartialEq)]
pub enum Aggregate {
    Count,
    Rate,
    CountOverTime,
    SumOverTime(Field),
    AvgOverTime(Field),
    MinOverTime(Field),
    MaxOverTime(Field),
    HistogramOverTime(Field),
    QuantileOverTime { field: Field, quantiles: Vec<f64> },
    Sum(Field),
    Avg(Field),
    Max(Field),
    Min(Field),
}
