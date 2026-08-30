#[derive(Clone, Copy)]
pub(crate) enum MetricFunction {
    Rate,
    CountOverTime,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    HistogramOverTime,
    QuantileOverTime,
}
