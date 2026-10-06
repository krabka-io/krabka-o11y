use super::{Field, ScalarExpr};

#[derive(Clone, Debug, PartialEq)]
/// A `TraceQL` spanset or metrics aggregation.
pub enum Aggregate {
    Count,
    Rate,
    CountOverTime,
    SumOverTime(Field),
    AvgOverTime(Field),
    MinOverTime(Field),
    MaxOverTime(Field),
    HistogramOverTime(Field),
    QuantileOverTime {
        field: Field,
        quantiles: Vec<f64>,
    },
    /// A numeric expression evaluated on each original typed span.
    Expression {
        function: ScalarAggregate,
        expr: ScalarExpr,
    },
    Sum(Field),
    Avg(Field),
    Max(Field),
    Min(Field),
}

/// A scalar reduction over the spans in each current spanset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarAggregate {
    Sum,
    Avg,
    Min,
    Max,
}

impl Aggregate {
    pub(crate) fn is_metric(&self) -> bool {
        matches!(
            self,
            Self::Rate
                | Self::CountOverTime
                | Self::SumOverTime(_)
                | Self::AvgOverTime(_)
                | Self::MinOverTime(_)
                | Self::MaxOverTime(_)
                | Self::HistogramOverTime(_)
                | Self::QuantileOverTime { .. }
        )
    }
}
