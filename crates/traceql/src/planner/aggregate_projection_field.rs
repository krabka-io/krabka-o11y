use super::*;

pub(crate) fn aggregate_projection_field(agg: &Aggregate) -> Option<&Field> {
    match agg {
        Aggregate::Sum(field)
        | Aggregate::Avg(field)
        | Aggregate::Min(field)
        | Aggregate::Max(field)
        | Aggregate::SumOverTime(field)
        | Aggregate::AvgOverTime(field)
        | Aggregate::MinOverTime(field)
        | Aggregate::MaxOverTime(field)
        | Aggregate::HistogramOverTime(field)
        | Aggregate::QuantileOverTime { field, .. } => Some(field),
        Aggregate::Count | Aggregate::Rate | Aggregate::CountOverTime => None,
    }
}
