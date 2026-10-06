use krabka_traceql::{Aggregate, Pipeline};

pub(crate) fn metrics_operation_name(query: &str) -> Option<&'static str> {
    let query = krabka_traceql::parse(query).ok()?;
    if query
        .pipeline
        .iter()
        .any(|stage| matches!(stage, Pipeline::By(fields) if !fields.is_empty()))
    {
        return None;
    }
    query.pipeline.iter().find_map(|stage| {
        let Pipeline::Aggregate(aggregate) = stage else {
            return None;
        };
        match aggregate {
            Aggregate::Rate => Some("rate"),
            Aggregate::CountOverTime => Some("count_over_time"),
            Aggregate::SumOverTime(_) => Some("sum_over_time"),
            Aggregate::AvgOverTime(_) => Some("avg_over_time"),
            Aggregate::MinOverTime(_) => Some("min_over_time"),
            Aggregate::MaxOverTime(_) => Some("max_over_time"),
            Aggregate::HistogramOverTime(_) => Some("histogram_over_time"),
            Aggregate::QuantileOverTime { .. } => Some("quantile_over_time"),
            _ => None,
        }
    })
}
