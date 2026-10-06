use super::{Aggregate, Field, MetricFunction, MetricPlan, Pipeline, Result, TraceqlError};

pub(crate) fn metric_plan_for(
    aggregate: &Aggregate,
    by: Vec<Field>,
    stages: Vec<Pipeline>,
) -> Result<MetricPlan> {
    let (function, value, quantiles) = match aggregate {
        Aggregate::Rate => (MetricFunction::Rate, None, Vec::new()),
        Aggregate::CountOverTime => (MetricFunction::CountOverTime, None, Vec::new()),
        Aggregate::SumOverTime(field) => {
            (MetricFunction::SumOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::AvgOverTime(field) => {
            (MetricFunction::AvgOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::MinOverTime(field) => {
            (MetricFunction::MinOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::MaxOverTime(field) => {
            (MetricFunction::MaxOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::HistogramOverTime(field) => (
            MetricFunction::HistogramOverTime,
            Some(field.clone()),
            Vec::new(),
        ),
        Aggregate::QuantileOverTime { field, quantiles } => (
            MetricFunction::QuantileOverTime,
            Some(field.clone()),
            quantiles.clone(),
        ),
        Aggregate::Expression { .. }
        | Aggregate::Count
        | Aggregate::Avg(_)
        | Aggregate::Sum(_)
        | Aggregate::Min(_)
        | Aggregate::Max(_) => {
            return Err(TraceqlError::Unsupported(
                "traceql metrics: expected supported *_over_time() metric".into(),
            ));
        }
    };
    Ok(MetricPlan {
        function,
        value,
        quantiles,
        by,
        exemplar_fields: Vec::new(),
        stages,
        spanset_pipeline: Vec::new(),
        sampling_factor: 1.0,
        spanset_pipeline_had_input: false,
        frontend_labels: false,
        instant: false,
        compare: None,
    })
}
