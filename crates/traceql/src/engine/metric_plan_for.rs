use super::{Aggregate, Field, MetricFunction, MetricPlan, Pipeline, Result, TraceqlError};

struct MetricPlanSpec {
    function: MetricFunction,
    value: Option<Field>,
    quantiles: Vec<f64>,
}

pub(crate) fn metric_plan_for(
    aggregate: &Aggregate,
    by: Vec<Field>,
    stages: Vec<Pipeline>,
) -> Result<MetricPlan> {
    let MetricPlanSpec {
        function,
        value,
        quantiles,
    } = match aggregate {
        Aggregate::Rate => MetricPlanSpec {
            function: MetricFunction::Rate,
            value: None,
            quantiles: Vec::new(),
        },
        Aggregate::CountOverTime => MetricPlanSpec {
            function: MetricFunction::CountOverTime,
            value: None,
            quantiles: Vec::new(),
        },
        Aggregate::SumOverTime(field) => MetricPlanSpec {
            function: MetricFunction::SumOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::AvgOverTime(field) => MetricPlanSpec {
            function: MetricFunction::AvgOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::MinOverTime(field) => MetricPlanSpec {
            function: MetricFunction::MinOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::MaxOverTime(field) => MetricPlanSpec {
            function: MetricFunction::MaxOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::HistogramOverTime(field) => MetricPlanSpec {
            function: MetricFunction::HistogramOverTime,
            value: Some(field.clone()),
            quantiles: Vec::new(),
        },
        Aggregate::QuantileOverTime { field, quantiles } => MetricPlanSpec {
            function: MetricFunction::QuantileOverTime,
            value: Some(field.clone()),
            quantiles: quantiles.clone(),
        },
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
