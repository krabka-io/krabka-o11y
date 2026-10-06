use super::{
    CompareSpec, Field, FieldExpr, MetricFunction, Pipeline, Query, Result, SpansetExpr,
    is_inert_metric_stage, metric_pipeline_parts, metric_plan_for, metric_plan_with_compare,
    unsupported_metric_pipeline,
};

pub(crate) struct MetricPlan {
    pub(crate) function: MetricFunction,
    pub(crate) value: Option<Field>,
    pub(crate) quantiles: Vec<f64>,
    pub(crate) by: Vec<Field>,
    pub(crate) exemplar_fields: Vec<Field>,
    pub(crate) stages: Vec<Pipeline>,
    pub(crate) sampling_factor: f64,
    pub(crate) spanset_pipeline_had_input: bool,
    pub(crate) frontend_labels: bool,
    pub(crate) instant: bool,
    pub(crate) spanset_pipeline: Vec<Pipeline>,
    pub(crate) compare: Option<CompareSpec>,
}

pub(crate) fn metric_plan(q: &Query) -> Result<MetricPlan> {
    let metric_start = q
        .pipeline
        .iter()
        .position(is_metric_stage)
        .ok_or_else(unsupported_metric_pipeline)?;
    let spanset_pipeline = q.pipeline[..metric_start].to_vec();
    let metric_pipeline = &q.pipeline[metric_start..];
    let normalized_pipeline;
    let pipeline = if metric_pipeline.iter().any(is_inert_metric_stage) {
        normalized_pipeline = metric_pipeline
            .iter()
            .filter(|stage| !is_inert_metric_stage(stage))
            .cloned()
            .collect::<Vec<_>>();
        normalized_pipeline.as_slice()
    } else {
        metric_pipeline
    };

    let parts = metric_pipeline_parts(pipeline)?;
    // A `compare()` stage is a standalone metric (no `*_over_time()` aggregate
    // needed): `{outer} | compare({selection}, topN)`. When present it takes
    // precedence and the aggregate, if any, is ignored.
    if let Some(compare) = parts.as_ref().and_then(|parts| parts.compare.clone()) {
        let mut plan = metric_plan_with_compare(compare);
        plan.spanset_pipeline = spanset_pipeline;
        return Ok(plan);
    }
    let Some(parts) = parts else {
        return Err(unsupported_metric_pipeline());
    };
    let Some(aggregate) = parts.aggregate else {
        return Err(unsupported_metric_pipeline());
    };
    let mut plan = metric_plan_for(aggregate, parts.by, parts.stages)?;
    plan.spanset_pipeline = spanset_pipeline;
    collect_exemplar_fields(&q.root, &mut plan.exemplar_fields);
    for field in plan.by.iter().chain(plan.value.iter()) {
        if !plan.exemplar_fields.contains(field) {
            plan.exemplar_fields.push(field.clone());
        }
    }
    Ok(plan)
}

fn collect_exemplar_fields(expr: &SpansetExpr, fields: &mut Vec<Field>) {
    match expr {
        SpansetExpr::Selector(expr) => collect_selector_fields(expr, fields),
        SpansetExpr::And(lhs, rhs)
        | SpansetExpr::Or(lhs, rhs)
        | SpansetExpr::Structural { lhs, rhs, .. } => {
            collect_exemplar_fields(lhs, fields);
            collect_exemplar_fields(rhs, fields);
        }
    }
}

fn collect_selector_fields(expr: &FieldExpr, fields: &mut Vec<Field>) {
    match expr {
        FieldExpr::ExpressionComparison { lhs, rhs, .. } => {
            let mut dependencies = Vec::new();
            lhs.collect_fields(&mut dependencies);
            rhs.collect_fields(&mut dependencies);
            for field in dependencies {
                add_field(field, fields);
            }
        }
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => add_field(lhs, fields),
        FieldExpr::FieldComparison { lhs, rhs, .. } => {
            add_field(lhs, fields);
            add_field(rhs, fields);
        }
        FieldExpr::And(lhs, rhs) | FieldExpr::Or(lhs, rhs) => {
            collect_selector_fields(lhs, fields);
            collect_selector_fields(rhs, fields);
        }
        FieldExpr::Not(inner) => collect_selector_fields(inner, fields),
        FieldExpr::Const(_) => {}
    }
}

fn add_field(field: &Field, fields: &mut Vec<Field>) {
    if !fields.contains(field) {
        fields.push(field.clone());
    }
}

fn is_metric_stage(stage: &Pipeline) -> bool {
    matches!(stage, Pipeline::Compare { .. })
        || matches!(stage, Pipeline::Aggregate(aggregate) if aggregate.is_metric())
}
