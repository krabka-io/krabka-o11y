use super::{
    CompareSpec, Field, FieldExpr, MetricFilter, MetricFunction, Query, RankLimit, Result,
    SpansetExpr, is_inert_metric_stage, metric_pipeline_parts, metric_plan_for,
    metric_plan_with_compare, unsupported_metric_pipeline,
};

pub(crate) struct MetricPlan {
    pub(crate) function: MetricFunction,
    pub(crate) value: Option<Field>,
    pub(crate) quantiles: Vec<f64>,
    pub(crate) by: Vec<Field>,
    pub(crate) exemplar_fields: Vec<Field>,
    pub(crate) filter: Option<MetricFilter>,
    pub(crate) rank: Option<RankLimit>,
    pub(crate) compare: Option<CompareSpec>,
}

pub(crate) fn metric_plan(q: &Query) -> Result<MetricPlan> {
    let normalized_pipeline;
    let pipeline = if q.pipeline.iter().any(is_inert_metric_stage) {
        normalized_pipeline = q
            .pipeline
            .iter()
            .filter(|stage| !is_inert_metric_stage(stage))
            .cloned()
            .collect::<Vec<_>>();
        normalized_pipeline.as_slice()
    } else {
        q.pipeline.as_slice()
    };

    let parts = metric_pipeline_parts(pipeline)?;
    // A `compare()` stage is a standalone metric (no `*_over_time()` aggregate
    // needed): `{outer} | compare({selection}, topN)`. When present it takes
    // precedence and the aggregate, if any, is ignored.
    if let Some(compare) = parts.as_ref().and_then(|parts| parts.compare.clone()) {
        return Ok(metric_plan_with_compare(compare));
    }
    let Some(parts) = parts else {
        return Err(unsupported_metric_pipeline());
    };
    let Some(aggregate) = parts.aggregate else {
        return Err(unsupported_metric_pipeline());
    };
    let mut plan = metric_plan_for(aggregate, parts.by, parts.filter, parts.rank)?;
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
