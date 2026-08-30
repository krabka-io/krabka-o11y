use super::*;

pub(crate) struct MetricPlan {
    pub(crate) function: MetricFunction,
    pub(crate) value: Option<Field>,
    pub(crate) quantiles: Vec<f64>,
    pub(crate) by: Vec<Field>,
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
    metric_plan_for(aggregate, parts.by, parts.filter, parts.rank)
}
