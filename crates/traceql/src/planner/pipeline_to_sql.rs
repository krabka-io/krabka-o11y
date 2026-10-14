use super::{
    Aggregate, Pipeline, Result, aggregate_filter_query_any_sql, aggregate_filter_sql_query,
    grouped_pipeline_sql, is_inert_pipeline_stage, is_search_preserving_pipeline_stage,
};

pub(crate) fn pipeline_to_sql(spanset_sql: &str, pipeline: &[Pipeline]) -> Result<String> {
    if !pipeline.is_empty() && pipeline.iter().all(is_search_preserving_pipeline_stage) {
        return Ok(format!("SELECT * FROM ({spanset_sql}) AS q"));
    }

    let normalized_pipeline;
    let pipeline = if pipeline.iter().any(is_inert_pipeline_stage) {
        normalized_pipeline = pipeline
            .iter()
            .filter(|stage| !is_inert_pipeline_stage(stage))
            .cloned()
            .collect::<Vec<_>>();
        normalized_pipeline.as_slice()
    } else {
        pipeline
    };

    match pipeline {
        [] => Ok(format!("SELECT * FROM ({spanset_sql}) AS q")),
        [
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::Filter { op, value },
        ] => aggregate_filter_query_any_sql(spanset_sql, "COUNT(*)", *op, *value),
        [
            Pipeline::Aggregate(numeric_aggregate!(agg)),
            Pipeline::Filter { op, value },
        ] => aggregate_filter_sql_query(spanset_sql, agg, *op, *value),
        _ => grouped_pipeline_sql(spanset_sql, pipeline),
    }
}
