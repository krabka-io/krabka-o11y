use super::{
    Aggregate, COL_TRACE_ID, Pipeline, Result, aggregate_filter_sql, aggregate_filter_sql_query,
    grouped_pipeline_sql, is_inert_pipeline_stage, is_search_preserving_pipeline_stage, selector,
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
        ] => {
            let trace = selector::ident(COL_TRACE_ID);
            let pred = aggregate_filter_sql("COUNT(*)", *op, *value)?;
            Ok(format!(
                "WITH matched AS ({spanset_sql}), \
                 passing AS (SELECT {trace} FROM matched GROUP BY {trace} HAVING {pred}) \
                 SELECT matched.* FROM matched JOIN passing ON matched.{trace} = passing.{trace}"
            ))
        }
        [
            Pipeline::Aggregate(
                agg @ (Aggregate::Sum(_)
                | Aggregate::Avg(_)
                | Aggregate::Min(_)
                | Aggregate::Max(_)),
            ),
            Pipeline::Filter { op, value },
        ] => aggregate_filter_sql_query(spanset_sql, agg, *op, *value),
        _ => grouped_pipeline_sql(spanset_sql, pipeline),
    }
}
