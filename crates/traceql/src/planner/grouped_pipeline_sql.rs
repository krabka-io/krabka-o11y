use super::{
    Aggregate, Pipeline, Result, TraceqlError, aggregate_expr_sql, grouped_aggregate_sql,
    grouped_aggregate_sql::AggregateFilter, grouped_no_filter_by, grouped_rank_pipeline_sql,
    numeric_aggregate, ungrouped_rank_pipeline_sql,
};

pub(crate) fn grouped_pipeline_sql(spanset_sql: &str, pipeline: &[Pipeline]) -> Result<String> {
    if let Some(by) = grouped_no_filter_by(pipeline) {
        return grouped_aggregate_sql(spanset_sql, by, None);
    }
    if let Some(sql) = grouped_rank_pipeline_sql(spanset_sql, pipeline)? {
        return Ok(sql);
    }
    if let Some(sql) = ungrouped_rank_pipeline_sql(spanset_sql, pipeline)? {
        return Ok(sql);
    }

    match pipeline {
        [
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::Filter { op, value },
            Pipeline::By(by),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::Filter { op, value },
        ] => grouped_aggregate_sql(
            spanset_sql,
            by,
            Some(AggregateFilter {
                expression: "COUNT(*)".to_string(),
                op: *op,
                value: *value,
            }),
        ),
        [
            Pipeline::Aggregate(numeric_aggregate!(agg)),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(numeric_aggregate!(agg)),
            Pipeline::Filter { op, value },
            Pipeline::By(by),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(numeric_aggregate!(agg)),
            Pipeline::Filter { op, value },
        ] => grouped_aggregate_sql(
            spanset_sql,
            by,
            Some(AggregateFilter {
                expression: aggregate_expr_sql(agg)?,
                op: *op,
                value: *value,
            }),
        ),
        _ => Err(TraceqlError::Unsupported(format!(
            "pipeline shape is not valid for trace search: {pipeline:?}"
        ))),
    }
}
