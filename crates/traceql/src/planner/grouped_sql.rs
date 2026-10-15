use super::{
    Aggregate, ComparisonOp, Field, GroupJoinSql, Pipeline, Result, TraceqlError,
    aggregate_expr_sql, aggregate_filter_sql, group_join_sql, grouped_rank_pipeline_sql,
    is_search_preserving_aggregate, ungrouped_rank_pipeline_sql,
};

pub(crate) struct AggregateFilter {
    pub(crate) expression: String,
    pub(crate) op: ComparisonOp,
    pub(crate) value: f64,
}

pub(crate) fn grouped_aggregate_sql(
    spanset_sql: &str,
    by: &[Field],
    filter: Option<AggregateFilter>,
) -> Result<String> {
    let Some(filter) = filter else {
        return Ok(format!("SELECT * FROM ({spanset_sql}) AS q"));
    };
    let GroupJoinSql {
        group_exprs,
        join_pred,
    } = group_join_sql(by);
    let pred = aggregate_filter_sql(&filter.expression, filter.op, filter.value)?;
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         passing AS (SELECT {group_exprs} FROM matched GROUP BY {group_exprs} HAVING {pred}) \
         SELECT matched.* FROM matched JOIN passing ON {join_pred}"
    ))
}

pub(crate) fn grouped_no_filter_by(pipeline: &[Pipeline]) -> Option<&[Field]> {
    match pipeline {
        [Pipeline::Aggregate(agg), Pipeline::By(by)]
        | [Pipeline::By(by), Pipeline::Aggregate(agg)]
        | [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            Pipeline::Coalesce,
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            Pipeline::Coalesce,
        ] if is_search_preserving_aggregate(agg) => Some(by),
        _ => None,
    }
}

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
