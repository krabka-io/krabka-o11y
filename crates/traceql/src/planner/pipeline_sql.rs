use super::{
    Aggregate, Field, Pipeline, Result, ScanOptions, SpanMatcher, aggregate_filter_query_any_sql,
    aggregate_filter_sql_query, aggregate_projection_field, is_inert_pipeline_stage,
    is_search_preserving_pipeline_stage, nested_projection_matcher, Scope,
};
use super::grouped_sql::grouped_pipeline_sql;

pub(crate) fn pipeline_nested_projection_matchers(pipeline: &[Pipeline]) -> Vec<SpanMatcher> {
    let mut out = Vec::new();
    for stage in pipeline {
        match stage {
            Pipeline::By(fields) | Pipeline::Select(fields) => {
                for field in fields {
                    push_nested_projection_matcher(&mut out, field);
                }
            }
            Pipeline::Group(expr) | Pipeline::Aggregate(Aggregate::Expression { expr, .. }) => {
                let mut fields = Vec::new();
                expr.collect_fields(&mut fields);
                for field in fields {
                    push_nested_projection_matcher(&mut out, field);
                }
            }
            Pipeline::Aggregate(agg) => {
                if let Some(field) = aggregate_projection_field(agg) {
                    push_nested_projection_matcher(&mut out, field);
                }
            }
            Pipeline::Filter { .. }
            | Pipeline::TopK(_)
            | Pipeline::BottomK(_)
            | Pipeline::Compare { .. }
            | Pipeline::Coalesce
            | Pipeline::With(_) => {}
        }
    }
    out
}

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

pub(crate) fn scan_options_with_pipeline_projections(
    options: &ScanOptions,
    pipeline: &[Pipeline],
) -> ScanOptions {
    let mut options = options.clone();
    options.include_raw_attributes |= pipeline
        .iter()
        .any(super::apply_expression_pipeline::requires_expression_execution);
    for matcher in pipeline_nested_projection_matchers(pipeline) {
        if !options.projection_matchers.contains(&matcher) {
            options.projection_matchers.push(matcher);
        }
    }
    options
}


pub(crate) fn push_nested_projection_matcher(out: &mut Vec<SpanMatcher>, field: &Field) {
    if field.scope == Scope::Both {
        for scope in [Scope::Event, Scope::Link, Scope::Instrumentation] {
            let scoped = Field {
                scope,
                key: field.key.clone(),
            };
            push_nested_projection_matcher(out, &scoped);
        }
    }
    let Some(matcher) = nested_projection_matcher(field) else {
        return;
    };
    if !out.contains(&matcher) {
        out.push(matcher);
    }
}
