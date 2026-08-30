use super::{Pipeline, SpanMatcher, aggregate_projection_field, push_nested_projection_matcher};

pub(crate) fn pipeline_nested_projection_matchers(pipeline: &[Pipeline]) -> Vec<SpanMatcher> {
    let mut out = Vec::new();
    for stage in pipeline {
        match stage {
            Pipeline::By(fields) | Pipeline::Select(fields) => {
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
