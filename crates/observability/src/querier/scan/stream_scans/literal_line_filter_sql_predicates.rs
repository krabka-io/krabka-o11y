use super::{LineFilterOp, PipelineStage, sql_like_pattern_literal};

pub(crate) fn literal_line_filter_sql_predicates(pipeline: &[PipelineStage]) -> Vec<String> {
    let mut predicates = Vec::new();
    for stage in pipeline {
        if stage.mutates_line() {
            break;
        }
        if let Some(predicate) = {
            let PipelineStage::LineFilter(filter) = stage else {
                continue;
            };
            if filter.is_ip_matcher() {
                continue;
            }
            match filter.op {
                LineFilterOp::Contains => Some(format!(
                    "line like '%{}%'",
                    sql_like_pattern_literal(&filter.pattern)
                )),
                LineFilterOp::NotContains => Some(format!(
                    "line not like '%{}%'",
                    sql_like_pattern_literal(&filter.pattern)
                )),
                LineFilterOp::Regex
                | LineFilterOp::NotRegex
                | LineFilterOp::Pattern
                | LineFilterOp::NotPattern => None,
            }
        } {
            predicates.push(predicate);
        }
    }
    predicates
}
