use super::*;

pub(crate) fn format_stream_query(query: &StreamQuery) -> String {
    let mut formatted = format!(
        "{{{}}}",
        query
            .matchers
            .iter()
            .map(format_label_matcher)
            .collect::<Vec<_>>()
            .join(",")
    );
    for stage in &query.pipeline {
        if matches!(stage, PipelineStage::LineFilter(_)) {
            formatted.push(' ');
        } else {
            formatted.push_str(" | ");
        }
        formatted.push_str(&format_pipeline_stage(stage));
    }
    formatted
}
