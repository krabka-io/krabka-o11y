use super::{Labels, PipelineStage, StreamQuery, should_insert_unknown_detected_level};

pub(crate) fn should_insert_unknown_detected_level_for_stream_query(
    query: &StreamQuery,
    labels: &Labels,
) -> bool {
    should_insert_unknown_detected_level(labels)
        && !query
            .pipeline
            .iter()
            .any(|stage| matches!(stage, PipelineStage::KeepLabels(_)))
}
