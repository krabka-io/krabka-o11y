use super::{
    Labels, StreamQuery, UNWRAP_SAMPLE_VALUE_LABEL,
    should_insert_unknown_detected_level_for_stream_query,
};

pub(crate) fn matching_loki_stream_entry(
    query: &StreamQuery,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> Option<(Labels, String)> {
    let evaluation =
        query.evaluate_with_fields_at(labels, line, structured_metadata, timestamp_ns)?;
    let mut stream_labels = evaluation.fields;
    stream_labels.remove(UNWRAP_SAMPLE_VALUE_LABEL);
    if should_insert_unknown_detected_level_for_stream_query(query, &stream_labels) {
        stream_labels.insert("detected_level".to_string(), "unknown".to_string());
    }
    Some((stream_labels, evaluation.line))
}
