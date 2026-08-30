use super::{
    BTreeMap, DetectedFieldStats, DetectedFieldType, Labels, add_generated_detected_field,
    detect_log_level, should_insert_unknown_detected_level,
};

pub(crate) fn detect_detected_level_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    labels: &Labels,
    line: &str,
) {
    if !should_insert_unknown_detected_level(labels) {
        return;
    }
    let level = detect_log_level(line).unwrap_or("unknown");
    add_generated_detected_field(
        fields,
        "detected_level",
        level.to_string(),
        DetectedFieldType::String,
    );
}
