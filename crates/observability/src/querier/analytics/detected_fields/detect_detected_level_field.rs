use super::{
    BTreeMap, DetectedFieldStats, DetectedFieldType, Labels, add_generated_detected_field,
    detect_log_level, should_insert_unknown_detected_level,
};

pub(crate) fn detect_detected_level_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    labels: &Labels,
    line: &str,
) {
    // The distributor writes the level it discovered at push time. An entry
    // that reached a block without one is classified from its line here, the
    // way the distributor would have, unless its stream carries a level label
    // of its own.
    let level = if let Some(level) = labels.get("detected_level") {
        level.clone()
    } else if should_insert_unknown_detected_level(labels) {
        detect_log_level(line).unwrap_or("unknown").to_string()
    } else {
        return;
    };
    add_generated_detected_field(fields, "detected_level", level, DetectedFieldType::String);
}
