use super::{
    BTreeMap, DetectedFieldStats, Labels, detect_detected_level_field, detect_json_fields,
    detect_logfmt_fields, detect_structured_metadata_fields,
};

/// Records every field one log entry shows: its level, its structured
/// metadata, and what the JSON and logfmt parsers find in its line.
pub(crate) fn detect_entry_fields(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
) {
    detect_detected_level_field(fields, labels, line);
    detect_structured_metadata_fields(fields, structured_metadata);
    detect_json_fields(fields, line);
    detect_logfmt_fields(fields, line);
}
