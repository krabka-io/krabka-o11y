use super::{BTreeMap, DetectedFieldStats, Labels, add_detected_field, field_type_from_str};

pub(crate) fn detect_structured_metadata_fields(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    metadata: &Labels,
) {
    for (name, value) in metadata {
        add_detected_field(
            fields,
            name,
            value.clone(),
            field_type_from_str(value),
            "structured_metadata",
        );
    }
}
