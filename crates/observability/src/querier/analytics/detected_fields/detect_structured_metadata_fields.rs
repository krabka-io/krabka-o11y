use super::{
    BTreeMap, DetectedFieldStats, Labels, add_generated_detected_field, field_type_from_str,
};

/// Records each structured-metadata pair as a field.
///
/// No parser produced it, so it names none: Loki 3.5.1 reports such a field
/// with `"parsers": null`, as it does `detected_level`.
pub(crate) fn detect_structured_metadata_fields(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    metadata: &Labels,
) {
    for (name, value) in metadata {
        add_generated_detected_field(fields, name, value.clone(), field_type_from_str(value));
    }
}
