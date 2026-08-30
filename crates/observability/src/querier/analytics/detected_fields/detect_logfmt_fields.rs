use super::*;

pub(crate) fn detect_logfmt_fields(fields: &mut BTreeMap<String, DetectedFieldStats>, line: &str) {
    for (name, value) in parse_logfmt_pairs(line) {
        let ty = field_type_from_str(&value);
        add_detected_field(fields, &name, value, ty, "logfmt");
    }
}
