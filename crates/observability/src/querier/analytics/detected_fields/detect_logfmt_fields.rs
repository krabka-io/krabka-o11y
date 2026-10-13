use super::{
    BTreeMap, DetectedFieldStats, add_detected_field, field_type_from_str, parse_logfmt_pairs,
};

pub(crate) fn detect_logfmt_fields(fields: &mut BTreeMap<String, DetectedFieldStats>, line: &str) {
    for pair in parse_logfmt_pairs(line) {
        let ty = field_type_from_str(&pair.value);
        add_detected_field(fields, &pair.name, pair.value, ty, "logfmt");
    }
}
