use super::*;

pub(crate) fn add_generated_detected_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    name: &str,
    value: String,
    ty: DetectedFieldType,
) {
    fields
        .entry(name.to_string())
        .and_modify(|stats| stats.add_generated(ty, value.clone()))
        .or_insert_with(|| DetectedFieldStats::new_generated(ty, value));
}
