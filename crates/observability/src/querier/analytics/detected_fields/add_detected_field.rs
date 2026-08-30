use super::*;

pub(crate) fn add_detected_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    name: &str,
    value: String,
    ty: DetectedFieldType,
    parser: &'static str,
) {
    fields
        .entry(name.to_string())
        .and_modify(|stats| stats.add(ty, value.clone(), parser))
        .or_insert_with(|| DetectedFieldStats::new(ty, value, parser));
}
