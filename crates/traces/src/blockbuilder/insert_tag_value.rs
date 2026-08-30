use super::*;

pub(crate) fn insert_tag_value(
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
    tag: &str,
    value: String,
) {
    tag_names.insert(tag.to_string());
    tag_values.entry(tag.to_string()).or_default().insert(value);
}
