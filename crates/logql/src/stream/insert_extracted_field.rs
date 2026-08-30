use super::*;

pub(crate) fn insert_extracted_field(fields: &mut Labels, name: &str, value: String) {
    if fields.contains_key(name) {
        fields.entry(format!("{name}_extracted")).or_insert(value);
    } else {
        fields.insert(name.to_string(), value);
    }
}
