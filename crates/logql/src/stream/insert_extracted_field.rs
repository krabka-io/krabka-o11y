use super::Labels;

pub(crate) fn insert_extracted_field(fields: &mut Labels, name: &str, value: String) {
    if fields.contains_key(name) {
        fields.entry(format!("{name}_extracted")).or_insert(value);
    } else {
        fields.insert(name.to_string(), value);
    }
}

// Parser collection is separate from insertion into the stream labels. This
// preserves the first parsed occurrence without mistaking an earlier parsed
// field for a stream-label collision.
pub(crate) fn insert_raw_parsed_field(fields: &mut Labels, name: &str, value: String) {
    fields.entry(name.to_string()).or_insert(value);
}
