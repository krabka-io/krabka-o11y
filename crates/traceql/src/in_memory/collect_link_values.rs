use super::*;

pub(crate) fn collect_link_values(span: &InputSpan, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for link in &span.links {
        match tag {
            "link:traceID" => {
                values.insert(("string".to_string(), bytes_to_hex(&link.trace_id)));
            }
            "link:spanID" => {
                values.insert(("string".to_string(), bytes_to_hex(&link.span_id)));
            }
            _ => {}
        }
        values.extend(
            link.attributes
                .iter()
                .filter(|(key, _)| nested_attribute_key_matches(key, tag, "link."))
                .map(|(_, value)| typed_value_parts(value)),
        );
    }
}
