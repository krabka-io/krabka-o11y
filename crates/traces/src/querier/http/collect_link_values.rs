use super::{BTreeSet, SpanRef, nested_attribute_key_matches, typed_value_parts};

pub(crate) fn collect_link_values(span: &SpanRef, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for link in &span.links {
        match tag {
            "link:traceID" => {
                values.insert(("string".to_string(), hex::encode(link.trace_id)));
            }
            "link:spanID" => {
                values.insert(("string".to_string(), hex::encode(link.span_id)));
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
