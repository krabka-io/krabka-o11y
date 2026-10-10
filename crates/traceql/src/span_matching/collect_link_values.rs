use super::{BTreeSet, LinkRef, bytes_to_hex, nested_attribute_key_matches, typed_value_parts};

/// Adds the `(type, value)` pairs that `tag` lists across `links`.
///
/// `tag` is a link intrinsic, a link attribute, or an attribute key with
/// the `link.` scope prefix.
pub fn collect_link_values(links: &[LinkRef], tag: &str, values: &mut BTreeSet<(String, String)>) {
    for link in links {
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
                .flat_map(|(_, value)| typed_value_parts(value)),
        );
    }
}
