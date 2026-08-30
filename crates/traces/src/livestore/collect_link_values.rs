use super::{BTreeSet, Span, bytes_to_hex, typed_value_parts};

pub(crate) fn collect_link_values(span: &Span, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for link in &span.links {
        match tag {
            "link:traceID" => {
                values.insert(("string".into(), bytes_to_hex(&link.trace_id)));
            }
            "link:spanID" => {
                values.insert(("string".into(), bytes_to_hex(&link.span_id)));
            }
            _ => {
                values.extend(
                    link.attrs
                        .iter()
                        .filter(|attr| attr.key == tag)
                        .map(|attr| typed_value_parts(&attr.value)),
                );
            }
        }
    }
}
