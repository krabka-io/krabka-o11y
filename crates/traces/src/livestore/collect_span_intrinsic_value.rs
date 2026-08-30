use super::{BTreeSet, Span, bytes_to_hex};

pub(crate) fn collect_span_intrinsic_value(
    span: &Span,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    match tag {
        "span:duration" => {
            values.insert(("duration".into(), span.duration_ns.to_string()));
        }
        "span:id" => {
            values.insert(("string".into(), bytes_to_hex(&span.span_id)));
        }
        "span:kind" => {
            values.insert(("int".into(), span.kind.as_i32().to_string()));
        }
        "span:name" => {
            values.insert(("string".into(), span.name.clone()));
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(("string".into(), bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            values.insert(("int".into(), span.status.as_i32().to_string()));
        }
        "span:statusMessage" => {
            if !span.status_message.is_empty() {
                values.insert(("string".into(), span.status_message.clone()));
            }
        }
        "trace:id" => {
            values.insert(("string".into(), bytes_to_hex(&span.trace_id)));
        }
        _ => {}
    }
}
