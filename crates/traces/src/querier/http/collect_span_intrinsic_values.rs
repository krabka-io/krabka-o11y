use super::*;

pub(crate) fn collect_span_intrinsic_values(
    span: &SpanRef,
    spans: &[SpanRef],
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    match tag {
        "span:childCount" => {
            let count = spans
                .iter()
                .filter(|other| other.nested_set_parent == span.nested_set_left)
                .count();
            values.insert(("int".to_string(), count.to_string()));
        }
        "span:duration" => {
            values.insert((
                "duration".to_string(),
                span.duration.nanos_i64().to_string(),
            ));
        }
        "span:id" => {
            values.insert(("string".to_string(), hex::encode(span.span_id)));
        }
        "span:kind" => {
            values.insert(("int".to_string(), span.kind.to_string()));
        }
        "span:name" => {
            values.insert(("string".to_string(), span.name.clone()));
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(("string".to_string(), hex::encode(parent_id)));
            }
        }
        "span:nestedSetLeft" => {
            values.insert(("int".to_string(), span.nested_set_left.to_string()));
        }
        "span:nestedSetParent" | "span:Parent" => {
            values.insert(("int".to_string(), span.nested_set_parent.to_string()));
        }
        "span:nestedSetRight" => {
            values.insert(("int".to_string(), span.nested_set_right.to_string()));
        }
        "span:status" => {
            values.insert(("int".to_string(), span.status_code.to_string()));
        }
        "span:statusMessage" if !span.status_message.is_empty() => {
            values.insert(("string".to_string(), span.status_message.clone()));
        }
        "instrumentation:name" if !span.instrumentation_name.is_empty() => {
            values.insert(("string".to_string(), span.instrumentation_name.clone()));
        }
        "instrumentation:version" if !span.instrumentation_version.is_empty() => {
            values.insert(("string".to_string(), span.instrumentation_version.clone()));
        }
        _ => {}
    }
}
